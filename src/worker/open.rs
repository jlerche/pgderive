use super::{program, registration, spec::Settings, sql::Session};
use crate::{
    Config,
    catalog::{Catalog, Deltas, Sink, Snapshot, Writer},
    engine::plan::query,
    source::{Contract, Export},
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

pub(super) struct Open {
    pub(super) session: Session,
    pub(super) catalog: Catalog,
    pub(super) query: program::Query,
    pub(super) compiled: crate::compiler::Compiled,
    pub(super) writer: Writer,
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) nonce: String,
    pub(super) replication: Config,
}
impl Open {
    pub(super) async fn connect(config: &Config, settings: &Settings) -> Result<Self> {
        settings.validate()?;
        let store = config
            .object_store
            .as_ref()
            .context("worker requires durable object storage")?
            .build(&settings.object_prefix)?;
        let mut session = Session::connect(config).await?;
        session.own(&settings.catalog_schema, &settings.query_id).await?;
        let catalog = Catalog::new(&settings.catalog_schema, &settings.query_id)?;
        catalog.install(&session.client).await?;
        let contract = registration::source(&mut session.client, config, settings).await?;
        let mut replication = config.clone();
        replication.replication.slot = contract.slot.clone();
        let options = query::Settings {
            store: store.clone(),
            block_rows: settings.block_rows,
            limits: config.execution,
        };
        let compiled = settings.query.compile(&contract)?;
        let mut query = program::build(&contract, &compiled, options.clone())?;
        if catalog.load_durable(&mut session.client, query.plan()).await?.is_none() {
            registration::reset_unactivated(&session.client, &contract).await?;
            bootstrap(&replication, settings, &mut session, &catalog, &mut query).await?;
        }
        let (writer, nonce) =
            reopen(&mut session, &catalog, &mut query, (&contract, settings)).await?;
        Ok(Self { session, catalog, query, compiled, writer, store, nonce, replication })
    }
}
async fn reopen(
    session: &mut Session,
    catalog: &Catalog,
    query: &mut program::Query,
    binding: (&Contract, &Settings),
) -> Result<(Writer, String)> {
    let (contract, settings) = binding;
    let prior = catalog
        .load_durable(&mut session.client, query.plan())
        .await?
        .context("worker has no durable activation")?;
    ensure!(
        prior.binding.registered_source()?.as_ref() == Some(contract),
        "worker native source contract differs from activation"
    );
    ensure!(
        prior.binding.sink == Sink::Grouped(settings.sink_table.clone()),
        "configured worker destination differs from durable binding"
    );
    let writer = catalog.claim(&mut session.client, query.plan(), prior.binding, prior.end).await?;
    let nonce: String = session
        .client
        .query_one("SELECT pg_catalog.gen_random_uuid()::text", &[])
        .await?
        .try_get(0)?;
    writer.seal_fenced_recovery(&mut session.client, query.plan()).await?;
    let pin = writer
        .protect_recovery(&mut session.client, query.plan(), &format!("worker-recovery:{nonce}"))
        .await?;
    let durable = writer.confirmed(&mut session.client, query.plan()).await?;
    ensure!(
        pin.stored()?.checkpoint == durable.stored.checkpoint,
        "worker recovery boundary changed after claim"
    );
    super::drive::emit(
        &serde_json::json!({"event":"recovering","time":durable.stored.checkpoint.time,"pin":format!("worker-recovery:{nonce}")}),
    )?;
    query.restore_checkpoint(pin.stored()?.checkpoint.clone()).await?;
    let verified = writer.confirmed(&mut session.client, query.plan()).await?;
    ensure!(
        verified.stored.checkpoint == query.checkpoint()?,
        "private recovery lost ownership or membership before readiness"
    );
    pin.reader_finished(&mut session.client).await?;
    writer.seal_fenced_uploads(&mut session.client, query.plan()).await?;
    Ok((writer, nonce))
}
async fn bootstrap(
    config: &Config,
    settings: &Settings,
    session: &mut Session,
    catalog: &Catalog,
    query: &mut program::Query,
) -> Result<()> {
    let export = Export::create(config, &config.replication.slot).await?;
    let boundary = export.consistent();
    let snapshot = export.import(&mut session.client).await?;
    let contract = Contract::inspect(
        &snapshot,
        export.identity().clone(),
        &config.replication.publication,
        &config.replication.slot,
    )
    .await?;
    let batch = crate::source::copy(&snapshot, &contract, config.execution).await?;
    snapshot.commit().await?;
    export.close().await?;
    // The preliminary registration must match the exact imported source layout.
    let exact = program::build(
        &contract,
        &settings.query.compile(&contract)?,
        query::Settings {
            store: config
                .object_store
                .as_ref()
                .context("missing storage")?
                .build(&settings.object_prefix)?,
            block_rows: settings.block_rows,
            limits: config.execution,
        },
    )?;
    ensure!(
        exact.plan().identity() == query.plan().identity(),
        "source changed while establishing snapshot"
    );
    let token: String = session
        .client
        .query_one("SELECT pg_catalog.gen_random_uuid()::text", &[])
        .await?
        .try_get(0)?;
    let protection =
        catalog.protect_upload(&mut session.client, &format!("worker-bootstrap:{token}")).await?;
    super::drive::emit(
        &serde_json::json!({"event":"bootstrap_upload","slot":contract.slot,"boundary":boundary.to_string()}),
    )?;
    let prepared = query
        .prepare_protected(
            program::inputs(&batch, &settings.query.compile(&contract)?.selectors, 1)?,
            &protection,
        )
        .await?;
    let checkpoint = query.prepared_checkpoint(&prepared)?;
    let deltas = Deltas::grouped(&prepared.output().batch)?;
    catalog
        .activate_snapshot(
            &mut session.client,
            query.plan(),
            Snapshot {
                checkpoint: &checkpoint,
                deltas: &deltas,
                source: &contract,
                boundary,
                sink: Sink::Grouped(settings.sink_table.clone()),
            },
        )
        .await?;
    query.commit(prepared)?;
    // Activation establishes fence one; claiming validates the binding and allows
    // an exact authoritative release of bootstrap PUT protection.
    let durable = catalog
        .load_durable(&mut session.client, query.plan())
        .await?
        .context("activation disappeared")?;
    let writer =
        catalog.claim(&mut session.client, query.plan(), durable.binding, durable.end).await?;
    protection.published(&mut session.client, &writer, query.plan(), &checkpoint).await
}
