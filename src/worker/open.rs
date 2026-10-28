use super::{program::relational, registration, spec::Settings, sql::Session};
use crate::{
    Config,
    catalog::{Catalog, Snapshot, Writer},
    engine::plan::Settings as EngineSettings,
    source::{Contract, Export},
};
use anyhow::{Context, Result, ensure};

use std::sync::Arc;

pub(super) struct Open {
    pub(super) maintenance: super::checkpoint::debt::Controller,
    pub(super) checkpoint: Option<super::checkpoint::Pending>,
    pub(super) collector: super::collection::Collector,
    pub(super) settings: Settings,
    pub(super) session: Session,
    pub(super) catalog: Catalog,
    pub(super) query: relational::Query,
    pub(super) compiled: crate::compiler::Compiled,
    pub(super) writer: Writer,
    pub(super) store: Arc<dyn crate::storage::PageStore>,
    pub(super) nonce: String,
    pub(super) replication: Config,
}
impl Open {
    pub(super) async fn input(
        &self,
        batch: &crate::weighted::Batch,
        time: u64,
    ) -> Result<relational::Inputs> {
        let _workspace = self.query.workspaces().acquire().await?;
        let mut input =
            relational::Input::new(self.compiled.relational(), self.replication.execution)?;
        input.append(batch, self.compiled.relational())?;
        input.seal(time)
    }
    pub(super) async fn connect_with_session(
        config: &Config,
        settings: &Settings,
        session: Session,
    ) -> Result<Self> {
        session.own(&settings.catalog_schema, &settings.query_id).await?;
        let resolved = super::lifecycle::resolve(&*session.client, config, settings).await?;
        Self::raw(&resolved.config, &resolved.settings, session).await
    }

    pub(super) async fn raw(
        config: &Config,
        settings: &Settings,
        mut session: Session,
    ) -> Result<Self> {
        settings.validate()?;
        let store = config
            .object_store
            .as_ref()
            .context("worker requires durable object storage")?
            .build(&settings.object_prefix, config.execution.object_requests)?;
        config
            .object_store
            .as_ref()
            .context("worker requires durable object storage")?
            .preflight(store.as_ref())
            .await?;
        session.own(&settings.catalog_schema, &settings.query_id).await?;
        let catalog = Catalog::new(&settings.catalog_schema, &settings.query_id)?;
        catalog.install(&session.client).await?;
        let contract = registration::source(&mut session.client, config, settings).await?;
        let mut replication = config.clone();
        replication.replication.slot = contract.slot.clone();
        let encoded_cache = match &config.encoded_cache {
            Some(cache) => Some(Arc::new(cache.open().await?)),
            None => None,
        };
        let options = EngineSettings {
            store: store.clone(),
            block_rows: settings.block_rows,
            limits: config.execution,
            encoded_cache,
        };
        let compiled = settings.query.compile_bound(&*session.client, &contract).await?;
        let mut query = relational::build_committed(&contract, &compiled, &options)?;
        if catalog
            .load_durable(&mut session.client, query.plan())
            .await
            .context("loading durable physical plan; incompatible aggregation state requires query rebuild")?
            .is_none()
        {
            registration::reset_unactivated(&session.client, &contract).await?;
            bootstrap(&replication, settings, &mut session, &catalog, &mut query).await?;
        }
        let (writer, nonce) =
            reopen(&mut session, &catalog, &mut query, (&contract, settings)).await?;
        Ok(Self {
            maintenance: super::checkpoint::debt::Controller::default(),
            checkpoint: None,
            collector: super::collection::Collector::connect(config).await?,
            settings: settings.clone(),
            session,
            catalog,
            query,
            compiled,
            writer,
            store,
            nonce,
            replication,
        })
    }
}
async fn reopen(
    session: &mut Session,
    catalog: &Catalog,
    query: &mut relational::Query,
    binding: (&Contract, &Settings),
) -> Result<(Writer, String)> {
    let (contract, settings) = binding;
    let prior = catalog
        .load_durable(&mut session.client, query.plan())
        .await?
        .context("worker has no durable activation")?;
    ensure!(
        prior.binding.source == *contract,
        "worker native source contract differs from activation"
    );
    ensure!(
        prior.binding.sink
            == settings.query.compile_bound(&*session.client, contract).await?.sink(
                contract,
                &settings.sink_table,
                settings.sink_output,
                &settings.sink_key
            )?,
        "configured worker destination differs from durable binding"
    );
    let writer = catalog.claim(&mut session.client, query.plan(), prior.binding).await?;
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
    query: &mut relational::Query,
) -> Result<()> {
    let export = Export::create(config, &config.replication.slot).await?;
    let boundary = export.consistent();
    let snapshot = export.import(session.client.client_mut()).await?;
    let contract = Contract::inspect(
        &snapshot,
        export.identity().clone(),
        &config.replication.publication,
        &config.replication.slot,
    )
    .await?;
    let compiled = settings.query.compile_bound(&snapshot, &contract).await?;
    snapshot
        .execute(
            "SELECT pg_catalog.set_config('statement_timeout',$1,true)",
            &[&format!("{}s", settings.startup_timeout_secs)],
        )
        .await?;
    let input = read_snapshot(
        &snapshot,
        &contract,
        compiled.relational(),
        (config.execution, settings.memtable_bytes),
        query,
    )
    .await?;
    snapshot.commit().await?;
    export.close().await?;
    // The preliminary registration must match the exact imported source layout.
    let compiled = settings.query.compile_bound(&*session.client, &contract).await?;
    verify_snapshot_plan(config, settings, &contract, &compiled, query)?;
    let protection = protect_bootstrap(&mut session.client, catalog).await?;
    super::drive::emit(
        &serde_json::json!({"event":"bootstrap_upload","slot":contract.slot,"boundary":boundary.to_string()}),
    )?;
    let prepared = query.prepare_bootstrap(input, &protection).await?;
    let checkpoint = query.prepared_checkpoint(&prepared)?;
    let deltas = relational::deltas(query, &prepared.output().batch, compiled.output()).await?;
    let sink =
        compiled.sink(&contract, &settings.sink_table, settings.sink_output, &settings.sink_key)?;
    catalog
        .activate_snapshot(
            &mut session.client,
            query.plan(),
            Snapshot {
                checkpoint: &checkpoint,
                deltas: &deltas,
                source: &contract,
                boundary,
                sink,
            },
        )
        .await?;
    query.commit(prepared)?;
    release_bootstrap(&mut session.client, catalog, query, protection, &checkpoint).await
}

async fn protect_bootstrap(
    sql: &mut crate::catalog::Connection,
    catalog: &Catalog,
) -> Result<crate::catalog::Protection> {
    let token: String =
        sql.query_one("SELECT pg_catalog.gen_random_uuid()::text", &[]).await?.try_get(0)?;
    catalog.protect_upload(sql, &format!("worker-bootstrap:{token}")).await
}

async fn read_snapshot(
    snapshot: &tokio_postgres::Transaction<'_>,
    contract: &Contract,
    ir: &crate::compiler::relational::Relational,
    resources: (crate::engine::execution::Limits, usize),
    query: &relational::Query,
) -> Result<crate::engine::plan::relational::Bootstrap<relational::Key, crate::transaction::Row>> {
    let (limits, budget) = resources;
    let mut input = query.bootstrap_workspace(budget)?;
    let limits = crate::engine::execution::Limits {
        resident_bytes: limits.resident_bytes.min(input.input_limits().resident_bytes),
        ..limits
    };
    let mut reader = crate::source::SnapshotReader::new(
        snapshot,
        contract,
        &ir.source_columns(contract),
        limits,
    )
    .await?;
    let mut rows = 0_usize;
    while let Some(batch) = reader.next(snapshot).await? {
        rows = rows.checked_add(batch.updates.len()).context("snapshot row count overflow")?;
        let chunk = {
            let _workspace = query.workspaces().acquire().await?;
            relational::chunk(&batch, ir)?
        };
        input.append(chunk).await?;
        if rows.is_multiple_of(limits.resident_entries.saturating_mul(128)) {
            super::drive::emit(&serde_json::json!({"event":"bootstrap_progress","rows":rows}))?;
        }
    }
    Ok(input)
}

fn verify_snapshot_plan(
    config: &Config,
    settings: &Settings,
    contract: &Contract,
    compiled: &crate::compiler::Compiled,
    query: &relational::Query,
) -> Result<()> {
    let exact = relational::build_committed(
        contract,
        compiled,
        &EngineSettings {
            store: config
                .object_store
                .as_ref()
                .context("missing storage")?
                .build(&settings.object_prefix, config.execution.object_requests)?,
            block_rows: settings.block_rows,
            limits: config.execution,
            encoded_cache: None,
        },
    )?;
    ensure!(
        exact.plan().identity() == query.plan().identity(),
        "source changed while establishing snapshot"
    );
    Ok(())
}

async fn release_bootstrap(
    sql: &mut crate::catalog::Connection,
    catalog: &Catalog,
    query: &relational::Query,
    protection: crate::catalog::Protection,
    checkpoint: &crate::engine::plan::Checkpoint,
) -> Result<()> {
    // Activation establishes fence one; claiming validates the binding and allows
    // an exact authoritative release of bootstrap PUT protection.
    let durable =
        catalog.load_durable(sql, query.plan()).await?.context("activation disappeared")?;
    let writer = catalog.claim(sql, query.plan(), durable.binding).await?;
    protection.published(sql, &writer, query.plan(), checkpoint).await
}
