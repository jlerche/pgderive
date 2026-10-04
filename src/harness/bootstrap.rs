mod activation;
mod codec;
mod collection;
mod maintenance;
use super::{Fixture, apply, connect, source_state};
use crate::{
    Config,
    catalog::{Catalog, Deltas, Lsn, Snapshot, Writer},
    engine::{dataflow::Stream as Tick, plan::query::Settings},
    source::{Contract, Export},
};
use anyhow::{Context, Result, ensure};
use std::time::Duration;
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    fixture: &Fixture,
    recovery: &super::recovery::Recovery,
) -> Result<()> {
    let config = &recovery.config;
    let slot = format!("{}_snapshot", fixture.slot);
    let export = Export::create(config, &slot).await?;
    let boundary = export.consistent();
    let (contract, mut model, batch) = snapshot_copy(sql, fixture, config, export).await?;
    let mut graph = super::mvp::Query::new_with_limits(
        plan(&contract)?,
        super::mvp::operators(),
        recovery.store.clone(),
        3,
        config.execution,
    )?;
    let catalog = Catalog::new(&fixture.schema, "snapshot_grouped")?;
    let protection = activate(
        sql,
        &catalog,
        &mut graph,
        Initial { contract: &contract, boundary, batch: &batch, config },
    )
    .await?;
    let (mut graph, mut writer) = restore(sql, &catalog, graph, recovery, boundary).await?;
    protection.published(sql, &writer, graph.plan(), &graph.checkpoint()?).await?;
    let mut config = config.clone();
    config.replication.slot = slot.clone();
    let verified = writer.confirmed(sql, graph.plan()).await?;
    let mut stream = crate::acknowledged::Stream::connect(&config, &verified).await?;
    let result = catch_up(sql, &fixture.schema, &mut graph, &mut writer, &mut stream).await;
    let shutdown = stream.shutdown().await;
    let transaction = crate::outcome::combine(result, shutdown, "snapshot CDC shutdown")?;
    apply(&mut model, &transaction)?;
    ensure!(
        model == source_state(sql, &fixture.schema).await?,
        "snapshot plus CDC has a gap or duplicate"
    );
    let report = recovery.recover_query(sql, &fixture.schema, "snapshot_grouped").await?;
    ensure!(
        report.time == 2 && report.epoch == 2,
        "fresh process recovered wrong snapshot/CDC boundary"
    );
    maintenance::check(sql, &fixture.schema, &mut graph, &mut writer, recovery).await?;
    collection::check(sql, &catalog, &mut graph, &mut writer, recovery).await?;
    finish_source(sql, fixture, &config, &catalog, graph.plan()).await?;
    eprintln!(
        "MVP exported snapshot plus concurrent insert/update/delete CDC reconstructed exact source state; native schema drift rejected"
    );
    Ok(())
}
async fn finish_source(
    sql: &mut Client,
    fixture: &Fixture,
    config: &Config,
    catalog: &Catalog,
    plan: &crate::engine::plan::Plan,
) -> Result<()> {
    let durable = catalog.load_durable(sql, plan).await?.context("missing source contract")?;
    let contract = durable.binding.registered_source()?.context("missing native contract")?;
    schema_drift(sql, &fixture.schema, &contract).await?;
    advanced_slot(sql, config, catalog, plan).await?;
    sql.query_one("SELECT pg_drop_replication_slot($1)", &[&config.replication.slot]).await?;
    codec::check(sql, fixture, config).await?;
    Ok(())
}
async fn restore(
    sql: &mut Client,
    catalog: &Catalog,
    mut graph: super::mvp::Query,
    recovery: &super::recovery::Recovery,
    boundary: Lsn,
) -> Result<(super::mvp::Query, Writer)> {
    let durable =
        catalog.load_durable(sql, graph.plan()).await?.context("snapshot activation missing")?;
    ensure!(
        durable.stored.checkpoint.time == 1 && durable.xid.is_none() && durable.end == boundary,
        "snapshot fabricated a source transaction"
    );
    graph = super::mvp::Query::reopen(
        graph.plan().clone(),
        super::mvp::operators(),
        Settings {
            store: recovery.store.clone(),
            block_rows: 3,
            limits: recovery.config.execution,
        },
        durable.stored.checkpoint,
    )
    .await?;
    let writer = catalog.claim(sql, graph.plan(), durable.binding, boundary).await?;
    Ok((graph, writer))
}
pub(super) fn plan(contract: &Contract) -> Result<crate::engine::plan::Plan> {
    let mut definition = super::mvp::registered_plan()?.definition().clone();
    let identity = contract.digest()?;
    definition.revision = format!("snapshot-count-sum-v1:{identity}");
    for source in &mut definition.sources {
        source.schema = format!("{}:{identity}", source.schema);
    }
    for node in &mut definition.nodes {
        if node.kind == crate::engine::plan::Kind::Source {
            node.schema = format!("{}:{identity}", node.schema);
        }
    }
    crate::engine::plan::Plan::new(definition)
}
struct Initial<'a> {
    contract: &'a Contract,
    boundary: Lsn,
    batch: &'a crate::weighted::Batch,
    config: &'a Config,
}
async fn activate(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut super::mvp::Query,
    initial: Initial<'_>,
) -> Result<crate::catalog::Protection> {
    let Initial { contract, boundary, batch, config } = initial;
    let protection = catalog.protect_upload(sql, "snapshot-upload").await?;
    let prepared = graph
        .prepare_protected(
            Tick {
                time: 1,
                batch: (
                    super::mvp::input(batch, "auction", "id")?,
                    super::mvp::input(batch, "bid", "auction")?,
                ),
            },
            &protection,
        )
        .await?;
    let checkpoint = graph.prepared_checkpoint(&prepared)?;
    let deltas = Deltas::grouped(&prepared.output().batch)?;
    let snapshot = Snapshot {
        checkpoint: &checkpoint,
        deltas: &deltas,
        source: contract,
        boundary,
        sink: crate::catalog::Sink::Grouped("snapshot_groups".into()),
    };
    activation::check(sql, catalog, graph.plan(), snapshot, config).await?;
    graph.commit(prepared)?;
    Ok(protection)
}
async fn catch_up(
    sql: &mut Client,
    schema: &str,
    graph: &mut super::mvp::Query,
    writer: &mut Writer,
    stream: &mut crate::acknowledged::Stream,
) -> Result<crate::transaction::Transaction> {
    let transaction = tokio::time::timeout(Duration::from_secs(20), stream.recv())
        .await
        .context("snapshot CDC timed out")??;
    ensure!(transaction.changes.len() == 5, "snapshot boundary lost concurrent source changes");
    let protection = writer.protect_upload(sql, "snapshot-cdc-upload").await?;
    let prepared = graph
        .prepare_protected(
            Tick {
                time: 2,
                batch: (
                    super::mvp::input(&transaction.batch, "auction", "id")?,
                    super::mvp::input(&transaction.batch, "bid", "auction")?,
                ),
            },
            &protection,
        )
        .await?;
    let progress = crate::catalog::Progress::new(
        transaction.xid,
        &transaction.commit_lsn,
        &transaction.end_lsn,
    )?;
    let checkpoint = graph.prepared_checkpoint(&prepared)?;
    graph.publish_prepared(sql, writer, prepared, &progress).await?;
    protection.published(sql, writer, graph.plan(), &checkpoint).await?;
    stream.acknowledge(sql, writer, graph.plan()).await?;
    reject_publication_drift(sql, schema, graph, writer).await?;
    let different: bool = sql.query_one(&format!("SELECT EXISTS((SELECT group_key,row_count,total FROM {schema}.snapshot_groups EXCEPT SELECT COALESCE(to_jsonb(a.category::text),'null'::jsonb),COUNT(*),SUM(b.price)::bigint FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction GROUP BY a.category) UNION ALL (SELECT COALESCE(to_jsonb(a.category::text),'null'::jsonb),COUNT(*),SUM(b.price)::bigint FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction GROUP BY a.category EXCEPT SELECT group_key,row_count,total FROM {schema}.snapshot_groups))"), &[]).await?.try_get(0)?;
    ensure!(!different, "snapshot/CDC sink differs from SQL");
    Ok(transaction)
}

async fn reject_publication_drift(
    sql: &mut Client,
    schema: &str,
    graph: &super::mvp::Query,
    writer: &mut Writer,
) -> Result<()> {
    let before = writer.confirmed(sql, graph.plan()).await?;
    let prepared = graph
        .prepare(Tick {
            time: 3,
            batch: (
                crate::engine::Batch::from_updates([])?,
                crate::engine::Batch::from_updates([])?,
            ),
        })
        .await?;
    let checkpoint = graph.prepared_checkpoint(&prepared)?;
    let deltas = Deltas::grouped(&prepared.output().batch)?;
    sql.batch_execute(&format!("ALTER TABLE {schema}.person DROP CONSTRAINT person_pkey")).await?;
    let end: String = sql.query_one("SELECT pg_current_wal_lsn()::text", &[]).await?.try_get(0)?;
    let progress = crate::catalog::Progress::new(1, &end, &end)?;
    ensure!(
        writer
            .publish(
                sql,
                graph.plan(),
                crate::catalog::Publication {
                    checkpoint: &checkpoint,
                    deltas: &deltas,
                    progress: &progress
                }
            )
            .await
            .is_err(),
        "source PK drift published a result/source boundary"
    );
    let after = writer.confirmed(sql, graph.plan()).await?;
    ensure!(
        after.stored.checkpoint == before.stored.checkpoint
            && after.end == before.end
            && after.stored.epoch == before.stored.epoch,
        "source schema drift changed durable progress"
    );
    sql.batch_execute(&format!("ALTER TABLE {schema}.person ADD PRIMARY KEY(id)")).await?;
    eprintln!("MVP source schema drift blocked publication and preserved durable boundary");
    Ok(())
}

async fn schema_drift(sql: &Client, schema: &str, contract: &Contract) -> Result<()> {
    // Under FULL, PK membership is absent from pgoutput Relation; inspect native metadata.
    sql.batch_execute(&format!("ALTER TABLE {schema}.person DROP CONSTRAINT person_pkey")).await?;
    ensure!(contract.verify(sql).await.is_err(), "source contract accepted primary-key drift");
    sql.batch_execute(&format!("ALTER TABLE {schema}.person ADD PRIMARY KEY(id)")).await?;
    contract.verify(sql).await?;
    sql.batch_execute(&format!(
        "ALTER PUBLICATION {} SET (publish='insert,update,delete')",
        contract.publication
    ))
    .await?;
    ensure!(
        contract.verify(sql).await.is_err(),
        "source contract accepted silent TRUNCATE omission"
    );
    sql.batch_execute(&format!(
        "ALTER PUBLICATION {} SET (publish='insert,update,delete,truncate')",
        contract.publication
    ))
    .await?;
    contract.verify(sql).await?;
    Ok(())
}
async fn advanced_slot(
    sql: &mut Client,
    config: &Config,
    catalog: &Catalog,
    plan: &crate::engine::plan::Plan,
) -> Result<()> {
    let durable =
        catalog.load_durable(sql, plan).await?.context("missing slot test durable boundary")?;
    sql.query_one(
        "SELECT pg_replication_slot_advance($1,pg_current_wal_lsn())",
        &[&config.replication.slot],
    )
    .await?;
    ensure!(
        crate::acknowledged::Stream::connect(config, &durable).await.is_err(),
        "externally advanced source slot silently skipped input"
    );
    sql.query_one("SELECT pg_drop_replication_slot($1)", &[&config.replication.slot]).await?;
    let replacement = Export::create(config, &config.replication.slot).await?;
    replacement.close().await?;
    ensure!(
        crate::acknowledged::Stream::connect(config, &durable).await.is_err(),
        "recreated source slot silently skipped input"
    );
    eprintln!(
        "MVP externally advanced and recreated source slots rejected before replication starts"
    );
    Ok(())
}

async fn snapshot_copy(
    sql: &Client,
    fixture: &Fixture,
    config: &Config,
    export: Export,
) -> Result<(Contract, super::Model, crate::weighted::Batch)> {
    let baseline = source_state(sql, &fixture.schema).await?;
    let (mut reader, task) = connect(config).await?;
    let snapshot = export.import(&mut reader).await?;
    let contract = Contract::inspect(
        &snapshot,
        export.identity().clone(),
        &fixture.publication,
        export.slot(),
    )
    .await?;
    ensure!(contract.relations.len() == 3, "snapshot source contract lost a relation");
    ensure!(contract.digest()? == contract.clone().digest()?, "unstable source contract identity");
    let schema = &fixture.schema;
    sql.batch_execute(&format!("BEGIN; INSERT INTO {schema}.person VALUES(1001,'after snapshot'); UPDATE {schema}.person SET name='changed after snapshot' WHERE id=999; DELETE FROM {schema}.person WHERE id=1001; UPDATE {schema}.auction SET category=123 WHERE id=1; UPDATE {schema}.bid SET price=7 WHERE id=3; COMMIT")).await?;
    let model = source_state(&snapshot, schema).await?;
    ensure!(model == baseline, "snapshot copy included post-boundary writes");
    let batch = crate::source::copy(&snapshot, &contract, config.execution).await?;
    snapshot.commit().await?;
    export.close().await?;
    drop(reader);
    task.await.context("snapshot SQL driver failed")??;
    Ok((contract, model, batch))
}
