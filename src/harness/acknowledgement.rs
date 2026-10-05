use super::{Fixture, mvp};
use crate::{
    Config,
    acknowledged::Stream,
    catalog::{Catalog, Progress},
    engine::{dataflow::TimedBatch as Tick, plan::query::Settings},
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::{sync::Arc, time::Duration};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    fixture: &Fixture,
    config: &Config,
    store: Arc<dyn ObjectStore>,
) -> Result<crate::catalog::Lsn> {
    let plan = mvp::registered_plan()?;
    let catalog = Catalog::new(&fixture.schema, "atomic_grouped")?;
    let durable =
        catalog.load_durable(sql, &plan).await?.context("missing durable resume boundary")?;
    let mut graph = mvp::Query::reopen(
        plan,
        mvp::operators(),
        Settings { store, block_rows: 3, limits: config.execution },
        durable.stored.checkpoint.clone(),
    )
    .await?;
    let stale = catalog.claim(sql, graph.plan(), durable.binding.clone(), durable.end).await?;
    let mut writer = catalog.claim(sql, graph.plan(), durable.binding, durable.end).await?;
    let verified = writer.confirmed(sql, graph.plan()).await?;
    let mut wrong = verified.clone();
    wrong.binding.source.push_str(":different");
    ensure!(
        Stream::connect(config, &wrong).await.is_err(),
        "resume accepted a different source registration"
    );
    let mut stream = Stream::connect(config, &verified).await?;
    ensure!(
        stream.acknowledge(sql, &stale, graph.plan()).await.is_err(),
        "replaced writer authorized acknowledgement"
    );

    let result = exercise(sql, fixture, &mut graph, &mut writer, &mut stream).await;
    let shutdown = stream.shutdown().await;
    crate::outcome::combine(result, shutdown, "acknowledged source shutdown")
}
async fn exercise(
    sql: &mut Client,
    fixture: &Fixture,
    graph: &mut mvp::Query,
    writer: &mut crate::catalog::Writer,
    stream: &mut Stream,
) -> Result<crate::catalog::Lsn> {
    sql.batch_execute(&format!("INSERT INTO {}.person VALUES(999,'durable ack')", fixture.schema))
        .await?;
    let transaction = tokio::time::timeout(Duration::from_secs(20), stream.recv())
        .await
        .context("waiting for acknowledged source transaction")??;
    let progress = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
    ensure!(
        transaction.changes.len() == 1 && transaction.changes[0].table == "person",
        "resume replayed previously published source changes"
    );
    // Let pgwire send periodic feedback while this transaction is merely received.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let received = flush(sql, &fixture.slot).await?;
    ensure!(received < progress.end, "receiving an unprocessed transaction acknowledged it");
    stream.acknowledge(sql, writer, graph.plan()).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    ensure!(
        flush(sql, &fixture.slot).await? < progress.end,
        "old durable capability acknowledged newer received transaction"
    );
    let prepared = graph
        .prepare(Tick {
            time: graph.time() + 1,
            batch: (
                mvp::input(&transaction.batch, "auction", "id")?,
                mvp::input(&transaction.batch, "bid", "auction")?,
            ),
        })
        .await?;
    graph.publish_prepared(sql, writer, prepared, &progress).await?;
    stream.acknowledge(sql, writer, graph.plan()).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if flush(sql, &fixture.slot).await? >= progress.end {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("waiting for durable replication acknowledgement")??;
    ensure!(graph.time() == 16, "resume advanced wrong logical clock");
    eprintln!(
        "MVP resumed from durable tick 15; received rows remained unacknowledged until atomic tick 16 publication"
    );
    Ok(progress.end)
}
async fn flush(sql: &Client, slot: &str) -> Result<crate::catalog::Lsn> {
    sql.query_one(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
        &[&slot],
    )
    .await?
    .try_get::<_, String>(0)?
    .parse()
}
