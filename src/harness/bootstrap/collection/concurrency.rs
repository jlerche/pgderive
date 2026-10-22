use super::{Catalog, GcLimits, Query, Recovery, Writer, compact};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStoreExt, path::Path};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    compact(sql, graph, writer, recovery, "gc-race-old").await?;
    let stale = graph.checkpoint()?;
    compact(sql, graph, writer, recovery, "gc-race-current").await?;
    let (mut collector, connection) = crate::harness::connect(&recovery.config).await?;
    let result = paused((sql, &mut collector), catalog, graph, writer, (recovery, stale)).await;
    drop(collector);
    connection.await.context("GC race SQL connection task failed")??;
    result
}
async fn paused(
    sessions: (&mut Client, &mut Client),
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    input: (&Recovery, crate::engine::plan::Checkpoint),
) -> Result<()> {
    let (recovery, stale) = input;
    let (sql, collector) = sessions;
    let store = Arc::new(super::paused::Store::new(recovery.store.clone()));
    let collection = catalog.collect(collector, store.clone(), GcLimits::default());
    tokio::pin!(collection);
    tokio::select! {
        result = &mut collection => { result?; anyhow::bail!("GC did not pause during marking"); }
        () = store.entered.notified() => {}
        () = tokio::time::sleep(Duration::from_secs(10)) => anyhow::bail!("GC mark pause timed out"),
    }
    competing(sql, catalog, recovery).await?;
    let mut resurrection = graph.checkpoint()?;
    for (member, old) in resurrection.arrangements.iter_mut().zip(&stale.arrangements) {
        member.trace.generation =
            member.trace.generation.checked_add(1).context("GC race trace generation overflow")?;
        member.trace.objects.clone_from(&old.trace.objects);
    }
    let rejection = writer.maintain(sql, graph.plan(), &resurrection).await;
    ensure!(
        rejection.is_err_and(|error| error
            .to_string()
            .contains("new object has no active upload reservation")),
        "sealed namespace resurrection did not reach reservation validation"
    );
    let reader = timeout(
        Duration::from_secs(5),
        catalog.protect_checkpoint(sql, graph.plan(), "gc-race-reader"),
    )
    .await
    .context("GC object I/O blocked reader pin")??;
    let pinned = reader.stored()?.checkpoint.clone();
    timeout(Duration::from_secs(5), compact(sql, graph, writer, recovery, "gc-race-fresh"))
        .await
        .context("GC object I/O blocked checkpoint publication")??;
    let current = graph.checkpoint()?;
    store.release.notify_one();
    timeout(Duration::from_secs(10), &mut collection)
        .await
        .context("GC race collection timed out")??;
    graph.restore_checkpoint(pinned).await?;
    graph.snapshot().trace("output")?.materialize().await?;
    graph.restore_checkpoint(current).await?;
    reader.reader_finished(sql).await?;
    ensure!(
        recovery
            .store
            .head(&Path::from(stale.arrangements[0].trace.objects[0].path()))
            .await
            .is_err(),
        "GC race orphan survived"
    );
    eprintln!(
        "MVP paused GC permits pin/publication, rejects sealed-root resurrection and preserves fresh uploads"
    );
    Ok(())
}

async fn competing(sql: &mut Client, catalog: &Catalog, recovery: &Recovery) -> Result<()> {
    let result = timeout(
        Duration::from_secs(5),
        catalog.collect(sql, recovery.store.clone(), GcLimits::default()),
    )
    .await
    .context("concurrent collection waited on object I/O")??;
    ensure!(
        result == crate::catalog::Collection::AlreadyRunning,
        "concurrent collection was not coalesced"
    );
    Ok(())
}
