//! Owned-fixture proof for finished-upload ledger retirement and token generations.
use super::super::{mvp::Query, recovery::Recovery};
use crate::catalog::{Catalog, GcLimits, Writer};
use anyhow::{Result, ensure};
use object_store::{ObjectStoreExt, path::Path};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    let protection = writer.protect_upload(sql, "gc-empty-finished").await?;
    protection.published(sql, writer, graph.plan(), &graph.checkpoint()?).await?;
    catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    let reused = writer.protect_upload(sql, "gc-empty-finished").await?;
    let namespace = reused.namespace()?.to_owned();
    let unknown = Path::from(format!("{namespace}/unknown-format"));
    recovery.store.put(&unknown, b"owned unknown fixture".to_vec().into()).await?;
    reused.published(sql, writer, graph.plan(), &graph.checkpoint()?).await?;
    catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    recovery.store.head(&unknown).await?;
    ensure!(
        writer.protect_upload(sql, "gc-empty-finished").await.is_err(),
        "unknown namespace contents lost ledger"
    );
    recovery.store.delete(&unknown).await?;
    catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    let reused = writer.protect_upload(sql, "gc-empty-finished").await?;
    reused.published(sql, writer, graph.plan(), &graph.checkpoint()?).await?;
    stale_handle(sql, catalog, graph, writer, recovery).await
}
async fn stale_handle(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    let old = writer.protect_recovery(sql, graph.plan(), "gc-token-generation").await?;
    let prior = writer.confirmed(sql, graph.plan()).await?;
    let next = catalog.claim(sql, graph.plan(), prior.binding, prior.end).await?;
    *writer = next;
    ensure!(
        writer.seal_fenced_recovery(sql, graph.plan()).await? == 1,
        "old private recovery pin not sealed"
    );
    catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    let current = catalog.protect_checkpoint(sql, graph.plan(), "gc-token-generation").await?;
    ensure!(old.reader_finished(sql).await.is_err(), "stale handle closed reused token");
    // The new protection remains active and can close exactly its own generation.
    current.reader_finished(sql).await?;
    catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    Ok(())
}
