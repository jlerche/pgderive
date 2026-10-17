use super::super::{mvp::Query, recovery::Recovery};
use crate::catalog::{Catalog, Collection, GcLimits, Writer};
use anyhow::{Result, ensure};
use object_store::{ObjectStoreExt, path::Path};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    compact(sql, graph, writer, recovery, "gc-first").await?;
    let reader = catalog.protect_checkpoint(sql, graph.plan(), "gc-reader").await?;
    let old = reader.stored()?.checkpoint.clone();
    let pinned = graph.snapshot();
    let expected = pinned.trace("output")?.materialize().await?;
    compact(sql, graph, writer, recovery, "gc-second").await?;
    let limits = GcLimits::default();
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    ensure!(result != Collection::UploadProtected, "GC unexpectedly blocked by upload");
    graph.restore_checkpoint(old).await?;
    ensure!(
        graph.snapshot().trace("output")?.materialize().await? == expected,
        "pinned cold recovery failed"
    );
    let durable = writer.confirmed(sql, graph.plan()).await?;
    graph.restore_checkpoint(durable.stored.checkpoint).await?;
    ensure!(
        pinned.trace("output")?.materialize().await? == expected,
        "GC invalidated active reader"
    );
    drop(pinned);
    reader.reader_finished(sql).await?;
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    collected(result, true)?;
    abandoned_and_ledger(sql, catalog, graph, writer, recovery).await?;
    ensure!(
        graph.snapshot().trace("output")?.materialize().await? == expected,
        "GC changed visible weighted state"
    );
    eprintln!(
        "MVP GC retained committed and pinned state; fenced abandoned uploads reclaimed without source/logical advance"
    );
    Ok(())
}

async fn compact(
    sql: &mut Client,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
    token: &str,
) -> Result<()> {
    let protection = writer.protect_upload(sql, token).await?;
    let checkpoint = rewrite(graph.checkpoint()?, &protection, recovery).await?;
    writer.maintain(sql, graph.plan(), &checkpoint).await?;
    graph.restore_checkpoint(checkpoint.clone()).await?;
    protection.published(sql, writer, graph.plan(), &checkpoint).await
}

async fn rewrite(
    mut checkpoint: crate::engine::plan::Checkpoint,
    protection: &crate::catalog::Protection,
    recovery: &Recovery,
) -> Result<crate::engine::plan::Checkpoint> {
    // Selective runtime compaction may reuse every singleton. A detached rewrite
    // gives this fixture distinct live, pinned and abandoned physical objects.
    let member = &mut checkpoint.arrangements[0];
    let arrangement = crate::engine::dataflow::Arrangement::<String, crate::transaction::Row>::new(
        recovery.store.clone(),
        member.schema.clone(),
        3,
    )?
    .with_namespace(protection.namespace()?);
    let prior = arrangement.reopen(member.trace.clone()).await?;
    member.trace = arrangement.compact(&prior).await?.manifest()?;
    Ok(checkpoint)
}

async fn abandoned(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    let protection = writer.protect_upload(sql, "gc-abandoned").await?;
    let prepared = graph.prepare_compaction_protected(&protection).await?;
    let checkpoint =
        rewrite(graph.prepared_compaction_checkpoint(&prepared)?, &protection, recovery).await?;
    let root = checkpoint.arrangements[0].trace.objects[0].clone();
    let orphan_data = recovery.store.get(&Path::from(root.path())).await?.bytes().await?.to_vec();
    let limits = GcLimits::default();
    ensure!(
        catalog.collect(sql, recovery.store.clone(), limits).await? == Collection::UploadProtected,
        "GC swept unresolved uploads"
    );
    let before = fence_abandoned(sql, catalog, writer, graph.plan(), &checkpoint).await?;
    rejected_sweep(sql, catalog, graph, recovery, root.path()).await?;
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    collected(result, true)?;
    ensure!(
        recovery.store.head(&Path::from(root.path())).await.is_err(),
        "abandoned root survived collection"
    );
    late_put(sql, catalog, recovery, root.path(), orphan_data).await?;
    let after = writer.confirmed(sql, graph.plan()).await?;
    ensure!(
        after.end == before.end && after.stored.checkpoint == before.stored.checkpoint,
        "GC recovery changed durable progress"
    );
    graph.restore_checkpoint(after.stored.checkpoint.clone()).await?;
    ensure!(
        graph.publish_compaction(sql, writer, prepared).await.is_err(),
        "closed namespace was resurrected"
    );
    Ok(())
}

fn collected(result: Collection, expected_deletion: bool) -> Result<()> {
    let Collection::Collected { deleted, .. } = result else {
        anyhow::bail!("GC unexpectedly blocked by upload");
    };
    ensure!((deleted > 0) == expected_deletion, "GC deleted unexpected object count");
    Ok(())
}

async fn rejected_sweep(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &Query,
    recovery: &Recovery,
    orphan: &str,
) -> Result<()> {
    let limits = GcLimits::default();
    ensure!(
        catalog
            .collect(sql, recovery.store.clone(), GcLimits { objects: 1, ..limits })
            .await
            .is_err(),
        "GC ignored metadata budget"
    );
    recovery.store.head(&Path::from(orphan)).await?;
    let checkpoint = graph.checkpoint()?;
    let current = &checkpoint.arrangements[0].trace.objects[0];
    let path = Path::from(current.path());
    let original = recovery.store.get(&path).await?.bytes().await?;
    recovery.store.delete(&path).await?;
    let rejected = catalog.collect(sql, recovery.store.clone(), limits).await.is_err();
    let orphan_retained = recovery.store.head(&Path::from(orphan)).await.is_ok();
    recovery.store.put(&path, original.into()).await?;
    ensure!(rejected && orphan_retained, "missing committed root allowed destructive sweep");
    Ok(())
}

async fn late_put(
    sql: &mut Client,
    catalog: &Catalog,
    recovery: &Recovery,
    path: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let path = Path::from(path);
    recovery.store.put(&path, bytes.into()).await?;
    let collected = catalog.collect(sql, recovery.store.clone(), GcLimits::default()).await?;
    ensure!(
        matches!(collected,Collection::Collected {deleted,..} if deleted>0),
        "fenced late PUT was no longer collectible"
    );
    ensure!(recovery.store.head(&path).await.is_err(), "late orphan survived collection");
    Ok(())
}

async fn fence_abandoned(
    sql: &mut Client,
    catalog: &Catalog,
    writer: &mut Writer,
    plan: &crate::engine::plan::Plan,
    checkpoint: &crate::engine::plan::Checkpoint,
) -> Result<crate::catalog::Durable> {
    let before = writer.confirmed(sql, plan).await?;
    let next = catalog.claim(sql, plan, before.binding.clone()).await?;
    let stale = std::mem::replace(writer, next);
    ensure!(
        stale.protect_upload(sql, "gc-stale").await.is_err(),
        "stale writer reserved at new fence"
    );
    ensure!(
        catalog.protect_upload(sql, "gc-bootstrap-bypass").await.is_err(),
        "bootstrap reservation bypassed active writer"
    );
    ensure!(
        writer.maintain(sql, plan, checkpoint).await.is_err(),
        "new writer published fenced upload"
    );
    ensure!(
        writer.seal_fenced_uploads(sql, plan).await? == 1,
        "recovery did not seal abandoned upload"
    );
    Ok(before)
}

async fn abandoned_and_ledger(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    abandoned(sql, catalog, graph, writer, recovery).await?;
    super::ledger::check(sql, catalog, graph, writer, recovery).await
}
