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
    compact(sql, graph, writer, "gc-first").await?;
    let reader = catalog.protect_checkpoint(sql, graph.plan(), "gc-reader").await?;
    let old = reader.stored()?.checkpoint.clone();
    let pinned = graph.snapshot();
    let expected = pinned.output.materialize().await?;
    compact(sql, graph, writer, "gc-second").await?;
    let limits = GcLimits::default();
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    ensure!(result != Collection::UploadProtected, "GC unexpectedly blocked by upload");
    graph.restore_checkpoint(old).await?;
    ensure!(
        graph.snapshot().output.materialize().await? == expected,
        "pinned cold recovery failed"
    );
    let durable = writer.confirmed(sql, graph.plan()).await?;
    graph.restore_checkpoint(durable.stored.checkpoint).await?;
    ensure!(pinned.output.materialize().await? == expected, "GC invalidated active reader");
    drop(pinned);
    reader.reader_finished(sql).await?;
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    collected(result, true)?;
    abandoned(sql, catalog, graph, writer, recovery).await?;
    ensure!(
        graph.snapshot().output.materialize().await? == expected,
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
    token: &str,
) -> Result<()> {
    let protection = writer.protect_upload(sql, token).await?;
    let prepared = graph.prepare_compaction_protected(&protection).await?;
    let checkpoint = graph.prepared_compaction_checkpoint(&prepared)?;
    graph.publish_compaction(sql, writer, prepared).await?;
    protection.published(sql, writer, graph.plan(), &checkpoint).await
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
    let checkpoint = graph.prepared_compaction_checkpoint(&prepared)?;
    let root = checkpoint.arrangements[0].trace.objects[0].clone();
    let limits = GcLimits::default();
    ensure!(
        catalog.collect(sql, recovery.store.clone(), limits).await? == Collection::UploadProtected,
        "GC swept unresolved uploads"
    );
    let before = writer.confirmed(sql, graph.plan()).await?;
    let next = catalog.claim(sql, graph.plan(), before.binding, before.end).await?;
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
        writer.maintain(sql, graph.plan(), &checkpoint).await.is_err(),
        "new writer published fenced upload"
    );
    ensure!(
        writer.seal_fenced_uploads(sql, graph.plan()).await? == 1,
        "recovery did not seal abandoned upload"
    );
    rejected_sweep(sql, catalog, graph, recovery, root.path()).await?;
    let result = catalog.collect(sql, recovery.store.clone(), limits).await?;
    collected(result, true)?;
    ensure!(
        recovery.store.head(&Path::from(root.path())).await.is_err(),
        "abandoned root survived collection"
    );
    let after = writer.confirmed(sql, graph.plan()).await?;
    ensure!(
        after.end == before.end && after.stored.checkpoint == before.stored.checkpoint,
        "GC recovery changed durable progress"
    );
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
