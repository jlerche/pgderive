use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    reader::BlockCache,
};
use anyhow::Result;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

fn append(
    trace: &TraceSnapshot<i64, i64>,
    time: u64,
    weight: i64,
) -> Result<TraceSnapshot<i64, i64>> {
    trace.append_validated(Run::Memory(Arc::new(Batch::from_updates([((1, 1), weight)])?)), time)
}

#[tokio::test]
async fn flush_preserves_object_membership_and_signed_memory_identity() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "flush-v1".into(), 1)?;
    let base = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((1, 1), 5), ((2, 2), 1)])? },
        )
        .await?;
    let root = base.manifest()?.objects;
    let pending = append(&append(&base, 2, -2)?, 3, 1)?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(flushed.time(), pending.time());
    assert_eq!(flushed.generation(), pending.generation() + 1);
    assert_eq!(flushed.manifest()?.objects.first(), root.first());
    assert_eq!(flushed.materialize().await?, pending.materialize().await?);
    assert_eq!(flushed.memory_charge(), 0);
    let restored = TraceSnapshot::<i64, i64>::reopen(
        store,
        flushed.manifest()?,
        "flush-v1",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(restored.materialize().await?, flushed.materialize().await?);
    let zero = append(&append(&base, 2, 1)?, 3, -1)?;
    assert_eq!(writer.flush(&zero).await?.manifest()?.objects, root);
    assert_eq!(writer.flush(&base).await?.manifest()?, base.manifest()?);
    Ok(())
}

#[tokio::test]
async fn subset_overflow_cancels_against_retained_object_state() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "flush-v1".into(), 1)?;
    let base = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((1, 1), -i64::MAX)])? },
        )
        .await?;
    let pending = append(&append(&base, 2, i64::MAX)?, 3, i64::MAX)?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(flushed.materialize().await?, Batch::from_updates([((1, 1), i64::MAX)])?);
    assert_eq!(flushed.run_count(), 1);
    Ok(())
}

#[tokio::test]
async fn repeated_flushes_keep_merge_fan_in_bounded() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "flush-v1".into(), 1)?;
    let mut trace = writer.empty();
    for time in 1..=24 {
        trace = writer.flush(&append(&trace, time, 1)?).await?;
        assert!(trace.run_count() <= 8);
        assert_eq!(
            trace.materialize().await?,
            Batch::from_updates([((1, 1), i64::try_from(time)?)])?
        );
    }
    Ok(())
}

#[tokio::test]
async fn flush_never_reads_retained_blocks_and_preserves_duplicate_membership() -> Result<()> {
    use crate::engine::reader::ObjectBatch;
    use anyhow::Context;
    use object_store::{ObjectStoreExt, path::Path};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "flush-v1".into(), 1)?;
    let reference =
        ObjectBatch::write(store.clone(), &Batch::from_updates([((1, 1), 2)])?, "flush-v1", 1)
            .await?;
    let object = ObjectBatch::open(store.clone(), reference.clone(), "flush-v1").await?;
    let block = Path::from(object.block_range(0).map(|(path, _)| path).context("missing block")?);
    let bytes = store.get(&block).await?.bytes().await?;
    let base = writer
        .empty()
        .append_validated(Run::Object(object.clone()), 1)?
        .append_validated(Run::Object(object), 2)?;
    let pending = append(&base, 3, -1)?;
    store.delete(&block).await?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(&flushed.manifest()?.objects[..2], &[reference.clone(), reference]);
    store.put(&block, bytes.into()).await?;
    assert_eq!(flushed.materialize().await?, Batch::from_updates([((1, 1), 3)])?);
    assert_eq!(pending.materialize().await?, flushed.materialize().await?);
    Ok(())
}

#[tokio::test]
async fn wide_memory_delta_uses_byte_bounded_blocks_and_cold_reopens() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, String>::new(store.clone(), "wide-flush-v1".into(), 4096)?;
    let batch = Batch::from_updates((0..4096).map(|key| ((key, "x".repeat(4096)), 1)))?;
    let pending = writer.empty().append_validated(Run::Memory(Arc::new(batch.clone())), 1)?;
    let flushed = writer.flush(&pending).await?;
    let restored = writer.reopen(flushed.manifest()?).await?;
    assert_eq!(restored.materialize().await?, batch);
    Ok(())
}

#[tokio::test]
async fn density_boundary_compacts_only_above_half_the_retained_rows() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "density-v1".into(), 2)?;
    let base = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates((0..4).map(|key| ((key, key), 2)))? },
        )
        .await?;
    for entries in [2, 3] {
        let delta = Batch::from_updates((0..entries).map(|key| ((key, key), -1)))?;
        let pending = base.append_validated(Run::Memory(Arc::new(delta)), 2)?;
        let flushed = writer.flush(&pending).await?;
        assert_eq!(flushed.run_count(), if entries == 2 { 2 } else { 1 });
        assert_eq!(flushed.materialize().await?, pending.materialize().await?);
        assert_eq!(
            writer.reopen(flushed.manifest()?).await?.materialize().await?,
            pending.materialize().await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn canceling_memory_never_reads_eight_retained_runs() -> Result<()> {
    use crate::engine::reader::ObjectBatch;
    use anyhow::Context;
    use object_store::{ObjectStoreExt, path::Path};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "zero-v1".into(), 1)?;
    let reference =
        ObjectBatch::write(store.clone(), &Batch::from_updates([((1, 1), 2)])?, "zero-v1", 1)
            .await?;
    let object = ObjectBatch::open(store.clone(), reference, "zero-v1").await?;
    let block = Path::from(object.block_range(0).map(|(path, _)| path).context("missing block")?);
    let bytes = store.get(&block).await?.bytes().await?;
    let mut base = writer.empty();
    for time in 1..=8 {
        base = base.append_validated(Run::Object(object.clone()), time)?;
    }
    let pending = append(&append(&base, 9, 1)?, 10, -1)?;
    store.delete(&block).await?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(flushed.manifest()?.objects, base.manifest()?.objects);
    assert_eq!(flushed.time(), 10);
    assert_eq!(flushed.generation(), pending.generation() + 1);
    assert_eq!(flushed.memory_charge(), 0);
    store.put(&block, bytes.into()).await?;
    assert_eq!(flushed.materialize().await?, Batch::from_updates([((1, 1), 16)])?);
    Ok(())
}
