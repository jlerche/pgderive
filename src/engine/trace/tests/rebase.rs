use super::*;
use crate::engine::{Batch, dataflow::Arrangement, reader::ObjectBatch};
use object_store::memory::InMemory;

fn append(trace: &TraceSnapshot<i64, i64>, weight: i64) -> Result<TraceSnapshot<i64, i64>> {
    trace.append_validated(
        Run::Memory(Arc::new(Batch::from_updates([((1, 1), weight)])?)),
        trace.time() + 1,
    )
}

#[tokio::test]
async fn uploaded_prefix_preserves_newer_retractions_and_duplicate_objects() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::new(store.clone(), "rebase-v1".into(), 16)?;
    let reference =
        ObjectBatch::write(store.clone(), &Batch::from_updates([((1, 1), 2)])?, "rebase-v1", 16)
            .await?;
    let object = ObjectBatch::open(store, reference, "rebase-v1").await?;
    let base = writer
        .empty()
        .append_validated(Run::Object(object.clone()), 1)?
        .append_validated(Run::Object(object), 2)?;
    let captured = append(&base, 3)?.seal_memory();
    let flushed = writer.flush(&captured).await?;
    let current = append(&append(&captured, -2)?, 4)?;
    let rebased = current.rebase_flushed(&captured, &flushed)?;
    assert_eq!(rebased.time(), current.time());
    assert_eq!(rebased.generation(), current.generation() + 1);
    assert_eq!(rebased.materialize().await?, current.materialize().await?);
    assert_eq!(rebased.materialize().await?, Batch::from_updates([((1, 1), 9)])?);
    assert!(rebased.memory_charge() < current.memory_charge());
    assert_eq!(captured.materialize().await?, Batch::from_updates([((1, 1), 7)])?);
    assert!(rebased.rebase_flushed(&captured, &flushed).is_err());
    Ok(())
}

#[tokio::test]
async fn prefix_rebase_rejects_unrelated_memory_clocks_and_unflushed_state() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "rebase-v1".into(), 16)?;
    let captured = append(&writer.empty(), 1)?.seal_memory();
    let flushed = writer.flush(&captured).await?;
    let current = append(&captured, 2)?;
    let unrelated = append(&writer.empty(), 1)?;
    assert!(current.rebase_flushed(&unrelated, &flushed).is_err());
    assert!(current.rebase_flushed(&captured, &captured).is_err());
    assert!(captured.rebase_flushed(&current, &flushed).is_err());
    assert!(current.rebase_flushed(&captured, &writer.empty()).is_err());
    let same_boundary = captured.rebase_flushed(&captured, &flushed)?;
    assert_eq!(same_boundary.materialize().await?, captured.materialize().await?);
    assert_eq!(same_boundary.memory_charge(), 0);
    Ok(())
}

#[tokio::test]
async fn rebase_rejects_physical_replacement() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "rebase-v1".into(), 16)?;
    let captured = append(&writer.empty(), 1)?.seal_memory();
    let flushed = writer.flush(&captured).await?;
    let current = append(&captured, 2)?;
    let rewritten = writer.compact(&current).await?;
    assert!(rewritten.rebase_flushed(&captured, &flushed).is_err());
    Ok(())
}

#[tokio::test]
async fn maximum_uploaded_roots_plus_bounded_suffix_rebases_and_flushes() -> Result<()> {
    use crate::engine::reader::{BatchReader as _, WriteLimits};
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::new(store.clone(), "rebase-v1".into(), 16)?;
    let batch = Batch::from_updates((1..=128).map(|key| ((key, key), 1)))?;
    let captured =
        writer.empty().append_validated(Run::Memory(Arc::new(batch.clone())), 1)?.seal_memory();
    let limits = WriteLimits {
        block_bytes: 128,
        block_rows: 1,
        index_bytes: 1024,
        index_blocks: 1,
        output_roots: 128,
    };
    let references =
        ObjectBatch::write_stream(store.clone(), &mut batch.cursor().await?, "rebase-v1", limits)
            .await?;
    assert_eq!(references.len(), 128);
    let mut roots = Vec::new();
    for reference in references {
        roots.push(Run::Object(ObjectBatch::open(store.clone(), reference, "rebase-v1").await?));
    }
    let flushed = TraceSnapshot::from_runs(roots, 2, 1)?;
    let mut current = captured.clone();
    for _ in 0..120 {
        current = append(&current, 1)?;
    }
    let rebased = current.rebase_flushed(&captured, &flushed)?;
    assert_eq!(rebased.run_count(), 132);
    assert_eq!(rebased.materialize().await?, current.materialize().await?);
    let next = writer.flush(&rebased).await?;
    assert!(next.run_count() <= super::super::MAX_RUNS);
    let next = writer.consolidate(&next).await?;
    assert!(next.run_count() <= 128);
    assert_eq!(next.materialize().await?, current.materialize().await?);
    Ok(())
}

#[tokio::test]
async fn expanding_prefix_keeps_room_for_unmergeable_suffix_runs() -> Result<()> {
    use crate::engine::reader::{BatchReader as _, WriteLimits};
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::new(store.clone(), "rebase-v1".into(), 16)?;
    let batch = Batch::from_updates((1..=128).map(|key| ((key, 1), -i64::MAX)))?;
    let captured =
        writer.empty().append_validated(Run::Memory(Arc::new(batch.clone())), 1)?.seal_memory();
    let references = ObjectBatch::write_stream(
        store.clone(),
        &mut batch.cursor().await?,
        "rebase-v1",
        WriteLimits {
            block_bytes: 128,
            block_rows: 1,
            index_bytes: 1024,
            index_blocks: 1,
            output_roots: 128,
        },
    )
    .await?;
    let mut roots = Vec::new();
    for reference in references {
        roots.push(Run::Object(ObjectBatch::open(store.clone(), reference, "rebase-v1").await?));
    }
    assert_eq!(roots.len(), 128);
    let flushed = TraceSnapshot::from_runs(roots, 2, 1)?;
    let mut current = captured.clone();
    for key in 1..=120 {
        // Adjacent chunks share an overflowing positive coefficient, but
        // retained negative prefix weights keep every complete state valid.
        let delta = Batch::from_updates([((key, 1), i64::MAX), ((key + 1, 1), i64::MAX)])?;
        current.validate_delta(&delta).await?;
        current = current.append_validated(Run::Memory(Arc::new(delta)), current.time() + 1)?;
    }
    assert_eq!(current.run_count(), 121);
    assert_eq!(current.unsealed_run_count(), 120);
    let rebased = current.rebase_flushed(&captured, &flushed)?;
    assert_eq!(rebased.run_count(), 248);
    assert_eq!(rebased.unsealed_run_count(), 120);
    assert_eq!(rebased.materialize().await?, current.materialize().await?);
    let next = writer.flush(&rebased).await?;
    assert!(next.run_count() <= super::super::MAX_RUNS);
    let next = writer.consolidate(&next).await?;
    assert!(next.run_count() <= 128);
    assert_eq!(next.materialize().await?, current.materialize().await?);
    Ok(())
}

#[tokio::test]
async fn independent_checkpoint_reserves_maximum_roots_and_unmergeable_suffix() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::new(store.clone(), "headroom".into(), 16)?;
    let root =
        ObjectBatch::write(store.clone(), &Batch::from_updates([((1, 1), -1)])?, "headroom", 16)
            .await?;
    let object = ObjectBatch::open(store, root, "headroom").await?;
    let base = TraceSnapshot::from_runs(vec![Run::Object(object.clone()); 128], 1, 1)?;
    assert!(!base.consolidation_due()?); // One irreducible multipack cohort.
    assert_eq!(writer.consolidate(&base).await?.manifest()?, base.manifest()?);
    let memory = append(&base, 1)?.seal_memory();
    let mut flushed = TraceSnapshot::from_runs(vec![Run::Object(object); 256], 3, 2)?;
    flushed.cohorts = vec![128, 128];
    // Membership values are synthetic here; the owned uploader proves weighted
    // equivalence. This isolates the worst-case physical scheduling headroom.
    let mut current = memory.clone();
    for index in 0..120 {
        current = current.append_validated(
            Run::Memory(Arc::new(Batch::from_updates([
                ((index, 0), i64::MAX),
                ((index + 1, 0), i64::MAX),
            ])?)),
            current.time() + 1,
        )?;
    }
    let rebased = current.rebase_flushed(&memory, &flushed)?;
    assert_eq!(rebased.run_count(), 376);
    assert_eq!(rebased.unsealed_run_count(), 120);
    assert!(rebased.run_count() <= super::super::MAX_RUNS);
    Ok(())
}
