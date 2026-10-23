use super::*;
use crate::engine::{Batch, dataflow::Arrangement, reader::ObjectBatch};
use object_store::memory::InMemory;

fn append(
    trace: &TraceSnapshot<i64, i64>,
    time: u64,
    weight: i64,
) -> Result<TraceSnapshot<i64, i64>> {
    trace.append_validated(Run::Memory(Arc::new(Batch::from_updates([((1, 1), weight)])?)), time)
}

#[tokio::test]
async fn binary_carries_preserve_weighted_identity_pins_and_zero_spans() -> Result<()> {
    let mut trace = TraceSnapshot::empty();
    let mut expected = Batch::default();
    for time in 1..=8192 {
        let weight = if time % 2 == 0 { -1 } else { 1 };
        expected.apply(&Batch::from_updates([((1, 1), weight)])?)?;
        trace = append(&trace, time, weight)?;
        assert!(trace.run_count() <= 14);
        if time % 127 == 0 {
            assert_eq!(trace.materialize().await?, expected);
        }
    }
    assert_eq!(trace.run_count(), 1);
    assert_eq!(trace.spans, [Some(Span { first: 1, last: 8192 })]);
    assert_eq!(trace.memory_charge(), 0);
    let captured = trace.seal_memory();
    let current = append(&captured, 8193, 3)?;
    assert_eq!(captured.materialize().await?, Batch::default());
    assert_eq!(current.materialize().await?, Batch::from_updates([((1, 1), 3)])?);
    assert!(current.valid_suffix(&captured));
    assert_eq!(current.generation(), 8193);
    Ok(())
}

#[tokio::test]
async fn frozen_prefix_survives_thousands_of_epochs_and_another_freeze() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "memory-v1".into(), 16)?;
    let mut trace = append(&writer.empty(), 1, 1)?.seal_memory();
    let captured = trace.clone();
    let flushed = writer.flush(&captured).await?;
    for time in 2..=4097 {
        trace = append(&trace, time, 1)?;
    }
    assert_eq!(trace.run_count(), 2);
    assert_eq!(trace.materialize().await?, Batch::from_updates([((1, 1), 4097)])?);
    let second_capture = trace.seal_memory();
    let rebased = trace.rebase_flushed(&captured, &flushed)?;
    assert_eq!(rebased.spans.last(), trace.spans.last());
    let later = append(&rebased, 4098, -1)?;
    assert_eq!(later.materialize().await?, Batch::from_updates([((1, 1), 4096)])?);
    assert!(later.rebase_flushed(&second_capture, &writer.flush(&second_capture).await?).is_err());
    Ok(())
}

#[tokio::test]
async fn overflowing_memory_subsets_remain_exact_against_retained_objects() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1, 1), -i64::MAX)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "memory-v1", 16).await?;
    let object = ObjectBatch::open(store.clone(), root, "memory-v1").await?;
    let trace = TraceSnapshot::empty().append_validated(Run::Object(object), 1)?;
    let trace = append(&trace, 2, i64::MAX)?;
    let before = trace.clone();
    let trace = append(&trace, 3, 1)?;
    assert_eq!(trace.run_count(), 3);
    assert_eq!(before.materialize().await?, Batch::default());
    assert_eq!(trace.materialize().await?, Batch::from_updates([((1, 1), 1)])?);
    let writer = Arrangement::new(store, "memory-v1".into(), 16)?;
    assert_eq!(writer.flush(&trace).await?.materialize().await?, trace.materialize().await?);
    Ok(())
}

#[tokio::test]
async fn suffix_proof_rejects_gaps_overlaps_and_nonmemory_ticks() -> Result<()> {
    let captured = append(&TraceSnapshot::empty(), 1, 1)?.seal_memory();
    let current = append(&append(&captured, 2, 1)?, 3, -1)?;
    assert!(current.valid_suffix(&captured));
    for span in [
        None,
        Some(Span { first: 3, last: 3 }),
        Some(Span { first: 1, last: 3 }),
        Some(Span { first: 2, last: 4 }),
    ] {
        let mut invalid = current.clone();
        invalid.spans[1] = span;
        assert!(!invalid.valid_suffix(&captured));
    }
    Ok(())
}

#[tokio::test]
async fn hard_fan_in_bound_retains_original_state_on_error() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1, 1), 1)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "memory-v1", 16).await?;
    let object = ObjectBatch::open(store, root, "memory-v1").await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object); super::super::MAX_RUNS], 1, 1)?;
    assert!(append(&trace, 2, 1).is_err());
    assert_eq!(trace.run_count(), super::super::MAX_RUNS);
    assert_eq!(trace.materialize().await?, Batch::from_updates([((1, 1), 256)])?);
    Ok(())
}
