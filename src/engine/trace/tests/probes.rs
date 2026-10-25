use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    reader::{BlockCache, ObjectBatch},
    trace::{KeyCursor, Run, TraceSnapshot},
};
use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

async fn rows(mut cursor: KeyCursor<i64, i64>) -> Result<Batch<i64, i64>> {
    let mut updates = Vec::new();
    while let Some((key, value, weight)) = cursor.current() {
        updates.push(((*key, *value), weight));
        cursor.advance().await?;
    }
    Batch::from_updates(updates)
}
async fn object(
    store: Arc<dyn ObjectStore>,
    cache: Arc<BlockCache>,
    batch: &Batch<i64, i64>,
    block_rows: usize,
) -> Result<ObjectBatch<i64, i64>> {
    let reference = ObjectBatch::write(store.clone(), batch, "ints", block_rows).await?;
    ObjectBatch::open_with_cache(store, reference, "ints", cache).await
}
#[tokio::test]
async fn probes_skip_irrelevant_runs_and_stop_before_unrelated_blocks() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let wide = object(
        store.clone(),
        cache.clone(),
        &Batch::from_updates([((0, 0), 1), ((7, 1), 2), ((7, 2), 1), ((7, 3), 4), ((9, 0), 1)])?,
        1,
    )
    .await?;
    let unrelated =
        object(store.clone(), cache.clone(), &Batch::from_updates([((100, 0), 1)])?, 1).await?;
    let trace = TraceSnapshot::from_runs(
        vec![
            Run::Object(wide.clone()),
            Run::Object(unrelated.clone()),
            Run::Memory(Arc::new(Batch::from_updates([((7, 1), -2), ((7, 2), 3)])?)),
        ],
        1,
        1,
    )?;
    for (object, ordinal) in [(&wide, 0), (&wide, 4), (&unrelated, 0)] {
        crate::engine::tests::store::corrupt_block(store.as_ref(), object, ordinal).await?;
    }
    let snapshot = trace;
    assert_eq!(
        rows(snapshot.key_cursor(&7).await?).await?,
        Batch::from_updates([((7, 2), 4), ((7, 3), 4)])?
    );
    assert!(snapshot.key_cursor(&-1).await?.current().is_none());
    assert!(snapshot.key_cursor(&8).await?.current().is_none());
    assert!(snapshot.key_cursor(&101).await?.current().is_none());
    assert!(snapshot.materialize().await.is_err());
    Ok(())
}
#[tokio::test]
async fn cached_cold_evicted_and_batched_probes_have_equal_results() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(256, 2));
    let batch = Batch::from_updates((0..20).map(|n| ((n, n + 1), 1)))?;
    let data = object(store.clone(), cache.clone(), &batch, 1).await?;
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(data), 1)?;
    let snapshot = trace;
    let cold = rows(snapshot.key_cursor(&3).await?).await?;
    let before = cache.stats()?;
    assert_eq!(rows(snapshot.key_cursor(&3).await?).await?, cold);
    assert!(cache.stats()?.hits > before.hits);
    let mut probes = snapshot.probes();
    assert_eq!(rows(probes.cursor(&4).await?).await?, Batch::from_updates([((4, 5), 1)])?);
    let before = cache.stats()?;
    assert_eq!(rows(probes.cursor(&4).await?).await?, Batch::from_updates([((4, 5), 1)])?);
    assert_eq!(cache.stats()?.misses, before.misses);
    for key in 5..20 {
        rows(snapshot.key_cursor(&key).await?).await?;
    }
    let stats = cache.stats()?;
    assert!(stats.bytes <= 256);
    assert!(stats.entries <= 2);
    let before = stats.misses;
    assert_eq!(rows(snapshot.key_cursor(&3).await?).await?, cold);
    assert!(cache.stats()?.misses > before);
    let disabled = Arc::new(BlockCache::new(1, 1));
    let data = object(store, disabled.clone(), &batch, 1).await?;
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(data), 1)?;
    assert_eq!(rows(trace.key_cursor(&3).await?).await?, cold);
    assert_eq!(disabled.stats()?.bytes, 0);
    Ok(())
}
#[tokio::test]
async fn selected_block_failure_keeps_probe_position_and_can_retry() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = object(
        store.clone(),
        Arc::new(BlockCache::new(0, 0)),
        &Batch::from_updates([((7, 1), 1), ((7, 2), 1), ((7, 3), 1)])?,
        1,
    )
    .await?;
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(data.clone()), 1)?;
    let mut cursor = trace.key_cursor(&7).await?;
    let path =
        Path::from(data.block_range(1).map(|(path, _)| path).context("missing fixture block")?);
    let bytes = store.get(&path).await?.bytes().await?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &data, 1).await?;
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&7, &1, 1)));
    store.put(&path, bytes.into()).await?;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&7, &2, 1)));
    Ok(())
}
#[tokio::test]
async fn affected_identity_validation_checks_final_weights_without_scanning_unrelated_state()
-> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::new(store.clone(), "ints".into(), 1)?;
    let initial = Batch::from_updates([((1, 1), i64::MAX), ((1, 3), -2), ((100, 0), 1)])?;
    let prior =
        writer.stage(&writer.empty(), &TimedBatch { time: 1, batch: initial.clone() }).await?;
    let data = object(store.clone(), Arc::new(BlockCache::new(0, 0)), &initial, 1).await?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &data, 2).await?;
    let overflow = TimedBatch { time: 2, batch: Batch::from_updates([((1, 1), 1)])? };
    assert!(writer.stage(&prior, &overflow).await.is_err());
    assert_eq!(prior.time(), 1);
    let delta = TimedBatch {
        time: 2,
        batch: Batch::from_updates([((1, 1), -i64::MAX), ((1, 2), 2), ((1, 3), 3)])?,
    };
    let next = writer.stage(&prior, &delta).await?;
    assert_eq!(next.time(), 2);
    assert_eq!(
        rows(next.key_cursor(&1).await?).await?,
        Batch::from_updates([((1, 2), 2), ((1, 3), 1)])?
    );
    assert!(next.materialize().await.is_err());
    Ok(())
}
#[tokio::test]
async fn different_run_layouts_and_cache_budgets_preserve_full_identity() -> Result<()> {
    for block_rows in [1, 2, 7] {
        for budget in [0, 80, 1024] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache = Arc::new(BlockCache::new(budget, 8));
            let a = Batch::from_updates((0..20).map(|n| ((n / 4, n), 2)))?;
            let b = Batch::from_updates((0..20).map(|n| ((n / 4, n), -1)))?;
            let trace = TraceSnapshot::from_runs(
                vec![
                    Run::Object(object(store.clone(), cache.clone(), &a, block_rows).await?),
                    Run::Object(object(store, cache.clone(), &b, block_rows).await?),
                ],
                1,
                1,
            )?;
            for key in 0..5 {
                let expected = Batch::from_updates((key * 4..key * 4 + 4).map(|n| ((key, n), 1)))?;
                assert_eq!(rows(trace.key_cursor(&key).await?).await?, expected);
            }
            assert!(cache.stats()?.bytes <= budget);
        }
    }
    Ok(())
}

#[tokio::test]
async fn grouped_probes_reuse_blocks_and_retry_without_losing_heads() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let batch = Batch::from_updates((0..12).map(|n| ((n, n), 1)))?;
    let object = object(store.clone(), cache.clone(), &batch, 4).await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object.clone())], 1, 1)?;
    let mut probes = trace.probes();
    assert_eq!(rows(probes.cursor(&0).await?).await?, Batch::from_updates([((0, 0), 1)])?);
    let before = cache.stats()?;
    for key in [3, 1, 2] {
        assert_eq!(
            rows(probes.cursor(&key).await?).await?,
            Batch::from_updates([((key, key), 1)])?
        );
    }
    assert_eq!(cache.stats()?.misses, before.misses);
    let path = Path::from(object.block_range(1).map(|(path, _)| path).context("missing block")?);
    let bytes = store.get(&path).await?.bytes().await?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &object, 1).await?;
    assert!(probes.cursor(&5).await.is_err());
    assert_eq!(rows(probes.cursor(&1).await?).await?, Batch::from_updates([((1, 1), 1)])?);
    store.put(&path, bytes.into()).await?;
    assert_eq!(rows(probes.cursor(&5).await?).await?, Batch::from_updates([((5, 5), 1)])?);
    assert_eq!(rows(probes.cursor(&9).await?).await?, Batch::from_updates([((9, 9), 1)])?);
    assert!(probes.cursor(&20).await?.current().is_none());
    assert_eq!(rows(probes.cursor(&0).await?).await?, Batch::from_updates([((0, 0), 1)])?);
    Ok(())
}

#[tokio::test]
async fn batched_tuple_validation_reads_each_selected_block_once() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let batch = Batch::from_updates((0..24).map(|n| ((n / 8, n), 2)))?;
    let object = object(store.clone(), cache.clone(), &batch, 4).await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object.clone())], 1, 1)?;
    let delta = Batch::from_updates((0..16).map(|n| ((n / 8, n), -2)))?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &object, 5).await?;
    let before = cache.stats()?.misses;
    trace.validate_bag_delta(&delta).await?;
    assert_eq!(cache.stats()?.misses - before, 4);
    assert!(trace.validate_bag_delta(&Batch::from_updates([((0, 1), -3)])?).await.is_err());
    assert!(trace.validate_bag_delta(&Batch::from_updates([((0, 99), -1)])?).await.is_err());
    assert!(trace.validate_bag_delta(&Batch::from_updates([((2, 23), 1)])?).await.is_err());
    trace.validate_bag_delta(&Batch::from_updates([((0, 1), -2)])?).await?;
    let trace = TraceSnapshot::from_runs(
        vec![
            Run::Memory(Arc::new(Batch::from_updates([((1, 1), i64::MAX)])?)),
            Run::Memory(Arc::new(Batch::from_updates([((1, 1), i64::MAX)])?)),
            Run::Memory(Arc::new(Batch::from_updates([((1, 1), -i64::MAX)])?)),
        ],
        1,
        1,
    )?;
    trace.validate_bag_delta(&Batch::from_updates([((1, 1), -i64::MAX)])?).await?;
    assert!(trace.validate_delta(&Batch::from_updates([((1, 1), 1)])?).await.is_err());
    Ok(())
}

#[tokio::test]
async fn bag_proofs_are_bound_to_exact_immutable_roots_and_batches() -> Result<()> {
    let first = TraceSnapshot::from_runs(
        vec![Run::Memory(Arc::new(Batch::from_updates([((1, 2), 1)])?))],
        1,
        1,
    )?;
    let delta = Arc::new(Batch::from_updates([((1, 2), -1), ((1, 3), 1)])?);
    let proof = first.validate_bag_batch(delta.clone()).await?;
    assert!(proof.matches(&first, &delta));
    let other = first.clone();
    assert!(!proof.matches(&other, &delta));
    assert!(!proof.matches(&first, &(*delta).clone()));
    assert!(
        first.validate_bag_batch(Arc::new(Batch::from_updates([((1, 9), -1)])?)).await.is_err()
    );
    assert_eq!(first.time(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cold_run_heads_overlap_bounded_gets_and_cancel_without_advancing_probes() -> Result<()> {
    use std::{sync::atomic::Ordering, time::Duration};
    let mut store = crate::engine::tests::store::Store::unpaced();
    store.read_delay = Duration::from_millis(10);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(0, 0));
    let mut runs = Vec::new();
    for n in 0..20 {
        let batch = Batch::from_updates([((1, n), 1), ((2, n), -1)])?;
        runs.push(Run::Object(object(store.clone(), cache.clone(), &batch, 1).await?));
    }
    let snapshot = TraceSnapshot::from_runs(runs, 1, 1)?;
    let started = tokio::time::Instant::now();
    let actual = rows(snapshot.key_cursor(&1).await?).await?;
    assert_eq!(actual, Batch::from_updates((0..20).map(|n| ((1, n), 1)))?);
    assert_eq!(store.read_peak.load(Ordering::SeqCst), 8);
    assert_eq!(store.read_active.load(Ordering::SeqCst), 0);
    assert_eq!(started.elapsed(), Duration::from_millis(30));
    let mut probes = snapshot.probes();
    let before = rows(probes.cursor(&1).await?).await?;
    assert!(tokio::time::timeout(Duration::from_millis(1), probes.cursor(&2)).await.is_err());
    assert_eq!(store.read_active.load(Ordering::SeqCst), 0);
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(rows(probes.cursor(&1).await?).await?, before);
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    let started = tokio::time::Instant::now();
    assert_eq!(
        rows(probes.cursor(&2).await?).await?,
        Batch::from_updates((0..20).map(|n| ((2, n), -1)))?
    );
    assert_eq!(started.elapsed(), Duration::from_millis(30));
    assert_eq!(store.read_active.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_fetches_deduplicate_storage_without_deduplicating_run_weights() -> Result<()> {
    use std::sync::atomic::Ordering;
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let cache = Arc::new(BlockCache::new(0, 0));
    let batch = Batch::from_updates([((1, 2), 2)])?;
    let data = object(store.clone(), cache.clone(), &batch, 1).await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(data.clone()); 20], 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    assert_eq!(rows(trace.key_cursor(&1).await?).await?, Batch::from_updates([((1, 2), 40)])?);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 1);
    let isolated = object(store.clone(), Arc::new(BlockCache::new(0, 0)), &batch, 1).await?;
    let trace =
        TraceSnapshot::from_runs(vec![Run::Object(data.clone()), Run::Object(isolated)], 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    assert_eq!(rows(trace.key_cursor(&1).await?).await?, Batch::from_updates([((1, 2), 4)])?);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 2);
    let other: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let damaged = object(other.clone(), cache, &batch, 1).await?;
    assert_eq!(damaged.reference(), data.reference());
    crate::engine::tests::store::corrupt_block(other.as_ref(), &damaged, 0).await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(data), Run::Object(damaged)], 1, 1)?;
    assert!(trace.key_cursor(&1).await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn coefficient_validation_overlaps_cold_runs_with_exact_cancellation() -> Result<()> {
    use std::{sync::atomic::Ordering, time::Duration};
    let mut store = crate::engine::tests::store::Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(0, 0));
    let mut runs = Vec::new();
    for index in 0..16_i64 {
        let weight = if index < 8 { i64::MAX } else { -i64::MAX };
        let batch = Batch::from_updates([((1, 1), weight), ((2, index), 1)])?;
        runs.push(Run::Object(object(store.clone(), cache.clone(), &batch, 2).await?));
    }
    runs.push(Run::Memory(Arc::new(Batch::from_updates([((1, 1), 1)])?)));
    let delta = Batch::from_updates([((1, 1), -1)])?;
    let serial_started = tokio::time::Instant::now();
    for run in &runs {
        TraceSnapshot::from_runs(vec![run.clone()], 1, 1)?.validate_delta(&delta).await?;
    }
    assert_eq!(serial_started.elapsed(), Duration::from_millis(320));
    let trace = TraceSnapshot::from_runs(runs, 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    store.read_peak.store(0, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    trace.validate_bag_delta(&delta).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 16);
    assert_eq!(store.read_peak.load(Ordering::SeqCst), 8);
    assert_eq!(started.elapsed(), Duration::from_millis(40));
    assert!(trace.validate_bag_delta(&Batch::from_updates([((1, 1), -2)])?).await.is_err());
    let mut cancelled = Box::pin(trace.validate_bag_delta(&delta));
    assert!(futures_util::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    assert_eq!(store.read_active.load(Ordering::SeqCst), 0);
    trace.validate_bag_delta(&delta).await?;
    Ok(())
}
