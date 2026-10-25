use super::super::{
    MIN_WINDOW_BYTES, Ranges,
    tests::{packed, statistics, value},
};
use super::ReadAhead;
use crate::engine::{
    Batch,
    dataflow::TimedBatch,
    execution::Limits,
    reader::{BlockCache, ObjectBatch},
    tests::store::Store,
    trace::{Run, TraceSnapshot},
};
use anyhow::{Context as _, Result};
use object_store::ObjectStore;
use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[tokio::test]
async fn reservations_are_atomic_charge_tails_and_reuse_old_widths() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let width = u64::try_from(MIN_WINDOW_BYTES)?;
    let pack = packed(&remote, MIN_WINDOW_BYTES * 4 + 7).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 4, 4));
    ranges.read(&remote, &pack, 0..10).await?;
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut old = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    assert!(old.reserve(&remote, &pack, 0..10)?);
    assert_eq!(old.bytes, MIN_WINDOW_BYTES);
    assert!(!old.reserve(&remote, &pack, 0..pack.bytes)?);
    assert_eq!(old.windows.len(), 1);
    let before = store.reads.load(Ordering::SeqCst);
    ReadAhead::load(vec![old]).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    ranges.invalidate(&pack, &(0..pack.bytes))?;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    assert!(plan.reserve(&remote, &pack, width - 10..width + 20)?);
    assert!(plan.reserve(&remote, &pack, width * 4..pack.bytes)?);
    assert_eq!(plan.bytes, MIN_WINDOW_BYTES * 2 + 7);
    assert!(!plan.reserve(&remote, &pack, 0..pack.bytes)?);
    assert_eq!(plan.windows.len(), 3);
    assert!(plan.reserve(&remote, &pack, width * 2..width * 3)?);
    ReadAhead::load(vec![plan]).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 4);
    assert_eq!(ranges.read(&remote, &pack, width - 10..width + 20).await?.len(), 30);
    assert_eq!(ranges.read(&remote, &pack, width * 4..pack.bytes).await?.len(), 7);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 4);
    assert!(ReadAhead::new(Arc::new(Ranges::new(0, 1)))?.is_none());
    assert!(ReadAhead::new(Arc::new(Ranges::new(1, 0)))?.is_none());
    let mut tiny = ReadAhead::new(Arc::new(Ranges::new(1, 1)))?.context("missing tiny planner")?;
    assert!(!tiny.reserve(&remote, &pack, 0..pack.bytes)?);
    assert_eq!(tiny.planning_steps, 0);
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 4, 1));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut short = ReadAhead::new(ranges)?.context("missing short planner")?;
    assert!(!short.reserve(&remote, &pack, 0..width * 3)?);
    assert_eq!(short.planning_steps, 2);
    assert!(short.windows.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn overlapping_cold_runs_prefetch_all_windows_with_one_concurrency_bound() -> Result<()> {
    for entries in [1, 4096] {
        let mut store = Store::unpaced();
        store.read_delay = Duration::from_millis(20);
        let store = Arc::new(store);
        let cache = Arc::new(BlockCache::new(2 * 1024 * 1024, 32));
        let base = Batch::from_updates((0..4096_i64).map(|key| ((key * 8, value(key * 8, 0)), 1)))?;
        let update = Batch::from_updates(
            (0..4096_i64)
                .map(|key| key * 8)
                .flat_map(|key| [((key, value(key, 0)), -1), ((key, value(key, 1)), 1)]),
        )?;
        let mut runs = Vec::new();
        for (batch, rows) in [(&base, 1), (&update, 2)] {
            let reference = ObjectBatch::write(store.clone(), batch, "overlapping", rows).await?;
            runs.push(
                ObjectBatch::open_with_cache(
                    store.clone(),
                    reference,
                    "overlapping",
                    cache.clone(),
                )
                .await?,
            );
        }
        cache.ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width =
            MIN_WINDOW_BYTES;
        let keys = (0..8_i64)
            .flat_map(|key| (0..8).map(move |gap| (key * 512 + 13) * 8 + gap))
            .collect::<Vec<_>>();
        let mut expected_windows = BTreeSet::new();
        let width = u64::try_from(MIN_WINDOW_BYTES)?;
        for run in &runs {
            for block in keys.iter().filter_map(|key| run.key_block(key)) {
                let (path, range) = run.block_range(block).context("missing range")?;
                expected_windows.extend(
                    (range.start / width..=(range.end - 1) / width)
                        .map(|window| (path.to_owned(), window)),
                );
            }
        }
        let trace = TraceSnapshot::empty()
            .append_runs_validated(runs.into_iter().map(Run::Object).collect(), 1)?;
        let input = TimedBatch {
            time: 2,
            batch: Batch::from_updates(keys.iter().map(|key| ((*key, 0_i64), 1)))?,
        };
        let expected = Batch::from_updates(keys.iter().flat_map(|key| {
            if key % 8 == 0 {
                vec![((*key, value(*key, 1)), -1), ((*key, value(*key, 2)), 1)]
            } else {
                vec![((*key, value(*key, 1)), 1)]
            }
        }))?;
        let reads = store.reads.load(Ordering::SeqCst);
        store.read_peak.store(0, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let result = statistics()
            .evaluate(&input, &trace, Limits { resident_entries: entries, ..Limits::default() })
            .await?;
        assert_eq!(result.batch, expected);
        assert_eq!(store.reads.load(Ordering::SeqCst) - reads, expected_windows.len());
        assert!(store.read_peak.load(Ordering::SeqCst) <= crate::engine::reader::READ_CONCURRENCY);
        if entries == 1 {
            assert!(started.elapsed() >= Duration::from_millis(160));
        } else {
            assert!(store.read_peak.load(Ordering::SeqCst) > 2);
            assert!(started.elapsed() < Duration::from_millis(160));
        }
        println!(
            "{}",
            serde_json::json!({"overlapping_runs": 2, "keys": keys.len(), "window_entries": entries, "gets": expected_windows.len(), "logical_ms": started.elapsed().as_millis(), "peak_reads": store.read_peak.load(Ordering::SeqCst)})
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cached_wide_covers_do_not_starve_sparse_readiness() -> Result<()> {
    let mut store = Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store = Arc::new(store);
    let remote: Arc<dyn ObjectStore> = store.clone();
    let wide = super::super::WINDOW_BYTES;
    let pack = packed(&remote, wide * 16).await?;
    let ranges = Arc::new(Ranges::new(wide * 8, 128));
    for n in 0..8_u64 {
        let start = n * 2 * u64::try_from(wide)?;
        ranges.read(&remote, &pack, start..start + 1).await?;
    }
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing planner")?;
    let mut requests = Vec::new();
    for n in 0..16_u64 {
        let start = n * u64::try_from(wide)? + 13;
        assert!(plan.reserve(&remote, &pack, start..start + 1)?);
        requests.push(start..start + 1);
    }
    assert_eq!(plan.bytes, MIN_WINDOW_BYTES * 16);
    assert_eq!(
        plan.windows
            .values()
            .filter(|window| window.bytes.is_some())
            .map(|window| window.length)
            .sum::<usize>(),
        MIN_WINDOW_BYTES * 8
    );
    assert_eq!(plan.missing, 8);
    assert!(ReadAhead::ready(std::slice::from_ref(&plan)));
    let before = store.reads.load(Ordering::SeqCst);
    store.read_peak.store(0, Ordering::SeqCst);
    ReadAhead::load(vec![plan]).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 8);
    assert!(store.read_peak.load(Ordering::SeqCst) > 2);
    assert!(store.read_peak.load(Ordering::SeqCst) <= crate::engine::reader::READ_CONCURRENCY);
    for range in requests {
        let expected = u8::try_from(range.start % 251)?;
        assert_eq!(ranges.read(&remote, &pack, range).await?, vec![expected]);
    }
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 8);
    let before = store.reads.load(Ordering::SeqCst);
    let odd = Arc::new(Ranges::new(100_000, 8));
    odd.read(&remote, &pack, 99_999..100_000).await?;
    odd.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = 65_536;
    let mut plan = ReadAhead::new(odd.clone())?.context("missing odd planner")?;
    assert!(plan.reserve(&remote, &pack, 99_999..100_000)?);
    assert_eq!(plan.bytes, 100_000);
    ReadAhead::load(vec![plan]).await?;
    assert_eq!(odd.read(&remote, &pack, 99_999..100_000).await?, vec![u8::try_from(99_999 % 251)?]);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 1);
    Ok(())
}
