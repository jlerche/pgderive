use super::super::{
    MIN_WINDOW_BYTES, Ranges,
    tests::{packed, statistics, value},
};
use super::ReadAhead;
use crate::engine::{
    Batch,
    dataflow::TimedBatch,
    execution::Limits,
    reader::{BatchReader as _, BlockCache, ObjectBatch, WriteLimits},
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
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 4 + 4096, 4));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES * 4;
    ranges.read(&remote, &pack, 0..10).await?;
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut old = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    assert!(old.reserve(&remote, &pack, 0..10)?);
    assert_eq!(old.bytes, MIN_WINDOW_BYTES);
    assert!(!old.reserve(&remote, &pack, 0..pack.bytes)?);
    assert_eq!(old.windows.len(), 1);
    let before = store.reads.load(Ordering::SeqCst);
    let _read_ahead = ReadAhead::load(vec![old]).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    // A narrow planned slice must not consume a second charge and evict its
    // still-useful wider cover when the compressed cache is already full.
    assert_eq!(ranges.read(&remote, &pack, width * 3..width * 3 + 1).await?.len(), 1);
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    ranges.invalidate(&pack, &(0..pack.bytes))?;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    assert!(plan.reserve(&remote, &pack, width - 10..width + 20)?);
    assert!(plan.reserve(&remote, &pack, width * 4..pack.bytes)?);
    assert_eq!(plan.bytes, MIN_WINDOW_BYTES * 2 + 7);
    assert!(!plan.reserve(&remote, &pack, 0..pack.bytes)?);
    assert_eq!(plan.windows.len(), 3);
    assert!(plan.reserve(&remote, &pack, width * 2..width * 3)?);
    let _read_ahead = ReadAhead::load(vec![plan]).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 2);
    assert_eq!(ranges.read(&remote, &pack, width - 10..width + 20).await?.len(), 30);
    assert_eq!(ranges.read(&remote, &pack, width * 4..pack.bytes).await?.len(), 7);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 2);
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

async fn selected_windows(
    runs: &[ObjectBatch<i64, String>],
    keys: &[i64],
) -> Result<BTreeSet<(String, u64)>> {
    let mut windows = BTreeSet::new();
    let width = 16 * 1024;
    for run in runs {
        for key in keys {
            let Some(page) = run.key_page(key).await? else { continue };
            let (path, range) = run.block_range(page.ordinal).await?.context("missing range")?;
            windows.extend(
                (range.start / width..=(range.end - 1) / width)
                    .map(|window| (path.clone(), window)),
            );
        }
    }
    Ok(windows)
}

#[tokio::test(start_paused = true)]
async fn overlapping_cold_runs_prefetch_all_windows_with_one_concurrency_bound() -> Result<()> {
    for entries in [1, 4096] {
        let mut store = Store::unpaced();
        store.read_delay = Duration::from_millis(20);
        let store = Arc::new(store);
        let mut cache = BlockCache::new(2 * 1024 * 1024, 32);
        // This fixture isolates compressed data prefetch after warming navigation.
        // Whole-worker and growth benchmarks account for index I/O separately.
        cache.index = Arc::new(crate::engine::reader::index::Cache::new(16 * 1024 * 1024, 4096));
        let cache = Arc::new(cache);
        let base = Batch::from_updates((0..4096_i64).map(|key| ((key * 8, value(key * 8, 0)), 1)))?;
        let update = Batch::from_updates(
            (0..4096_i64)
                .map(|key| key * 8)
                .flat_map(|key| [((key, value(key, 0)), -1), ((key, value(key, 1)), 1)]),
        )?;
        let mut runs = Vec::new();
        for (batch, rows) in [(&base, 1), (&update, 2)] {
            let references = ObjectBatch::write_stream(
                store.clone(),
                &mut batch.cursor().await?,
                "overlapping",
                WriteLimits { block_rows: rows, ..WriteLimits::default() },
            )
            .await?;
            for reference in references {
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
        }
        cache.ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = 16 * 1024;
        let keys = (0..8_i64)
            .flat_map(|key| (0..8).map(move |gap| (key * 512 + 13) * 8 + gap))
            .collect::<Vec<_>>();
        let expected_windows = selected_windows(&runs, &keys).await?;
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
        let reads = cache.stats()?.foreground_io.gets;
        let total_reads = store.reads.load(Ordering::SeqCst);
        store.read_peak.store(0, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let result = statistics()
            .evaluate(&input, &trace, Limits { resident_entries: entries, ..Limits::default() })
            .await?;
        assert_eq!(result.batch, expected);
        let requests = usize::try_from(cache.stats()?.foreground_io.gets - reads)?;
        assert!(requests <= expected_windows.len());
        assert!(store.read_peak.load(Ordering::SeqCst) <= crate::engine::reader::READ_CONCURRENCY);
        if entries == 1 {
            assert_eq!(requests, expected_windows.len());
            assert!(started.elapsed() >= Duration::from_millis(160));
        } else {
            assert!(requests < expected_windows.len());
            assert!(store.read_peak.load(Ordering::SeqCst) > 2);
            assert!(started.elapsed() < Duration::from_millis(160));
        }
        println!(
            "{}",
            serde_json::json!({"input_batches": 2, "object_runs": trace.run_count(), "keys": keys.len(), "window_entries": entries, "data_gets": requests, "total_gets": store.reads.load(Ordering::SeqCst) - total_reads, "logical_ms": started.elapsed().as_millis(), "peak_reads": store.read_peak.load(Ordering::SeqCst)})
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
    let ranges = Arc::new(Ranges::new((wide + 4096) * 8, 128));
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
    assert_eq!(
        ReadAhead::ready(std::slice::from_ref(&plan)),
        plan.missing >= crate::engine::reader::READ_CONCURRENCY
    );
    let before = store.reads.load(Ordering::SeqCst);
    store.read_peak.store(0, Ordering::SeqCst);
    let _read_ahead = ReadAhead::load(vec![plan]).await?;
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
    odd.fix_width(100_000)?;
    odd.read(&remote, &pack, 99_999..100_000).await?;
    odd.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = 65_536;
    let mut plan = ReadAhead::new(odd.clone())?.context("missing odd planner")?;
    assert!(plan.reserve(&remote, &pack, 99_999..100_000)?);
    assert_eq!(plan.bytes, 65_536);
    let _read_ahead = ReadAhead::load(vec![plan]).await?;
    assert_eq!(odd.read(&remote, &pack, 99_999..100_000).await?, vec![u8::try_from(99_999 % 251)?]);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn contiguous_misses_share_one_get_and_admit_before_waking_waiters() -> Result<()> {
    let mut probe = Store::unpaced();
    probe.read_delay = Duration::from_millis(20);
    let probe = Arc::new(probe);
    let store: Arc<dyn ObjectStore> = probe.clone();
    let pack = packed(&store, MIN_WINDOW_BYTES * 4).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 4, 4));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    let width = u64::try_from(MIN_WINDOW_BYTES)?;
    for index in [0, 2, 1, 3] {
        assert!(plan.reserve(&store, &pack, index * width..index * width + 1)?);
    }
    assert_eq!(plan.missing, 1);
    let before = probe.reads.load(Ordering::SeqCst);
    let bytes = probe.range_bytes.load(Ordering::SeqCst);
    let loading = ReadAhead::load(vec![plan]);
    let waiter = async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        ranges.read(&store, &pack, width..width + 1).await
    };
    let (loaded, response) = tokio::join!(loading, waiter);
    let _read_ahead = loaded?;
    assert_eq!(response?.len(), 1);
    assert_eq!(probe.reads.load(Ordering::SeqCst) - before, 1);
    assert_eq!(probe.range_bytes.load(Ordering::SeqCst) - bytes, width * 4);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn raced_middle_window_is_not_downloaded_again() -> Result<()> {
    let probe = Arc::new(Store::unpaced());
    let store: Arc<dyn ObjectStore> = probe.clone();
    let pack = packed(&store, MIN_WINDOW_BYTES * 3).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 3, 3));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let width = u64::try_from(MIN_WINDOW_BYTES)?;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    assert!(plan.reserve(&store, &pack, 0..pack.bytes)?);
    ranges.read(&store, &pack, width..width + 1).await?;
    let before = probe.reads.load(Ordering::SeqCst);
    let bytes = probe.range_bytes.load(Ordering::SeqCst);
    let _read_ahead = ReadAhead::load(vec![plan]).await?;
    assert_eq!(probe.reads.load(Ordering::SeqCst) - before, 2);
    assert_eq!(probe.range_bytes.load(Ordering::SeqCst) - bytes, width * 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_contiguous_load_releases_all_stripes_for_retry() -> Result<()> {
    let mut probe = Store::unpaced();
    probe.read_delay = Duration::from_millis(20);
    let probe = Arc::new(probe);
    let store: Arc<dyn ObjectStore> = probe.clone();
    let pack = packed(&store, MIN_WINDOW_BYTES * 4).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 4, 4));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    for timeout in [true, false] {
        let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
        assert!(plan.reserve(&store, &pack, 0..pack.bytes)?);
        if timeout {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), ReadAhead::load(vec![plan]))
                    .await
                    .is_err()
            );
            assert_eq!(probe.read_active.load(Ordering::SeqCst), 0);
            assert!(
                ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.windows.is_empty()
            );
        } else {
            let _read_ahead = ReadAhead::load(vec![plan]).await?;
            let reads = probe.reads.load(Ordering::SeqCst);
            assert_eq!(
                ranges.read(&store, &pack, 0..pack.bytes).await?.len(),
                usize::try_from(pack.bytes)?
            );
            assert_eq!(probe.reads.load(Ordering::SeqCst), reads);
        }
    }
    Ok(())
}

#[tokio::test]
async fn scoped_pins_survive_eviction_release_and_invalidate() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let width = u64::try_from(MIN_WINDOW_BYTES)?;
    let pack = packed(&remote, MIN_WINDOW_BYTES * 8).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 2, 2));
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing planner")?;
    assert!(plan.reserve(&remote, &pack, 0..1)?);
    let guard = ReadAhead::load(vec![plan]).await?;
    for n in 2..8 {
        ranges.read(&remote, &pack, n * width..n * width + 1).await?;
    }
    let before = store.reads.load(Ordering::SeqCst);
    assert_eq!(ranges.read(&remote, &pack, 0..1).await?, vec![0]);
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    assert_eq!(ranges.take_diagnostics()?["active_batches"], 1);
    ranges.invalidate(&pack, &(0..1))?;
    ranges.read(&remote, &pack, 0..1).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before + 1);
    assert_eq!(ranges.take_diagnostics()?["pinned_windows"], 0);
    drop(guard);
    for n in 2..8 {
        ranges.read(&remote, &pack, n * width..n * width + 1).await?;
    }
    assert_eq!(ranges.take_diagnostics()?["active_batches"], 0);
    assert!(ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.pins.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_partial_load_releases_registered_batches() -> Result<()> {
    let mut store = Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    let pack = packed(&store, MIN_WINDOW_BYTES * 4).await?;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * 2, 2));
    let width = u64::try_from(MIN_WINDOW_BYTES)?;
    ranges.read(&store, &pack, 0..1).await?;
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing planner")?;
    assert!(plan.reserve(&store, &pack, 0..1)?);
    assert!(plan.reserve(&store, &pack, width * 2..width * 2 + 1)?);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), ReadAhead::load(vec![plan])).await.is_err()
    );
    assert_eq!(ranges.take_diagnostics()?["active_batches"], 0);
    ranges.read(&store, &pack, 0..1).await?;
    assert!(ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.pins.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cached_groups_do_not_block_later_independent_misses() -> Result<()> {
    use crate::engine::reader::format::Pack;
    use object_store::{ObjectStoreExt as _, path::Path};
    let mut probe = Store::unpaced();
    probe.read_delay = Duration::from_millis(20);
    let probe = Arc::new(probe);
    let store: Arc<dyn ObjectStore> = probe.clone();
    let concurrency = crate::engine::reader::READ_CONCURRENCY;
    let ranges = Arc::new(Ranges::new(MIN_WINDOW_BYTES * concurrency * 16, concurrency * 16));
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = MIN_WINDOW_BYTES;
    let mut plan = ReadAhead::new(ranges.clone())?.context("missing read ahead")?;
    let bytes = vec![1; 1024];
    let hash = crate::engine::reader::format::hash(&bytes);
    let mut stripes = BTreeSet::new();
    for index in 0..concurrency * 8 {
        let mut attempt = 0;
        let pack = loop {
            let pack = Pack {
                path: format!("interleave/{index:03}/{attempt}"),
                hash: hash.clone(),
                bytes: 1024,
            };
            let identity =
                (pack.path.clone(), hash.clone(), 0, 1024, u64::try_from(MIN_WINDOW_BYTES)?);
            if index % 8 != 0 || stripes.insert(Ranges::stripe(&identity)?) {
                break pack;
            }
            attempt += 1;
        };
        store.put(&Path::from(pack.path.clone()), bytes.clone().into()).await?;
        if index % 8 != 0 {
            ranges.read(&store, &pack, 0..1).await?;
        }
        assert!(plan.reserve(&store, &pack, 0..1)?);
    }
    assert!(ReadAhead::ready(std::slice::from_ref(&plan)));
    probe.reads.store(0, Ordering::SeqCst);
    probe.read_peak.store(0, Ordering::SeqCst);
    let start = tokio::time::Instant::now();
    let _read_ahead = ReadAhead::load(vec![plan]).await?;
    assert_eq!(probe.reads.load(Ordering::SeqCst), concurrency);
    assert_eq!(probe.read_peak.load(Ordering::SeqCst), concurrency);
    assert_eq!(start.elapsed(), Duration::from_millis(20));
    Ok(())
}
