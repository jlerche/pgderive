use super::Ranges;
use crate::engine::reader::{BatchReader as _, WriteLimits};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, Statistics, TimedBatch},
    execution::Limits,
    reader::{BlockCache, ObjectBatch, format::Pack},
    trace::Run,
};
use anyhow::{Context as _, Result, ensure};
use num_bigint::BigInt;
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

pub(super) async fn packed(store: &Arc<dyn ObjectStore>, size: usize) -> Result<Pack> {
    let bytes = (0..size).map(|i| u8::try_from(i % 251)).collect::<Result<Vec<_>, _>>()?;
    let hash = crate::engine::reader::format::hash(&bytes);
    let path = format!("pgderive/pack/{hash}");
    store.put(&Path::from(path.clone()), bytes.into()).await?;
    Ok(Pack { path, hash, bytes: u64::try_from(size)? })
}
#[tokio::test]
async fn windows_coalesce_concurrent_reads_cross_boundaries_and_evict() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let width = u64::try_from(super::WINDOW_BYTES)?;
    let pack = packed(&remote, usize::try_from(width * 3 + 7)?).await?;
    let ranges = Ranges::new((super::WINDOW_BYTES + 4096) * 2, 2);
    let (a, b) = tokio::try_join!(
        ranges.read(&remote, &pack, width - 10..width + 20),
        ranges.read(&remote, &pack, width - 5..width + 25)
    )?;
    assert_eq!(
        a,
        (width - 10..width + 20).map(|i| u8::try_from(i % 251)).collect::<Result<Vec<_>, _>>()?
    );
    assert_eq!(b.len(), 30);
    assert_eq!(store.reads.load(Ordering::SeqCst), 2);
    ranges.read(&remote, &pack, width * 2..width * 3 + 7).await?;
    assert!(
        ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.bytes
            <= super::WINDOW_BYTES * 2
    );
    let before = store.reads.load(Ordering::SeqCst);
    ranges.read(&remote, &pack, 0..10).await?;
    ranges.read(&remote, &pack, 0..10).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before + 1);
    ranges.invalidate(&pack, &(width * 2..pack.bytes))?;
    assert!(ranges.read(&remote, &pack, pack.bytes - 1..pack.bytes + 1).await.is_err());
    assert!(ranges.read(&remote, &pack, 5..5).await.is_err());
    for limits in [(0, 4), (100, 0), (1, 1)] {
        let cache = Ranges::new(limits.0, limits.1);
        assert_eq!(cache.read(&remote, &pack, 0..10).await?.len(), 10);
    }
    Ok(())
}
pub(super) fn value(key: i64, count: i64) -> String {
    format!("{count}:{}", format!("{key:016x}").repeat(48))
}
pub(super) fn statistics() -> Statistics<i64, i64, String> {
    Statistics::with_measure(
        1,
        |_, _: &i64| Ok(vec![BigInt::from(1)]),
        |s: &String| {
            Ok(vec![BigInt::from(s.split(':').next().context("missing count")?.parse::<i64>()?)])
        },
        |k, v| Ok(Some(value(*k, i64::try_from(v[0].clone())?))),
    )
}
async fn amplification(enabled: bool) -> Result<(usize, u128)> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = BlockCache::new(16 * 1024 * 1024, 256);
    if !enabled {
        cache.ranges = Arc::new(Ranges::new(0, 0));
    }
    let cache = Arc::new(cache);
    let batch = Batch::from_updates((0..100_000).map(|k| ((k, value(k, 1)), 1)))?;
    let roots = ObjectBatch::write_stream(
        remote.clone(),
        &mut batch.cursor().await?,
        "wide-statistics",
        WriteLimits::default(),
    )
    .await?;
    let mut runs = Vec::new();
    for root in roots {
        runs.push(Run::Object(
            ObjectBatch::open_with_cache(remote.clone(), root, "wide-statistics", cache.clone())
                .await?,
        ));
    }
    let trace = Arrangement::new(remote, "wide-statistics".into(), 4096)?
        .empty()
        .append_runs_validated(runs, 1)?;
    let operator = statistics();
    let input = TimedBatch {
        time: 2,
        batch: Batch::from_updates((0..513).map(|n| (((n * 193) % 100_000, 0_i64), 1)))?,
    };
    let before = cache.stats()?.foreground_io.gets;
    let started = Instant::now();
    let output = operator.evaluate(&input, &trace, Limits::default()).await?;
    // This is the same full-identity coefficient validation used by staging.
    trace.validate_delta(&output.batch).await?;
    let reads = usize::try_from(cache.stats()?.foreground_io.gets - before)?;
    let elapsed = started.elapsed().as_millis();
    let expected = Batch::from_updates((0..513).flat_map(|n| {
        let k = (n * 193) % 100_000;
        [((k, value(k, 1)), -1), ((k, value(k, 2)), 1)]
    }))?;
    ensure!(output.batch == expected, "statistics differ from independent delta oracle");
    assert!(cache.stats()?.bytes <= 16 * 1024 * 1024);
    Ok((reads, elapsed))
}
#[tokio::test]
async fn sf10_cold_statistics_and_staging_amortize_gets_without_more_decoded_cache() -> Result<()> {
    let before = amplification(false).await?;
    let after = amplification(true).await?;
    println!(
        "{}",
        serde_json::json!({"baseline_data_gets":before.0,"coalesced_data_gets":after.0,"baseline_ms":before.1,"coalesced_ms":after.1})
    );
    ensure!(
        before.0 > 500 && after.0 * 10 < before.0,
        "cold request amplification did not improve"
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_prefetched_page_is_rejected_and_repaired_storage_can_retry() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let batch = Batch::from_updates((0..10).map(|k| ((k, k), 1)))?;
    let reference = ObjectBatch::write(remote.clone(), &batch, "corruption", 1).await?;
    let object = ObjectBatch::<i64, i64>::open_with_cache(
        remote.clone(),
        reference,
        "corruption",
        Arc::new(BlockCache::new(4096, 1)),
    )
    .await?;
    let pack = object.test_packs().await?.first().cloned().context("missing pack")?;
    let path = Path::from(pack.path.clone());
    let original = remote.get(&path).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    let offset = usize::try_from(object.leaf(1).await?.block.offset)?;
    corrupt[offset] ^= 1;
    remote.put(&path, corrupt.into()).await?;
    assert_eq!(object.read(0).await?[0].0, (0, 0));
    assert!(object.read(1).await.is_err());
    remote.put(&path, original.into()).await?;
    let before = object.cache.stats()?.foreground_io.gets;
    assert_eq!(object.read(1).await?[0].0, (1, 1));
    assert_eq!(object.cache.stats()?.foreground_io.gets, before + 1);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn canceled_or_failed_window_load_does_not_poison_singleflight() -> Result<()> {
    let mut store = crate::engine::tests::store::Store::unpaced();
    store.read_delay = std::time::Duration::from_secs(1);
    let store = Arc::new(store);
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, 512).await?;
    let ranges = Arc::new(Ranges::new(1024, 2));
    let task = {
        let ranges = ranges.clone();
        let remote = remote.clone();
        let pack = pack.clone();
        tokio::spawn(async move { ranges.read(&remote, &pack, 0..10).await })
    };
    while store.read_active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.is_err());
    assert_eq!(ranges.read(&remote, &pack, 0..10).await?.len(), 10);
    let mut missing = pack.clone();
    missing.path = "missing".into();
    assert!(ranges.read(&remote, &missing, 0..10).await.is_err());
    missing.path = pack.path;
    assert_eq!(ranges.read(&remote, &missing, 0..10).await?.len(), 10);
    Ok(())
}

#[tokio::test]
async fn cold_full_scan_coalesces_pages_without_foreground_admission() -> Result<()> {
    for budget in [(0, 0), (16 * 1024 * 1024, 256)] {
        let store = Arc::new(crate::engine::tests::store::Store::unpaced());
        let remote: Arc<dyn ObjectStore> = store.clone();
        let batch =
            Batch::from_updates((0..3000).map(|k| ((k, format!("{k:016x}").repeat(8)), 1)))?;
        let reference = ObjectBatch::write(remote.clone(), &batch, "scan", 1).await?;
        let foreground = Arc::new(BlockCache::new(budget.0, budget.1));
        let object = ObjectBatch::<i64, String>::open_with_cache(
            remote,
            reference,
            "scan",
            if budget.0 == 0 { foreground.clone() } else { BlockCache::scan(foreground.clone()) },
        )
        .await?;
        let before = foreground.stats()?.scan_io;
        let mut cursor = object.cursor().await?;
        for ((key, value), weight) in batch.iter() {
            assert_eq!(cursor.current(), Some((key, value, *weight)));
            cursor.advance().await?;
        }
        assert!(cursor.current().is_none());
        let reads = usize::try_from(foreground.stats()?.scan_io.gets - before.gets)?;
        let bytes = foreground.stats()?.scan_io.fetched_bytes - before.fetched_bytes;
        let expected =
            object.test_packs().await?.iter().map(|p| p.bytes.div_ceil(256 * 1024)).sum::<u64>();
        assert_eq!(u64::try_from(reads)?, expected);
        assert_eq!(bytes, object.test_packs().await?.iter().map(|p| p.bytes).sum::<u64>());
        assert!(reads * 100 < object.block_count());
        let stats = foreground.stats()?;
        assert_eq!((stats.entries, stats.bytes), (0, 0));
        if budget.0 == 0 {
            assert_eq!(stats.decoded_rows, 3000);
        } else {
            assert_eq!((stats.hits, stats.misses, stats.decoded_bytes), (0, 0, 0));
        }
        println!(
            "{}",
            serde_json::json!({"pages":object.block_count(),"scan_gets":reads,"scan_bytes":bytes,"foreground_budget":budget.0})
        );
    }
    Ok(())
}

#[tokio::test]
async fn scan_corruption_keeps_cursor_position_and_invalidates_private_window() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let batch = Batch::from_updates((0..10).map(|k| ((k, k), 1)))?;
    let reference = ObjectBatch::write(remote.clone(), &batch, "scan-corruption", 1).await?;
    let object =
        ObjectBatch::<i64, i64>::open(remote.clone(), reference, "scan-corruption").await?;
    let pack = object.test_packs().await?.first().cloned().context("missing pack")?;
    let path = Path::from(pack.path.clone());
    let original = remote.get(&path).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    corrupt[usize::try_from(object.leaf(1).await?.block.offset)?] ^= 1;
    remote.put(&path, corrupt.into()).await?;
    let mut cursor = object.cursor().await?;
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    remote.put(&path, original.into()).await?;
    let before = object.cache.stats()?.scan_io.gets;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    assert_eq!(object.cache.stats()?.scan_io.gets, before + 1);
    Ok(())
}

async fn sparse_windows(adaptive: bool) -> Result<(usize, u64)> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, super::WINDOW_BYTES * 64).await?;
    let mut ranges = Ranges::new(super::WINDOW_BYTES * 2, 2);
    ranges.scan = !adaptive;
    let before = store.reads.load(Ordering::SeqCst);
    let bytes = store.range_bytes.load(Ordering::SeqCst);
    for n in 0..96 {
        let start = u64::try_from((n % 64) * super::WINDOW_BYTES + 123)?;
        let result = ranges.read(&remote, &pack, start..start + 1024).await?;
        assert_eq!(
            result,
            (start..start + 1024).map(|i| u8::try_from(i % 251)).collect::<Result<Vec<_>, _>>()?
        );
    }
    Ok((
        store.reads.load(Ordering::SeqCst) - before,
        store.range_bytes.load(Ordering::SeqCst) - bytes,
    ))
}
#[tokio::test]
async fn sparse_pressure_reduces_unused_prefetch_without_increasing_gets() -> Result<()> {
    let before = sparse_windows(false).await?;
    let after = sparse_windows(true).await?;
    assert_eq!(before.0, 96);
    assert_eq!(after.0, before.0);
    assert!(
        after.1 * 8 < before.1,
        "sparse prefetch bytes did not shrink: {before:?} -> {after:?}"
    );
    println!(
        "{}",
        serde_json::json!({"fixed_gets":before.0,"adaptive_gets":after.0,"fixed_bytes":before.1,"adaptive_bytes":after.1})
    );
    Ok(())
}
#[tokio::test]
async fn dense_pressure_retains_coalescing_and_read_origin_accounting() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, super::WINDOW_BYTES * 4).await?;
    let ranges = Ranges::new(super::WINDOW_BYTES * 2, 2);
    let before = store.reads.load(Ordering::SeqCst);
    for start in (0..pack.bytes).step_by(1024) {
        ranges.read(&remote, &pack, start..start + 1024).await?;
    }
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 4);
    assert_eq!(
        ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width,
        super::WINDOW_BYTES
    );
    let scans = Ranges::scan(ranges.metrics.clone());
    for start in (0..pack.bytes).step_by(1024) {
        scans.read(&remote, &pack, start..start + 1024).await?;
    }
    let (foreground, scan, _) = ranges.metrics.snapshot();
    assert_eq!(foreground.gets, 4);
    assert_eq!(foreground.fetched_bytes, pack.bytes);
    assert_eq!(foreground.requested_bytes, pack.bytes);
    assert_eq!(scan.gets, 4);
    assert_eq!(scan.requested_bytes, pack.bytes);
    assert_eq!(scan.fetched_bytes, pack.bytes);
    let total = ranges.metrics.total_io();
    assert_eq!(total.gets, 8);
    assert_eq!(total.requested_bytes, pack.bytes * 2);
    assert_eq!(total.fetched_bytes, pack.bytes * 2);
    Ok(())
}

#[tokio::test]
async fn non_power_of_two_budget_uses_one_ladder_for_sparse_to_dense_transitions() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, 300_000).await?;
    let ranges = Ranges::new(100_000, 4);
    {
        let mut state = ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
        for expected in [65536, 32768, 16384, 8192, 4096, 4096] {
            ranges.adapt(&mut state, 0);
            assert_eq!(state.width, expected);
        }
        for expected in [8192, 16384, 32768, 65536, 100_000, 100_000] {
            ranges.adapt(&mut state, 64);
            assert_eq!(state.width, expected);
        }
        drop(state);
    }
    for width in ranges.widths() {
        let cache = Ranges::new(100_000, 4);
        cache.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = width;
        let before = store.reads.load(Ordering::SeqCst);
        cache.read(&remote, &pack, 10..20).await?;
        cache.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = 100_000;
        cache.read(&remote, &pack, 10..20).await?;
        assert_eq!(store.reads.load(Ordering::SeqCst) - before, 1);
    }
    Ok(())
}

#[tokio::test]
async fn charged_windows_fit_small_cache_budgets() -> Result<()> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, super::WINDOW_BYTES).await?;
    let budget = 64 * 1024;
    let ranges = Ranges::new(budget, 4);
    ranges.read(&remote, &pack, 10..20).await?;
    ranges.read(&remote, &pack, 10..20).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    let state = ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
    assert_eq!(state.windows.len(), 1);
    assert_eq!(state.bytes, 32 * 1024);
    assert!(state.policy.usage() <= budget);
    drop(state);
    Ok(())
}

#[tokio::test]
async fn changing_width_never_retains_duplicate_compressed_covers() -> Result<()> {
    use super::{Identity, Window};
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let pack = packed(&remote, super::WINDOW_BYTES * 2).await?;
    let ranges = Ranges::new(super::WINDOW_BYTES + 4096, 32);
    ranges.read(&remote, &pack, 0..1).await?;
    let narrow = super::MIN_WINDOW_BYTES;
    let key: Identity = (
        pack.path.clone(),
        pack.hash.clone(),
        u64::try_from(narrow * 2)?,
        pack.bytes,
        u64::try_from(narrow)?,
    );
    let before = store.reads.load(Ordering::SeqCst);
    let start = key.2;
    let cached = ranges
        .fetch(&remote, key.clone(), start + u64::try_from(narrow)?, &(start..start + 1))
        .await?;
    assert_eq!(cached.len(), narrow);
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    ranges.retain(key, Window::new(Arc::new(vec![0; narrow]), 1 << 63))?;
    {
        let state = ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
        assert_eq!(state.windows.len(), 1);
        assert_eq!(state.bytes, super::WINDOW_BYTES);
        assert_eq!(
            state.windows.values().next().context("missing wide cover")?.used,
            1 | (1 << ((narrow * 2) / (super::WINDOW_BYTES / 64)))
                | (1 << ((narrow * 3 - 1) / (super::WINDOW_BYTES / 64)))
        );
        drop(state);
    }
    ranges.invalidate(&pack, &(0..pack.bytes))?;
    ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.width = narrow;
    for offset in [0, narrow] {
        let start = u64::try_from(offset)?;
        ranges.read(&remote, &pack, start..start + 1).await?;
    }
    assert_eq!(ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.windows.len(), 2);
    let key =
        (pack.path.clone(), pack.hash.clone(), 0, pack.bytes, u64::try_from(super::WINDOW_BYTES)?);
    ranges.retain(key, Window::new(Arc::new(vec![0; super::WINDOW_BYTES]), 1))?;
    let state = ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
    assert_eq!(state.windows.len(), 1);
    assert_eq!(state.bytes, super::WINDOW_BYTES);
    drop(state);
    Ok(())
}
