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

async fn packed(store: &Arc<dyn ObjectStore>, size: usize) -> Result<Pack> {
    let bytes = (0..size).map(|i| u8::try_from(i % 251)).collect::<Result<Vec<_>, _>>()?;
    let hash = crate::engine::reader::format::hash(&bytes);
    let path = format!("pgderive/pack/{hash}");
    store.put(&Path::from(path.clone()), bytes.into()).await?;
    Ok(Pack { path, hash, bytes: u64::try_from(size)? })
}
#[tokio::test]
async fn windows_coalesce_concurrent_reads_cross_boundaries_and_preserve_scan_policy() -> Result<()>
{
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn ObjectStore> = store.clone();
    let width = u64::try_from(super::WINDOW_BYTES)?;
    let pack = packed(&remote, usize::try_from(width * 3 + 7)?).await?;
    let ranges = Ranges::new(super::WINDOW_BYTES * 2, 2);
    let (a, b) = tokio::try_join!(
        ranges.read(&remote, &pack, width - 10..width + 20, true),
        ranges.read(&remote, &pack, width - 5..width + 25, true)
    )?;
    assert_eq!(
        a,
        (width - 10..width + 20).map(|i| u8::try_from(i % 251)).collect::<Result<Vec<_>, _>>()?
    );
    assert_eq!(b.len(), 30);
    assert_eq!(store.reads.load(Ordering::SeqCst), 2);
    ranges.read(&remote, &pack, width * 2..width * 3 + 7, true).await?;
    assert!(
        ranges.state.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.bytes
            <= super::WINDOW_BYTES * 2
    );
    let before = store.reads.load(Ordering::SeqCst);
    ranges.read(&remote, &pack, 0..10, false).await?;
    ranges.read(&remote, &pack, 0..10, false).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before + 2);
    ranges.invalidate(&pack, &(width * 2..pack.bytes))?;
    assert!(ranges.read(&remote, &pack, pack.bytes - 1..pack.bytes + 1, true).await.is_err());
    assert!(ranges.read(&remote, &pack, 5..5, true).await.is_err());
    for limits in [(0, 4), (100, 0), (1, 1)] {
        let cache = Ranges::new(limits.0, limits.1);
        assert_eq!(cache.read(&remote, &pack, 0..10, true).await?.len(), 10);
    }
    Ok(())
}
fn value(key: i64, count: i64) -> String {
    format!("{count}:{}", format!("{key:016x}").repeat(48))
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
    let operator = Statistics::with_measure(
        1,
        |_, _: &i64| Ok(vec![BigInt::from(1)]),
        |s: &String| {
            Ok(vec![BigInt::from(s.split(':').next().context("missing count")?.parse::<i64>()?)])
        },
        |k, v| Ok(Some(value(*k, i64::try_from(v[0].clone())?))),
    );
    let input = TimedBatch {
        time: 2,
        batch: Batch::from_updates((0..513).map(|n| (((n * 193) % 100_000, 0_i64), 1)))?,
    };
    let before = store.reads.load(Ordering::SeqCst);
    let started = Instant::now();
    let output = operator.evaluate(&input, &trace, Limits::default()).await?;
    // This is the same full-identity coefficient validation used by staging.
    trace.validate_delta(&output.batch).await?;
    let reads = store.reads.load(Ordering::SeqCst) - before;
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
        serde_json::json!({"baseline_gets":before.0,"coalesced_gets":after.0,"baseline_ms":before.1,"coalesced_ms":after.1})
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
    let pack = object.packs.first().context("missing pack")?;
    let path = Path::from(pack.path.clone());
    let original = remote.get(&path).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    let offset = usize::try_from(object.blocks[1].offset)?;
    corrupt[offset] ^= 1;
    remote.put(&path, corrupt.into()).await?;
    assert_eq!(object.read(0).await?[0].0, (0, 0));
    assert!(object.read(1).await.is_err());
    remote.put(&path, original.into()).await?;
    let before = store.reads.load(Ordering::SeqCst);
    assert_eq!(object.read(1).await?[0].0, (1, 1));
    assert_eq!(store.reads.load(Ordering::SeqCst), before + 1);
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
        tokio::spawn(async move { ranges.read(&remote, &pack, 0..10, true).await })
    };
    while store.read_active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.is_err());
    assert_eq!(ranges.read(&remote, &pack, 0..10, true).await?.len(), 10);
    let mut missing = pack.clone();
    missing.path = "missing".into();
    assert!(ranges.read(&remote, &missing, 0..10, true).await.is_err());
    missing.path = pack.path;
    assert_eq!(ranges.read(&remote, &missing, 0..10, true).await?.len(), 10);
    Ok(())
}
