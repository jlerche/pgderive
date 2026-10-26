use super::{Run, TraceSnapshot};
use crate::engine::Batch;
use crate::engine::{
    dataflow::Arrangement,
    reader::{BlockCache, KeyIdentity},
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::sync::Arc;

async fn key(trace: &TraceSnapshot<i64, i64>, found: i64) -> Result<Batch<i64, i64>> {
    let mut cursor = trace.key_cursor(&found).await?;
    let mut rows = crate::engine::BatchBuilder::default();
    while let Some((key, value, weight)) = cursor.current() {
        rows.push(*key, *value, weight);
        cursor.advance().await?;
    }
    rows.finish()
}
fn append(
    trace: &TraceSnapshot<i64, i64>,
    rows: impl IntoIterator<Item = ((i64, i64), i64)>,
) -> Result<TraceSnapshot<i64, i64>> {
    trace.append_validated(Run::Memory(Arc::new(Batch::from_updates(rows)?)), trace.time() + 1)
}
type Fixture = (Arrangement<i64, i64>, Arc<BlockCache>);
fn setup() -> Result<Fixture> {
    let cache = Arc::new(BlockCache::new(64 * 1024, 32));
    Ok((
        Arrangement::new(Arc::new(InMemory::new()), "logical-keys".into(), 2)?
            .with_cache(cache.clone()),
        cache,
    ))
}
#[tokio::test]
async fn write_through_preserves_old_payloads_branches_and_rollback() -> Result<()> {
    let (writer, cache) = setup()?;
    let base = append(&writer.empty(), [((1, 1), 2), ((1, 2), -3), ((2, 9), 1)])?;
    assert_eq!(key(&base, 1).await?, Batch::from_updates([((1, 1), 2), ((1, 2), -3)])?);
    let mut pinned = base.key_cursor(&1).await?;
    let read_hits = cache.stats()?.point_hits;
    let next = append(&base, [((1, 1), -2), ((1, 3), 5)])?;
    assert_eq!(cache.stats()?.point_hits, read_hits);
    let before = cache.stats()?.point_hits;
    assert_eq!(key(&next, 1).await?, Batch::from_updates([((1, 2), -3), ((1, 3), 5)])?);
    assert!(cache.stats()?.point_hits > before);
    assert_eq!(pinned.current(), Some((&1, &1, 2)));
    pinned.advance().await?;
    assert_eq!(pinned.current(), Some((&1, &2, -3)));
    assert_eq!(key(&base, 1).await?, Batch::from_updates([((1, 1), 2), ((1, 2), -3)])?);
    let branch = append(&base, [((1, 4), 7)])?;
    assert_eq!(
        key(&branch, 1).await?,
        Batch::from_updates([((1, 1), 2), ((1, 2), -3), ((1, 4), 7)])?
    );
    assert_eq!(key(&next, 1).await?, Batch::from_updates([((1, 2), -3), ((1, 3), 5)])?);
    assert_eq!(key(&base, 1).await?, Batch::from_updates([((1, 1), 2), ((1, 2), -3)])?);
    Ok(())
}
#[tokio::test]
async fn delayed_partial_and_cloned_cursors_never_publish_incomplete_or_stale_keys() -> Result<()> {
    let (writer, cache) = setup()?;
    let base = append(&writer.empty(), [((1, 1), 1), ((1, 2), 1)])?;
    let mut delayed = base.key_cursor(&1).await?;
    delayed.advance().await?;
    assert_eq!(cache.stats()?.point_entries, 0);
    let mut cloned = delayed.clone();
    cloned.advance().await?;
    assert_eq!(cache.stats()?.point_entries, 0);
    let next = append(&base, [((1, 3), 1)])?;
    key(&next, 1).await?;
    delayed.advance().await?;
    assert_eq!(key(&next, 1).await?, Batch::from_updates([((1, 1), 1), ((1, 2), 1), ((1, 3), 1)])?);
    Ok(())
}
#[tokio::test]
async fn empty_retractions_physical_compaction_rebase_and_reopen_are_exact() -> Result<()> {
    let (writer, cache) = setup()?;
    let initial = writer.empty();
    assert_eq!(key(&initial, 1).await?, Batch::default());
    let base = append(&initial, [((1, 1), 1)])?;
    key(&base, 1).await?;
    let captured = base.seal_memory();
    let flushed = writer.flush(&captured).await?;
    let before = cache.stats()?.point_hits;
    assert_eq!(key(&flushed, 1).await?, Batch::from_updates([((1, 1), 1)])?);
    assert!(cache.stats()?.point_hits > before);
    let next = append(&captured, [((1, 2), 2)])?;
    let rebased = next.rebase_flushed(&captured, &flushed)?;
    assert_eq!(key(&rebased, 1).await?, Batch::from_updates([((1, 1), 1), ((1, 2), 2)])?);
    let compact = writer.compact(&rebased).await?;
    assert_eq!(key(&compact, 1).await?, key(&rebased, 1).await?);
    let reopened = writer.reopen(compact.manifest()?).await?;
    assert_eq!(key(&reopened, 1).await?, key(&rebased, 1).await?);
    let empty = append(&rebased, [((1, 1), -1), ((1, 2), -2)])?;
    assert_eq!(key(&empty, 1).await?, Batch::default());
    let nonempty = append(&empty, [((1, 9), -2)])?;
    assert_eq!(key(&nonempty, 1).await?, Batch::from_updates([((1, 9), -2)])?);
    let mut scan = nonempty.clone();
    scan.share_cache(&BlockCache::scan(cache.clone()));
    let before = cache.stats()?.point_hits;
    key(&scan, 1).await?;
    assert_eq!(cache.stats()?.point_hits, before);
    assert!(
        nonempty
            .logical_cache
            .as_ref()
            .context("missing cache")?
            .lookup::<i64, i64>(&KeyIdentity::new(&1)?, 0)
            .0
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn subset_compaction_cannot_reuse_complete_parent_results() -> Result<()> {
    let (writer, cache) = setup()?;
    let base = append(&writer.empty(), [((1, 1), 2), ((2, 9), 1)])?;
    let base = writer.flush(&base).await?;
    let repeated =
        TraceSnapshot::from_runs(vec![base.runs[0].clone(), base.runs[0].clone()], 2, 2)?;
    assert_eq!(key(&repeated, 1).await?, Batch::from_updates([((1, 1), 4)])?);
    let subset = TraceSnapshot::from_runs(vec![base.runs[0].clone()], 2, 2)?;
    let subset = writer.compact(&subset).await?;
    assert_eq!(key(&subset, 1).await?, Batch::from_updates([((1, 1), 2)])?);
    assert_eq!(key(&repeated, 1).await?, Batch::from_updates([((1, 1), 4)])?);
    assert!(cache.stats()?.point_bytes <= 16 * 1024);
    Ok(())
}

#[tokio::test]
async fn oversized_full_key_streams_without_optional_admission() -> Result<()> {
    let cache = Arc::new(BlockCache::new(64 * 1024, 32));
    let writer = Arrangement::new(Arc::new(InMemory::new()), "wide-logical".into(), 2)?
        .with_cache(cache.clone());
    let trace = writer.empty().append_validated(
        Run::Memory(Arc::new(Batch::from_updates([((1_i64, "x".repeat(1024 * 1024)), 1)])?)),
        1,
    )?;
    let mut cursor = trace.key_cursor(&1).await?;
    assert_eq!(cursor.current().map(|(_, value, _)| value.len()), Some(1024 * 1024));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    assert_eq!(cache.stats()?.point_entries, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_logical_probe_admits_nothing_and_retries_exactly() -> Result<()> {
    use crate::engine::{reader::ObjectBatch, tests::store::Store};
    use std::time::Duration;
    let mut store = Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(64 * 1024, 32));
    let batch = Batch::from_updates([((1_i64, 7_i64), -3), ((1, 8), 2)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "logical-cancel", 1).await?;
    let object = ObjectBatch::open_with_cache(store, root, "logical-cancel", cache.clone()).await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object)], 1, 1)?;
    assert!(tokio::time::timeout(Duration::from_millis(5), trace.key_cursor(&1)).await.is_err());
    assert_eq!(cache.stats()?.point_entries, 0);
    assert_eq!(key(&trace, 1).await?, batch);
    assert_eq!(cache.stats()?.point_entries, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_continuation_preserves_position_and_admits_only_after_retry() -> Result<()> {
    use crate::engine::{reader::ObjectBatch, tests::store::Store};
    use std::time::Duration;
    let mut store = Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(128 * 1024, 32));
    cache.fix_range_width(4096)?;
    let mut seed = 42_u64;
    let batch = Batch::from_updates((0..128).map(|_| {
        let value = (0..128)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                char::from(b'a' + seed.to_be_bytes()[0] % 26)
            })
            .collect::<String>();
        ((1_i64, value), 1)
    }))?;
    let root = ObjectBatch::write(store.clone(), &batch, "logical-continuation", 16).await?;
    let object = ObjectBatch::<i64, String>::open_with_cache(
        store,
        root,
        "logical-continuation",
        cache.clone(),
    )
    .await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object)], 1, 1)?;
    let mut cursor = trace.key_cursor(&1).await?;
    let mut cancelled = false;
    while cursor.current().is_some() {
        let before = cursor.current().map(|(key, value, weight)| (*key, value.clone(), weight));
        if let Ok(result) = tokio::time::timeout(Duration::from_millis(5), cursor.advance()).await {
            result?;
            continue;
        }
        assert_eq!(
            cursor.current().map(|(key, value, weight)| (*key, value.clone(), weight)),
            before
        );
        cancelled = true;
        break;
    }
    assert!(cancelled, "fixture must cross a cold compressed window mid-key");
    assert_eq!(cache.stats()?.point_entries, 0);
    cursor.advance().await?;
    while cursor.current().is_some() {
        cursor.advance().await?;
    }
    assert_eq!(cache.stats()?.point_entries, 1);
    let mut replay = trace.key_cursor(&1).await?;
    let mut rows = crate::engine::BatchBuilder::default();
    while let Some((key, value, weight)) = replay.current() {
        rows.push(*key, value.clone(), weight);
        replay.advance().await?;
    }
    assert_eq!(rows.finish()?, batch);
    Ok(())
}

#[tokio::test]
async fn cached_small_key_becoming_oversized_invalidates_without_changing_results() -> Result<()> {
    let cache = Arc::new(BlockCache::new(128 * 1024, 32));
    let writer = Arrangement::new(Arc::new(InMemory::new()), "large-delta".into(), 2)?
        .with_cache(cache.clone());
    let old = Batch::from_updates([((1_i64, "small".to_owned()), 1)])?;
    let base = writer.empty().append_validated(Run::Memory(Arc::new(old)), 1)?;
    let mut warm = base.key_cursor(&1).await?;
    warm.advance().await?;
    assert_eq!(cache.stats()?.point_entries, 1);
    let delta =
        Batch::from_updates([((1, "small".to_owned()), -1), ((1, "x".repeat(1024 * 1024)), 1)])?;
    let next = base.append_validated(Run::Memory(Arc::new(delta)), 2)?;
    let mut cursor = next.key_cursor(&1).await?;
    assert_eq!(
        cursor.current().map(|(_, value, weight)| (value.len(), weight)),
        Some((1024 * 1024, 1))
    );
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    let mut old = base.key_cursor(&1).await?;
    assert_eq!(
        old.current().map(|(_, value, weight)| (value.as_str(), weight)),
        Some(("small", 1))
    );
    old.advance().await?;
    assert!(cache.stats()?.point_bytes <= 32 * 1024);
    Ok(())
}

#[tokio::test]
async fn optional_write_through_overflow_does_not_reject_complete_cancellation() -> Result<()> {
    let (writer, _) = setup()?;
    let base = append(&writer.empty(), [((1, 1), i64::MAX)])?;
    key(&base, 1).await?;
    let added = [1, -1]
        .into_iter()
        .map(|weight| Ok(Run::Memory(Arc::new(Batch::from_updates([((1, 1), weight)])?))))
        .collect::<Result<Vec<_>>>()?;
    let next = base.append_runs_validated(added, 2)?;
    assert_eq!(key(&next, 1).await?, Batch::from_updates([((1, 1), i64::MAX)])?);
    assert_eq!(key(&base, 1).await?, Batch::from_updates([((1, 1), i64::MAX)])?);
    Ok(())
}

#[tokio::test]
async fn hot_replacements_survive_one_use_churn_and_repeated_physical_cycles() -> Result<()> {
    let cache = Arc::new(BlockCache::new(16 * 1024, 32));
    let writer = Arrangement::new(Arc::new(InMemory::new()), "replacement-churn".into(), 16)?
        .with_cache(cache.clone());
    let weight = |key: i64| if key % 2 == 0 { -2 } else { 3 };
    let mut trace = append(&writer.empty(), (1..=4).map(|id| ((id, 0), weight(id))))?;
    for id in 1..=4 {
        assert_eq!(key(&trace, id).await?, Batch::from_updates([((id, 0), weight(id))])?);
    }
    let mut steady_hits = 0;
    let mut windows = Vec::new();
    for epoch in 1..=512_i64 {
        let before = cache.stats()?.point_hits;
        for id in 1..=4 {
            assert_eq!(
                key(&trace, id).await?,
                Batch::from_updates([((id, epoch - 1), weight(id))])?
            );
        }
        let hits = cache.stats()?.point_hits - before;
        if epoch > 32 {
            steady_hits += hits;
        }
        if epoch % 32 == 0 {
            windows.push((epoch, hits, steady_hits));
        }
        let mut delta = Vec::new();
        for id in 1..=4 {
            delta.push(((id, epoch - 1), -weight(id)));
            delta.push(((id, epoch), weight(id)));
        }
        let cold = 1000 + epoch;
        delta.push(((cold, epoch), 1));
        trace = append(&trace, delta)?;
        assert_eq!(key(&trace, cold).await?, Batch::from_updates([((cold, epoch), 1)])?);
        if epoch % 8 == 0 {
            trace = writer.flush(&trace).await?;
        }
        if epoch % 16 == 0 {
            trace = writer.compact(&trace).await?;
        }
        let stats = cache.stats()?;
        assert!(stats.point_bytes <= 4096 && stats.point_entries <= 8);
    }
    // Four repeatedly demanded entries fit half the point budget. One-use cold
    // keys must not evict them once the first 32 iterations have warmed policy.
    assert_eq!(steady_hits, 4 * (512 - 32), "replacement/churn windows: {windows:?}");
    Ok(())
}
