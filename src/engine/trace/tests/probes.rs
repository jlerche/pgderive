use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    reader::{BlockCache, ObjectBatch},
    trace::{KeyCursor, Run, Trace},
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
    let mut trace = Trace::default();
    trace.commit(
        trace
            .prepare_runs(
                vec![
                    Run::Object(wide.clone()),
                    Run::Object(unrelated.clone()),
                    Run::Memory(Arc::new(Batch::from_updates([((7, 1), -2), ((7, 2), 3)])?)),
                ],
                1,
            )
            .await?,
    )?;
    for (object, ordinal) in [(&wide, 0), (&wide, 4), (&unrelated, 0)] {
        store
            .delete(&Path::from(object.block_path(ordinal).context("missing fixture block")?))
            .await?;
    }
    let snapshot = trace.snapshot();
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
    let cache = Arc::new(BlockCache::new(80, 2));
    let batch = Batch::from_updates((0..20).map(|n| ((n, n + 1), 1)))?;
    let data = object(store.clone(), cache.clone(), &batch, 1).await?;
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(data), 1).await?)?;
    let snapshot = trace.snapshot();
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
    assert!(stats.bytes <= 80);
    assert!(stats.entries <= 2);
    let before = stats.misses;
    assert_eq!(rows(snapshot.key_cursor(&3).await?).await?, cold);
    assert!(cache.stats()?.misses > before);
    let disabled = Arc::new(BlockCache::new(1, 1));
    let data = object(store, disabled.clone(), &batch, 1).await?;
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(data), 1).await?)?;
    assert_eq!(rows(trace.snapshot().key_cursor(&3).await?).await?, cold);
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
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(data.clone()), 1).await?)?;
    let mut cursor = trace.snapshot().key_cursor(&7).await?;
    let path = Path::from(data.block_path(1).context("missing fixture block")?);
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
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
    store.delete(&Path::from(data.block_path(2).context("missing fixture block")?)).await?;
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
            let mut trace = Trace::default();
            trace.commit(
                trace
                    .prepare_runs(
                        vec![
                            Run::Object(
                                object(store.clone(), cache.clone(), &a, block_rows).await?,
                            ),
                            Run::Object(object(store, cache.clone(), &b, block_rows).await?),
                        ],
                        1,
                    )
                    .await?,
            )?;
            for key in 0..5 {
                let expected = Batch::from_updates((key * 4..key * 4 + 4).map(|n| ((key, n), 1)))?;
                assert_eq!(rows(trace.snapshot().key_cursor(&key).await?).await?, expected);
            }
            assert!(cache.stats()?.bytes <= budget);
        }
    }
    Ok(())
}
