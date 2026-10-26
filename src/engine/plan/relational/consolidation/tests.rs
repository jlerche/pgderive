use super::*;
use crate::engine::plan::relational::tests::{operators, plan};
use crate::engine::{Batch, dataflow::TimedBatch, plan::Settings, tests::store::Store};
use std::sync::atomic::Ordering;
type Query = Relational<i64, Vec<i64>>;
fn settings(store: Arc<Store>) -> Settings {
    Settings {
        store,
        block_rows: 16,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    }
}
async fn tick(query: &mut Query) -> Result<()> {
    let work = query
        .prepare_memtable(
            TimedBatch {
                time: query.time() + 1,
                batch: (1..=3)
                    .map(|n| Batch::from_updates([((0, vec![n]), 1)]))
                    .collect::<Result<Vec<_>>>()?,
            },
            1_000_000,
        )
        .await?;
    query.commit(work)?;
    Ok(())
}
async fn checkpoint(query: &mut Query) -> Result<Checkpoint> {
    let uploaded = query
        .capture_flush(format!("pgderive/upload-v1/checkpoint-{}", query.time()))
        .await?
        .upload()
        .await?;
    let work = query.prepare_uploaded_flush(&uploaded).await?;
    query.commit_compaction(work)?;
    Ok(uploaded.checkpoint().clone())
}
async fn seed(query: &mut Query) -> Result<Checkpoint> {
    for _ in 0..8 {
        tick(query).await?;
        checkpoint(query).await?;
    }
    query.checkpoint()
}
#[tokio::test(start_paused = true)]
async fn cold_consolidation_does_not_block_checkpoints_and_keeps_both_suffixes() -> Result<()> {
    let mut storage = Store::unpaced();
    storage.read_delay = std::time::Duration::from_mins(10);
    let store = Arc::new(storage);
    let mut query = Query::new(plan()?, operators(), &settings(store.clone()))?;
    let captured = seed(&mut query).await?;
    assert!(query.consolidation_due(&captured)?);
    let mut job =
        query.capture_consolidation(&captured, "pgderive/upload-v1/consolidation".into())?;
    // Deliberately cold maintenance cache; foreground point/cache state stays
    // warm so retained-object reads belong exclusively to the blocked merge.
    let mut execution = (*job.execution).clone();
    execution.cache = Arc::new(BlockCache::new(0, 0));
    job.execution = Arc::new(execution);
    let task = tokio::spawn(job.upload());
    for _ in 0..100 {
        if store.read_active.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(store.read_active.load(Ordering::SeqCst) > 0);
    let started = tokio::time::Instant::now();
    let reads = store.reads.load(Ordering::SeqCst);
    // Two newer durable checkpoints and an uncheckpointed transaction overtake
    // the exact pinned object prefix, with no retained-object GETs.
    tick(&mut query).await?;
    checkpoint(&mut query).await?;
    tick(&mut query).await?;
    let durable = checkpoint(&mut query).await?;
    tick(&mut query).await?;
    assert_eq!(tokio::time::Instant::now(), started);
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert!(!task.is_finished());
    let uploaded = task.await??;
    let expected = query.snapshot().trace("abc")?.materialize().await?;
    let (work, replacement) = query.prepare_uploaded_consolidation(&uploaded, &durable).await?;
    assert_eq!(replacement.time, durable.time);
    for (before, after) in durable.arrangements.iter().zip(&replacement.arrangements) {
        assert_eq!(after.trace.generation, before.trace.generation + 1);
        assert!(after.trace.objects.len() < before.trace.objects.len());
    }
    query.commit_compaction(work)?;
    assert_eq!(query.time(), 11);
    assert!(query.memtable_bytes()? > 0);
    assert_eq!(query.snapshot().trace("abc")?.materialize().await?, expected);
    let mut cold = Query::reopen(plan()?, operators(), settings(store), replacement).await?;
    tick(&mut cold).await?;
    assert_eq!(cold.snapshot().trace("abc")?.materialize().await?, expected);
    assert!(query.prepare_uploaded_consolidation(&uploaded, &durable).await.is_err());
    Ok(())
}
#[tokio::test]
async fn consolidation_capability_rejects_foreign_and_recovered_runtimes() -> Result<()> {
    let settings = settings(Arc::new(Store::unpaced()));
    let mut query = Query::new(plan()?, operators(), &settings)?;
    let checkpoint = seed(&mut query).await?;
    let uploaded = query
        .capture_consolidation(&checkpoint, "pgderive/upload-v1/owned".into())?
        .upload()
        .await?;
    let foreign = Query::new(plan()?, operators(), &settings)?;
    assert!(foreign.prepare_uploaded_consolidation(&uploaded, &checkpoint).await.is_err());
    query.restore_checkpoint(checkpoint.clone()).await?;
    assert!(query.prepare_uploaded_consolidation(&uploaded, &checkpoint).await.is_err());
    Ok(())
}
