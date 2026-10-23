use super::super::tests::{operators, plan};
use super::*;
use crate::engine::{Batch, dataflow::TimedBatch, plan::Settings};
use object_store::memory::InMemory;

type Query = Relational<i64, Vec<i64>>;
async fn freeze(query: &mut Query) -> Result<FrozenFlush<i64, Vec<i64>>> {
    query.capture_flush("pgderive/upload-v1/frozen-test".into()).await
}

fn settings() -> Settings {
    Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 16,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    }
}
async fn tick(query: &mut Query, time: u64, weight: i64) -> Result<()> {
    let input = TimedBatch {
        time,
        batch: (1..=3)
            .map(|n| Batch::from_updates([((0, vec![n]), weight)]))
            .collect::<Result<_>>()?,
    };
    let work = query.prepare_memtable(input, 1_000_000).await?;
    query.commit(work)?;
    Ok(())
}

#[tokio::test]
async fn uploaded_snapshot_rebases_after_newer_join_transactions() -> Result<()> {
    let settings = settings();
    let mut query = Query::new(plan()?, operators(), &settings)?;
    tick(&mut query, 1, 3).await?;
    let upload = tokio::spawn(freeze(&mut query).await?.upload());
    for (time, weight) in [(2, -1), (3, 1), (4, -1)] {
        tick(&mut query, time, weight).await?;
    }
    let uploaded = upload.await??;
    assert_eq!(uploaded.checkpoint().time, 1);
    let expected = query.snapshot().trace("abc")?.materialize().await?;
    let before = query.memtable_bytes()?;
    let work = query.prepare_uploaded_flush(&uploaded).await?;
    query.commit_compaction(work)?;
    assert_eq!(query.time(), 4);
    assert!(query.memtable_bytes()? < before);
    assert_eq!(query.snapshot().trace("abc")?.materialize().await?, expected);
    let mut restored =
        Query::reopen(plan()?, operators(), settings, uploaded.checkpoint().clone()).await?;
    for (time, weight) in [(2, -1), (3, 1), (4, -1)] {
        tick(&mut restored, time, weight).await?;
    }
    assert_eq!(restored.snapshot().trace("abc")?.materialize().await?, expected);
    assert!(query.prepare_uploaded_flush(&uploaded).await.is_err());
    Ok(())
}

#[tokio::test]
async fn uploaded_snapshot_rejects_foreign_and_recovered_runtimes() -> Result<()> {
    let settings = settings();
    let mut first = Query::new(plan()?, operators(), &settings)?;
    let second = Query::new(plan()?, operators(), &settings)?;
    let uploaded = freeze(&mut first).await?.upload().await?;
    assert!(second.prepare_uploaded_flush(&uploaded).await.is_err());
    first.restore_checkpoint(uploaded.checkpoint().clone()).await?;
    assert!(first.prepare_uploaded_flush(&uploaded).await.is_err());
    Ok(())
}

#[tokio::test]
async fn freeze_fences_work_prepared_before_the_new_memory_boundary() -> Result<()> {
    let mut query = Query::new(plan()?, operators(), &settings())?;
    tick(&mut query, 1, 2).await?;
    let input = TimedBatch {
        time: 2,
        batch: (1..=3).map(|n| Batch::from_updates([((0, vec![n]), 1)])).collect::<Result<_>>()?,
    };
    let prepared = query.prepare_memtable(input, 1_000_000).await?;
    let maintenance = query.prepare_compaction().await?;
    let frozen = freeze(&mut query).await?;
    assert!(query.commit(prepared).is_err());
    assert!(query.commit_compaction(maintenance).is_err());
    assert_eq!(query.time(), 1);
    tick(&mut query, 2, 1).await?;
    let expected = query.snapshot().trace("abc")?.materialize().await?;
    let uploaded = frozen.upload().await?;
    let work = query.prepare_uploaded_flush(&uploaded).await?;
    query.commit_compaction(work)?;
    assert_eq!(query.snapshot().trace("abc")?.materialize().await?, expected);
    Ok(())
}

#[tokio::test]
async fn failed_upload_drains_siblings_without_changing_local_state() -> Result<()> {
    use crate::engine::tests::store::Store;
    use std::sync::atomic::Ordering;
    let store = Arc::new(Store::new(true));
    let settings = Settings { store: store.clone(), ..settings() };
    let mut query = Query::new(plan()?, operators(), &settings)?;
    tick(&mut query, 1, 2).await?;
    let before = query.snapshot();
    assert!(freeze(&mut query).await?.upload().await.is_err());
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert!(store.peak.load(Ordering::SeqCst) > 1);
    assert!(store.puts.load(Ordering::SeqCst) > 1);
    assert_eq!(
        before.trace("abc")?.materialize().await?,
        query.snapshot().trace("abc")?.materialize().await?
    );
    assert_eq!(query.time(), 1);
    Ok(())
}
