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
    let store = Arc::new(Store::new(Some(0)));
    let settings = Settings { store: store.clone(), ..settings() };
    let mut query = Query::new(plan()?, operators(), &settings)?;
    tick(&mut query, 1, 2).await?;
    let before = query.snapshot();
    let cache_before = query.cache_stats()?;
    assert!(freeze(&mut query).await?.upload().await.is_err());
    let cache_after = query.cache_stats()?;
    assert_eq!(
        (cache_before.bytes, cache_before.entries),
        (cache_after.bytes, cache_after.entries)
    );
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

fn sibling_plan() -> Result<crate::engine::plan::Plan> {
    use crate::engine::plan::{Arrangement, Kind, Node, Plan};
    let mut definition = plan()?.definition().clone();
    for (id, input) in [("d", "abc"), ("e", "d"), ("f", "e")] {
        definition.nodes.push(Node {
            id: id.into(),
            kind: Kind::Project,
            inputs: vec![input.into()],
            schema: "i64-vector-v1".into(),
        });
        definition.arrangements.push(Arrangement {
            id: id.into(),
            node: id.into(),
            schema: "i64-vector-v1".into(),
        });
    }
    definition.outputs = vec!["f".into()];
    Plan::new(definition)
}
fn sibling_operators() -> Result<super::super::Operators<i64, Vec<i64>>> {
    let mut callbacks = operators();
    for id in ["d", "e", "f"] {
        callbacks.insert(id.into(), operators().remove("a").context("missing identity project")?);
    }
    Ok(callbacks)
}
#[tokio::test(flavor = "current_thread")]
async fn sibling_uploads_share_fuel_and_rebase_after_foreground_progress() -> Result<()> {
    use crate::engine::tests::store::Store;
    use std::{
        future::{Future as _, poll_fn},
        task::Poll,
    };
    let settings = Settings { store: Arc::new(Store::unpaced()), ..settings() };
    let mut query = Query::new(sibling_plan()?, sibling_operators()?, &settings)?;
    let input = TimedBatch {
        time: 1,
        batch: (1..=3)
            .map(|source| Batch::from_updates((0..8).map(|key| ((key, vec![source]), 1))))
            .collect::<Result<_>>()?,
    };
    let work = query.prepare_memtable(input, 1_000_000).await?;
    query.commit(work)?;
    assert_eq!(query.snapshot().entries.len(), 8);
    let frozen = freeze(&mut query).await?;
    let captured = query.snapshot();
    let before = query.memtable_bytes()?;
    {
        let mut cancelled = Box::pin(frozen.upload());
        poll_fn(|cx| match cancelled.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(Ok(())),
            Poll::Ready(_) => {
                Poll::Ready(Err(anyhow::anyhow!("cancelled sibling upload failed to yield")))
            }
        })
        .await?;
    }
    assert_eq!(query.time(), 1);
    assert_eq!(query.memtable_bytes()?, before);
    assert_eq!(
        query.snapshot().trace("abc")?.materialize().await?,
        captured.trace("abc")?.materialize().await?
    );
    let mut upload =
        Box::pin(query.capture_flush("pgderive/upload-v1/retry-siblings".into()).await?.upload());
    poll_fn(|cx| match upload.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(_) => {
            Poll::Ready(Err(anyhow::anyhow!("sibling upload exhausted no shared fuel")))
        }
    })
    .await?;
    assert_eq!(query.time(), 1);
    assert_eq!(query.memtable_bytes()?, before);
    assert_eq!(
        query.snapshot().trace("abc")?.materialize().await?,
        captured.trace("abc")?.materialize().await?
    );
    tick(&mut query, 2, 1).await?;
    let expected = query.snapshot().trace("abc")?.materialize().await?;
    let uploaded = upload.await?;
    assert_eq!(uploaded.checkpoint().time, 1);
    let work = query.prepare_uploaded_flush(&uploaded).await?;
    query.commit_compaction(work)?;
    assert_eq!(query.time(), 2);
    assert_eq!(query.snapshot().trace("abc")?.materialize().await?, expected);
    Ok(())
}
