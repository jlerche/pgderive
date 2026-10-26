//! Immediate stores and warm memory must not hide unbounded task polls.
use super::{
    BatchCursor, BatchReader as _, BlockCache, Cursor, ObjectBatch, ObjectRef, WriteLimits,
};
use crate::engine::{
    Batch,
    tests::store::Store,
    trace::{Manifest, Run, TraceSnapshot},
};
use anyhow::Result;
use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

const SCHEMA: &str = "cooperative-maintenance-v1";
#[derive(Debug, Default, serde::Serialize)]
struct Polls {
    pending: usize,
    max_rows: usize,
    max_poll_ns: u128,
}
async fn measured<T>(future: impl Future<Output = T>, rows: &AtomicUsize) -> (T, Polls) {
    let mut future = std::pin::pin!(future);
    let mut stats = Polls::default();
    let output = poll_fn(|cx| {
        let before = rows.load(Ordering::Relaxed);
        let started = std::time::Instant::now();
        let poll = future.as_mut().poll(cx);
        stats.max_poll_ns = stats.max_poll_ns.max(started.elapsed().as_nanos());
        stats.max_rows = stats.max_rows.max(rows.load(Ordering::Relaxed) - before);
        if poll.is_pending() {
            stats.pending += 1;
        }
        poll
    })
    .await;
    (output, stats)
}
struct Counted {
    cursor: Cursor<i64, i64>,
    rows: Arc<AtomicUsize>,
}
impl BatchCursor for Counted {
    type Key = i64;
    type Val = i64;
    fn current(&self) -> Option<(&i64, &i64, i64)> {
        self.cursor.current()
    }
    async fn advance(&mut self) -> Result<()> {
        self.cursor.advance().await?;
        self.rows.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    async fn seek_key(&mut self, key: &i64) -> Result<()> {
        self.cursor.seek_key(key).await
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Costs {
    puts: usize,
    bytes: usize,
    reads: usize,
    read_bytes: u64,
}
fn costs(store: &Store) -> Costs {
    Costs {
        puts: store.puts.load(Ordering::SeqCst),
        bytes: store.put_bytes.load(Ordering::SeqCst),
        reads: store.reads.load(Ordering::SeqCst),
        read_bytes: store.range_bytes.load(Ordering::SeqCst),
    }
}
async fn oracle(
    store: Arc<Store>,
    roots: Vec<ObjectRef>,
    expected: &Batch<i64, i64>,
) -> Result<()> {
    let trace = TraceSnapshot::reopen(
        store,
        Manifest {
            version: 2,
            time: 1,
            generation: 1,
            cohorts: if roots.is_empty() { vec![] } else { vec![roots.len()] },
            objects: roots,
        },
        SCHEMA,
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(&trace.materialize().await?, expected);
    Ok(())
}
async fn streamed(unconstrained: bool) -> Result<(Vec<ObjectRef>, Costs, Polls)> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..8192_i64).map(|key| ((key, key * 7), 1)))?;
    let rows = Arc::new(AtomicUsize::new(0));
    let mut cursor = Counted { cursor: batch.cursor().await?, rows: rows.clone() };
    let write =
        ObjectBatch::write_stream(store.clone(), &mut cursor, SCHEMA, WriteLimits::default());
    let (roots, stats) = if unconstrained {
        measured(tokio::task::unconstrained(write), &rows).await
    } else {
        measured(write, &rows).await
    };
    let roots = roots?;
    let work = costs(&store);
    oracle(store, roots.clone(), &batch).await?;
    Ok((roots, work, stats))
}
fn report(case: &str, cooperative: &Polls, baseline: &Polls) -> Result<()> {
    if let Some(path) = std::env::var_os("PGDERIVE_COOPERATIVE_REPORT") {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        let mut record = serde_json::to_vec(&serde_json::json!({
            "case": case, "cooperative": cooperative, "unconstrained": baseline,
        }))?;
        record.push(b'\n');
        file.write_all(&record)?;
    }
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn streaming_yields_without_changing_roots_or_requests() -> Result<()> {
    let (roots, cost, polls) = tokio::spawn(streamed(false)).await??;
    let (baseline, baseline_cost, baseline_polls) = tokio::spawn(streamed(true)).await??;
    assert_eq!(roots, baseline);
    assert_eq!(cost, baseline_cost);
    assert!(polls.pending > 0);
    assert!(polls.max_rows <= 128, "{polls:?}");
    assert_eq!(baseline_polls.pending, 0);
    assert_eq!(baseline_polls.max_rows, 8192);
    report("streaming", &polls, &baseline_polls)?;
    Ok(())
}
async fn compact(unconstrained: bool, retained: bool) -> Result<(Vec<ObjectRef>, Costs, Polls)> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..16384_i64).map(|key| ((key, key), 1)))?;
    let cache = Arc::new(BlockCache::new(4 * 1024 * 1024, 2048));
    let runs = if retained {
        let object =
            ObjectBatch::write_resident(store.clone(), &batch, SCHEMA, (None, 32), cache.clone())
                .await?;
        vec![Run::Object(object)]
    } else {
        let negative = Batch::from_updates(batch.iter().map(|(tuple, weight)| (*tuple, -*weight)))?;
        vec![Run::Memory(Arc::new(batch.clone())), Run::Memory(Arc::new(negative))]
    };
    let before = costs(&store);
    let rows = AtomicUsize::new(0);
    let write = ObjectBatch::compact_resident(
        store.clone(),
        &runs,
        SCHEMA,
        (None, WriteLimits::default()),
        cache,
    );
    let (objects, stats) = if unconstrained {
        measured(tokio::task::unconstrained(write), &rows).await
    } else {
        measured(write, &rows).await
    };
    let roots = objects?.iter().map(|object| object.reference().clone()).collect::<Vec<_>>();
    let after = costs(&store);
    let work = Costs {
        puts: after.puts - before.puts,
        bytes: after.bytes - before.bytes,
        reads: after.reads - before.reads,
        read_bytes: after.read_bytes - before.read_bytes,
    };
    let expected = if retained { batch } else { Batch::from_updates([])? };
    oracle(store, roots.clone(), &expected).await?;
    Ok((roots, work, stats))
}
#[tokio::test(flavor = "current_thread")]
async fn retained_blocks_and_zero_output_cancellation_both_yield() -> Result<()> {
    for retained in [false, true] {
        let (roots, cost, polls) = tokio::spawn(compact(false, retained)).await??;
        let (baseline, baseline_cost, baseline_polls) =
            tokio::spawn(compact(true, retained)).await??;
        assert_eq!(roots, baseline);
        assert_eq!(cost, baseline_cost);
        assert!(polls.pending > 0, "retained={retained}: {polls:?}");
        assert_eq!(baseline_polls.pending, 0);
        if !retained {
            assert_eq!(cost.puts, 1, "empty canonical output writes only its root");
        }
        report(if retained { "retained-blocks" } else { "zero-output" }, &polls, &baseline_polls)?;
    }
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn partial_cancelled_upload_retries_without_publishing_membership() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let old = Batch::from_updates([((-1_i64, -1_i64), 1)])?;
    let root = ObjectBatch::write(store.clone(), &old, SCHEMA, 32).await?;
    let manifest =
        Manifest { version: 2, time: 1, generation: 1, cohorts: vec![1], objects: vec![root] };
    let prior = TraceSnapshot::<i64, i64>::reopen(
        store.clone(),
        manifest.clone(),
        SCHEMA,
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    let batch = Batch::from_updates((0..2048_i64).map(|key| ((key, key), 1)))?;
    let limits = WriteLimits { block_rows: 32, index_blocks: 1, ..WriteLimits::default() };
    let before = store.puts.load(Ordering::SeqCst);
    let mut cursor = batch.cursor().await?;
    {
        let mut write = std::pin::pin!(ObjectBatch::write_stream_namespaced(
            store.clone(),
            &mut cursor,
            SCHEMA,
            ("pgderive/upload-v1/cancelled", limits)
        ));
        poll_fn(|cx| match write.as_mut().poll(cx) {
            Poll::Pending if store.puts.load(Ordering::SeqCst) > before => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                Poll::Ready(Err(anyhow::anyhow!("upload completed before partial cancellation")))
            }
        })
        .await?;
    }
    assert!(store.puts.load(Ordering::SeqCst) > before);
    assert_eq!(prior.manifest()?, manifest);
    assert_eq!(prior.materialize().await?, old);
    let roots = ObjectBatch::write_stream_namespaced(
        store.clone(),
        &mut batch.cursor().await?,
        SCHEMA,
        ("pgderive/upload-v1/retry", limits),
    )
    .await?;
    assert!(roots.iter().all(|root| root.path().starts_with("pgderive/upload-v1/retry/")));
    oracle(store, roots, &batch).await?;
    assert_eq!(prior.manifest()?, manifest);
    Ok(())
}

async fn index_only(unconstrained: bool) -> Result<(Vec<u8>, Polls)> {
    use super::format::{Block, Pack};
    let pack = Pack {
        path: format!("pgderive/pack/{}", "0".repeat(64)),
        bytes: 4096,
        hash: "0".repeat(64),
    };
    let blocks = (0..4096_i64)
        .map(|key| {
            Ok(Block {
                pack: 0,
                offset: u64::try_from(key)?,
                first: (key, key),
                last: (key, key),
                length: 1,
                rows: 1,
                max_weight: 1,
                hash: "0".repeat(64),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let packs = [pack];
    let work = super::index::build(SCHEMA, super::bounded::MAX_BYTES, &packs, &blocks);
    let rows = AtomicUsize::new(0);
    let (result, polls) = if unconstrained {
        measured(tokio::task::unconstrained(work), &rows).await
    } else {
        measured(work, &rows).await
    };
    let (root, bytes) = result?;
    let mut identity = serde_json::to_vec(&root)?;
    identity.extend_from_slice(&bytes);
    Ok((identity, polls))
}
#[tokio::test(flavor = "current_thread")]
async fn index_finalization_yields_without_changing_authenticated_bytes() -> Result<()> {
    let (bytes, polls) = tokio::spawn(index_only(false)).await??;
    let (baseline, baseline_polls) = tokio::spawn(index_only(true)).await??;
    assert_eq!(bytes, baseline);
    assert!(polls.pending > 0);
    assert_eq!(baseline_polls.pending, 0);
    report("index-finalization", &polls, &baseline_polls)?;
    Ok(())
}
