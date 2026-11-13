use crate::engine::{
    Batch,
    dataflow::Arrangement,
    reader::{BatchReader, BlockCache, ObjectBatch, WriteLimits},
    trace::{Manifest, TraceSnapshot},
};
use anyhow::Result;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory};
use std::{hint::black_box, sync::Arc, time::Instant};

// Controlled in-memory adapter measurement; not cloud latency or RSS evidence.
#[tokio::test]
#[ignore = "manual controlled performance measurement"]
async fn reader_execution_measurement() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates(
        (0_i64..4096).map(|n| ((n / 4, format!("row-{n:08}-{}", "payload".repeat(8))), 1)),
    )?;
    let start = Instant::now();
    let root = ObjectBatch::write(store.clone(), &batch, "perf-string-v1", 128).await?;
    println!("write_us={}", start.elapsed().as_micros());
    let cache = Arc::new(BlockCache::new(8 * 1024 * 1024, 128));
    let object = ObjectBatch::<i64, String>::open_with_cache(
        store.clone(),
        root.clone(),
        "perf-string-v1",
        cache.clone(),
    )
    .await?;
    let start = Instant::now();
    for n in 0..2000 {
        let mut cursor = object.cursor().await?;
        cursor.seek_key(&(n % 1024)).await?;
        black_box(cursor.current());
    }
    println!("point_2000_us={}", start.elapsed().as_micros());
    let start = Instant::now();
    for _ in 0..200 {
        let mut cursor = object.cursor().await?;
        for _ in 0..20 {
            black_box(cursor.current());
            cursor.advance().await?;
        }
    }
    println!("prefix_200_us={}", start.elapsed().as_micros());
    let start = Instant::now();
    for _ in 0..20 {
        let mut cursor = object.cursor().await?;
        while cursor.current().is_some() {
            black_box(cursor.current());
            cursor.advance().await?;
        }
    }
    println!("scan_20_us={}", start.elapsed().as_micros());
    let start = Instant::now();
    for _ in 0..10 {
        let roots = ObjectBatch::write_stream(
            store.clone(),
            &mut object.cursor().await?,
            "perf-string-v1",
            WriteLimits::default(),
        )
        .await?;
        black_box(roots);
    }
    println!("stream_rewrite_10_us={}", start.elapsed().as_micros());
    measure_merged(store.clone(), root.clone(), cache.clone()).await?;
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    println!(
        "objects={};stored_bytes={}",
        objects.len(),
        objects.iter().map(|object| object.size).sum::<u64>()
    );
    Ok(())
}

async fn measure_merged(
    store: Arc<dyn ObjectStore>,
    root: crate::engine::reader::ObjectRef,
    cache: Arc<BlockCache>,
) -> Result<()> {
    let right = right_batch()?;
    let right_root = ObjectBatch::write(store.clone(), &right, "perf-string-v1", 128).await?;
    let manifest = Manifest {
        version: 2,
        time: 0,
        generation: 0,
        cohorts: vec![2],
        objects: vec![root, right_root],
    };
    let prior = TraceSnapshot::<i64, String>::reopen(
        store.clone(),
        manifest.clone(),
        "perf-string-v1",
        cache.clone(),
    )
    .await?;
    let start = Instant::now();
    for _ in 0..20 {
        let mut cursor = prior.cursor().await?;
        while cursor.current().is_some() {
            black_box(cursor.current());
            cursor.advance().await?;
        }
    }
    println!("merge_20_us={}", start.elapsed().as_micros());
    let arrangement = Arrangement::<i64, String>::new(store.clone(), "perf-string-v1".into(), 128)?
        .with_cache(cache.clone());
    let expected = expected_merged()?;
    assert_eq!(prior.materialize().await?, expected);
    let start = Instant::now();
    for _ in 0..10 {
        let compacted = arrangement.compact(&prior).await?;
        assert_eq!(compacted.materialize().await?, expected);
    }
    println!("compact_10_us={}", start.elapsed().as_micros());
    measure_recovery(store, manifest, &expected, &cache).await?;
    Ok(())
}

fn expected_merged() -> Result<Batch<i64, String>> {
    let mut raw = std::collections::BTreeMap::new();
    for n in 0_i64..4096 {
        for identity in [n, if n < 2048 { n } else { n + 4096 }] {
            let tuple = (identity / 4, format!("row-{identity:08}-{}", "payload".repeat(8)));
            *raw.entry(tuple).or_insert(0_i64) += 1;
        }
    }
    Batch::from_updates(raw)
}

fn right_batch() -> Result<Batch<i64, String>> {
    Batch::from_updates((0_i64..4096).map(|n| {
        let identity = if n < 2048 { n } else { n + 4096 };
        ((identity / 4, format!("row-{identity:08}-{}", "payload".repeat(8))), 1)
    }))
}

async fn measure_recovery(
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    expected: &Batch<i64, String>,
    cache: &BlockCache,
) -> Result<()> {
    let start = Instant::now();
    for _ in 0..10 {
        let cold = TraceSnapshot::<i64, String>::reopen(
            store.clone(),
            manifest.clone(),
            "perf-string-v1",
            Arc::new(BlockCache::new(0, 0)),
        )
        .await?;
        assert_eq!(&cold.materialize().await?, expected);
    }
    println!("cold_recovery_10_us={}; cache={:?}", start.elapsed().as_micros(), cache.stats()?);
    Ok(())
}
