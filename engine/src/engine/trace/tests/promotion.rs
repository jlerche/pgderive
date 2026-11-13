//! Threshold-adjacent generated-page qualification through production uploads.
use crate::engine::{
    Batch,
    dataflow::Arrangement,
    reader::BlockCache,
    tests::store::Store,
    trace::{Run, TraceSnapshot},
};
use anyhow::Result;
use std::sync::{Arc, atomic::Ordering};

async fn sample(pages: i64) -> Result<serde_json::Value> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(1024 * 1024, 4));
    let writer = Arrangement::<i64, i64>::new(store.clone(), "promotion".into(), 1)?
        .with_cache(cache.clone());
    let expected = Batch::from_updates((0..pages).map(|key| ((key, key), 1)))?;
    let memory = TraceSnapshot::from_runs(vec![Run::Memory(Arc::new(expected.clone()))], 0, 1)?;
    let trace = writer.compact(&memory).await?;
    let puts = store.puts.load(Ordering::SeqCst);
    let bytes = store.put_bytes.load(Ordering::SeqCst);
    let retained = cache.stats()?.entries;
    let before = store.reads.load(Ordering::SeqCst);
    let decoded = cache.stats()?.decoded_rows;
    // Include a key outside the retained prefix. Every probe must remain exact.
    for key in 0..pages {
        let cursor = trace.key_cursor(&key).await?;
        assert_eq!(cursor.current(), Some((&key, &key, 1)));
    }
    let reads = store.reads.load(Ordering::SeqCst) - before;
    let decoded = cache.stats()?.decoded_rows - decoded;
    let cold = writer.reopen(trace.manifest()?).await?;
    assert_eq!(cold.materialize().await?, expected);
    assert!(cache.stats()?.bytes <= 1024 * 1024);
    assert!(cache.stats()?.entries <= 4);
    Ok(serde_json::json!({"pages":pages,"retained":retained,"reads":reads,
        "decoded_rows":decoded,"puts":puts,"put_bytes":bytes}))
}

#[tokio::test]
async fn threshold_outputs_preserve_exact_cold_and_point_results() -> Result<()> {
    for pages in [3, 4, 5, 16] {
        sample(pages).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "paired generated-page threshold report"]
async fn report_promotion() -> Result<()> {
    let mut samples = Vec::new();
    for pages in [3, 4, 5, 16] {
        samples.push(sample(pages).await?);
    }
    println!("{}", serde_json::to_string(&samples)?);
    Ok(())
}

#[tokio::test]
async fn failed_output_does_not_promote_pages_or_change_the_snapshot() -> Result<()> {
    for fail_at in [0, 1] {
        let store = Arc::new(Store::new(Some(fail_at)));
        let cache = Arc::new(BlockCache::new(1024 * 1024, 4));
        let writer = Arrangement::<i64, i64>::new(store, "promotion-fail".into(), 1)?
            .with_cache(cache.clone());
        let expected = Batch::from_updates((0..16).map(|key| ((key, key), 1)))?;
        let memory = TraceSnapshot::from_runs(vec![Run::Memory(Arc::new(expected.clone()))], 0, 1)?;
        let before = (memory.generation(), memory.time(), memory.run_count());
        assert!(writer.compact(&memory).await.is_err());
        assert_eq!((memory.generation(), memory.time(), memory.run_count()), before);
        assert_eq!(memory.materialize().await?, expected);
        assert_eq!(cache.stats()?.entries, 0);
        assert_eq!(cache.stats()?.filter_entries, 0);
    }
    Ok(())
}

#[tokio::test]
async fn unrelated_hot_arrangement_survives_fitting_generated_output() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(1024 * 1024, 4));
    let hot_writer =
        Arrangement::<i64, i64>::new(store.clone(), "hot".into(), 1)?.with_cache(cache.clone());
    let hot_batch = Batch::from_updates((0..4).map(|key| ((key, key), 1)))?;
    let hot_memory =
        TraceSnapshot::from_runs(vec![Run::Memory(Arc::new(hot_batch.clone()))], 0, 1)?;
    let hot = hot_writer.compact(&hot_memory).await?;
    assert_eq!(cache.stats()?.entries, 4);
    assert_eq!(hot.materialize().await?, hot_batch);
    let output_writer =
        Arrangement::<i64, i64>::new(store.clone(), "output".into(), 1)?.with_cache(cache.clone());
    let output = Batch::from_updates((10..14).map(|key| ((key, key), 1)))?;
    let memory = TraceSnapshot::from_runs(vec![Run::Memory(Arc::new(output.clone()))], 0, 1)?;
    let output = output_writer.compact(&memory).await?;
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(hot.materialize().await?, hot_batch);
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert_eq!(output.materialize().await?, memory.materialize().await?);
    Ok(())
}
