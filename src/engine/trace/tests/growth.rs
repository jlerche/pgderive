//! Unpaced fixed-change maintenance measurements, separate from freshness tests.
use super::Run;
use crate::engine::tests::store::Store;
use crate::engine::{Batch, dataflow::Arrangement, reader::BlockCache};
use anyhow::Result;
use std::sync::atomic::Ordering;
use std::{sync::Arc, time::Instant};

#[tokio::test]
#[ignore = "explicit paired storage growth benchmark"]
async fn fixed_change_checkpoint_growth() -> Result<()> {
    let mut reports = Vec::new();
    for rows in [2000_i64, 8000, 32000, 128_000] {
        let cache = Arc::new(BlockCache::new(0, 0));
        let store = Arc::new(Store::unpaced());
        let writer =
            Arrangement::new(store.clone(), "growth".into(), 1024)?.with_cache(cache.clone());
        let base = Batch::from_updates((0..rows).map(|key| ((key, key), 1)))?;
        let mut oracle = base.clone();
        let mut trace =
            writer.flush(&writer.empty().append_validated(Run::Memory(Arc::new(base)), 1)?).await?;
        let initial_reads = store.reads.load(Ordering::SeqCst);
        let initial_bytes = store.range_bytes.load(Ordering::SeqCst);
        let initial_puts = store.puts.load(Ordering::SeqCst);
        let initial_put_bytes = store.put_bytes.load(Ordering::SeqCst);
        let mut elapsed = Vec::new();
        let mut max_roots = 0;
        for time in 2..=257 {
            let key = rows + i64::try_from(time)?;
            let delta = Batch::from_updates([((key, key), 1)])?;
            oracle.apply(&delta)?;
            let pending = trace.append_validated(Run::Memory(Arc::new(delta)), time)?;
            let started = Instant::now();
            trace = writer.flush(&pending).await?;
            elapsed.push(started.elapsed().as_nanos());
            max_roots = max_roots.max(trace.run_count());
        }
        let reads = store.reads.load(Ordering::SeqCst) - initial_reads;
        let read_bytes = store.range_bytes.load(Ordering::SeqCst) - initial_bytes;
        let puts = store.puts.load(Ordering::SeqCst) - initial_puts;
        let put_bytes = store.put_bytes.load(Ordering::SeqCst) - initial_put_bytes;
        let restored = writer.reopen(trace.manifest()?).await?;
        assert_eq!(trace.materialize().await?, oracle);
        assert_eq!(restored.materialize().await?, oracle);
        reports.push(serde_json::json!({"initial_rows":rows,"changes":256,
            "flush_ns":elapsed,"reads":reads,"read_bytes":read_bytes,
            "puts":puts,"put_bytes":put_bytes,
            "max_roots":max_roots,"final_roots":trace.run_count(),"weighted_bag_equal":true}));
    }
    let output = std::env::var("PGDERIVE_STORAGE_GROWTH_REPORT")?;
    std::fs::write(output, serde_json::to_vec_pretty(&reports)?)?;
    Ok(())
}
