//! Unpaced production compactions and probes, with virtual object-store latency.
use super::{operator, value};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    execution::Limits,
    reader::BlockCache,
    tests::store::Store,
    trace::{Run, TraceSnapshot},
};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};

struct Fixture {
    store: Arc<Store>,
    cache: Arc<BlockCache>,
    writer: Arrangement<i64, String>,
    trace: TraceSnapshot<i64, String>,
    oracle: BTreeMap<i64, i64>,
    size: i64,
}
impl Fixture {
    fn new(coalesced: bool) -> Result<Self> {
        let mut store = Store::unpaced();
        store.read_delay = std::time::Duration::from_millis(20);
        let store = Arc::new(store);
        let mut cache = BlockCache::new(16 * 1024 * 1024, 512);
        if !coalesced {
            Arc::get_mut(&mut cache.ranges)
                .ok_or_else(|| anyhow::anyhow!("shared fresh range cache"))?
                .disable_coalescing();
        }
        let cache = Arc::new(cache);
        let writer = Arrangement::new(store.clone(), "growing-statistics".into(), 4096)?
            .with_cache(cache.clone());
        let trace = writer.empty();
        Ok(Self { store, cache, writer, trace, oracle: BTreeMap::new(), size: 0 })
    }
    fn grow(&mut self, size: i64) -> Result<()> {
        let growing =
            Batch::from_updates((self.size..size).map(|key| ((4 * key, value(4 * key, 0)), 1)))?;
        self.oracle.extend((self.size..size).map(|key| (4 * key, 0_i64)));
        self.trace =
            self.trace.append_validated(Run::Memory(Arc::new(growing)), self.trace.time() + 1)?;
        self.size = size;
        Ok(())
    }
    async fn step(&mut self, epoch: i64, cycle: i64) -> Result<()> {
        let keys = (0..256_i64)
            .flat_map(|n| {
                let index = (n * 9973 + epoch * 127 + cycle * 53) % self.size;
                [4 * index, 4 * index + 1 + cycle]
            })
            .collect::<Vec<_>>();
        let input = TimedBatch {
            time: self.trace.time() + 1,
            batch: Batch::from_updates(keys.iter().map(|key| ((*key, 1_i64), 1)))?,
        };
        let mut expected = Vec::new();
        for key in keys {
            if let Some(old) = self.oracle.get(&key) {
                expected.push(((key, value(key, *old)), -1));
            }
            let count = self.oracle.entry(key).or_default();
            *count += 1;
            expected.push(((key, value(key, *count)), 1));
        }
        let output = operator().evaluate(&input, &self.trace, Limits::default()).await?;
        assert_eq!(output.batch, Batch::from_updates(expected)?);
        self.trace.validate_delta(&output.batch).await?;
        self.trace =
            self.trace.append_validated(Run::Memory(Arc::new(output.batch)), output.time)?;
        self.trace = self.writer.flush(&self.trace).await?;
        Ok(())
    }
    async fn cycle(&mut self, cycle: i64, coalesced: bool) -> Result<serde_json::Value> {
        let merges = self.cache.stats()?.merges.iter().sum::<u64>();
        let compaction_reads = self.store.reads.load(Ordering::SeqCst);
        let compaction_bytes = self.store.range_bytes.load(Ordering::SeqCst);
        let compaction_start = std::time::Instant::now();
        self.trace = self.writer.compact(&self.trace).await?;
        let compaction_wall_us = compaction_start.elapsed().as_micros();
        let compaction_reads = self.store.reads.load(Ordering::SeqCst) - compaction_reads;
        let compaction_bytes = self.store.range_bytes.load(Ordering::SeqCst) - compaction_bytes;
        let compactions = self.cache.stats()?.merges.iter().sum::<u64>() - merges;
        assert!(compactions > 0);
        let before = self.cache.stats()?;
        let reads = self.store.reads.load(Ordering::SeqCst);
        let read_bytes = self.store.range_bytes.load(Ordering::SeqCst);
        let start = tokio::time::Instant::now();
        let wall = std::time::Instant::now();
        for epoch in 0..8 {
            self.step(epoch, cycle).await?;
        }
        let elapsed = start.elapsed().as_micros();
        let wall_us = wall.elapsed().as_micros();
        let after = self.cache.stats()?;
        assert!(after.filter_bytes <= 4 * 1024 * 1024 && after.filter_entries <= 8192);
        let report = serde_json::json!({"rows":self.size,"cycle":cycle,"coalesced":coalesced,
            "changes":4096,"virtual_io_us":elapsed,"wall_us":wall_us,
            "compactions":compactions,"compaction_reads":compaction_reads,
            "compaction_read_bytes":compaction_bytes,"compaction_wall_us":compaction_wall_us,
            "reads":self.store.reads.load(Ordering::SeqCst)-reads,
            "read_bytes":self.store.range_bytes.load(Ordering::SeqCst)-read_bytes,
            "decoded_bytes":after.decoded_bytes-before.decoded_bytes,
            "filter_rejections":after.filter_rejections-before.filter_rejections,
            "filter_bytes":after.filter_bytes,"filter_entries":after.filter_entries});
        self.verify().await?;
        Ok(report)
    }
    async fn verify(&self) -> Result<()> {
        // Cold reopen and independent full bag comparison follow measurement.
        let expected = Batch::from_updates(
            self.oracle.iter().map(|(key, count)| ((*key, value(*key, *count)), 1)),
        )?;
        let cold = Arrangement::new(self.store.clone(), "growing-statistics".into(), 4096)?
            .reopen(self.trace.manifest()?)
            .await?;
        assert_eq!(cold.materialize().await?, expected);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "explicit unpaced growing-state multi-compaction qualification"]
async fn growing_statistics_compactions() -> Result<()> {
    for coalesced in [false, true] {
        let mut fixture = Fixture::new(coalesced)?;
        for size in [32_000, 64_000, 128_000, 262_144, 524_288] {
            fixture.grow(size)?;
            for cycle in 0..2 {
                println!("{}", fixture.cycle(cycle, coalesced).await?);
            }
        }
    }
    Ok(())
}
