//! Unpaced production compactions and probes, with virtual object-store latency.
use super::{operator, value};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    execution::Limits,
    reader::{BatchReader as _, BlockCache},
    tests::store::Store,
    trace::{Run, TraceSnapshot},
};
use anyhow::{Context as _, Result, ensure};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::Ordering},
};
mod retirement;

fn affected_keys(size: i64, epoch: i64, cycle: i64) -> Result<Vec<i64>> {
    ensure!(size >= 256 && epoch >= 0 && cycle >= 0, "invalid growth sample coordinates");
    let offset = epoch
        .checked_mul(127)
        .and_then(|value| cycle.checked_mul(53).and_then(|cycle| value.checked_add(cycle)))
        .context("growth sample offset overflow")?;
    let mut keys = Vec::with_capacity(512);
    for n in 0..256_i64 {
        let start = n.checked_mul(size).context("growth sample start overflow")? / 256;
        let end = (n + 1).checked_mul(size).context("growth sample end overflow")? / 256;
        let jitter = (n * 9973).checked_add(offset).context("growth sample jitter overflow")?
            % (end - start);
        let index = start.checked_add(jitter).context("growth sample index overflow")?;
        let key = index.checked_mul(4).context("growth sample key overflow")?;
        keys.push(key);
        keys.push(key.checked_add(1 + cycle % 3).context("growth sample gap overflow")?);
    }
    Ok(keys)
}

#[test]
fn stratified_keys_cover_the_full_relation_and_reject_overflow() -> Result<()> {
    for size in [524_288_i64, 1_000_003, 4_194_304, 1 << 40] {
        for (epoch, cycle) in [(0, 0), (3, 7), (7, 27)] {
            let keys = affected_keys(size, epoch, cycle)?;
            assert_eq!(keys.len(), 512);
            let mut unique = std::collections::BTreeSet::new();
            for (stratum, pair) in keys.chunks_exact(2).enumerate() {
                assert_eq!(pair[0] % 4, 0);
                assert_eq!(pair[1] - pair[0], 1 + cycle % 3);
                let index = pair[0] / 4;
                let n = i64::try_from(stratum)?;
                assert!((n * size / 256..(n + 1) * size / 256).contains(&index));
                unique.insert(index);
            }
            assert_eq!(unique.len(), 256);
        }
    }
    assert!(affected_keys(255, 0, 0).is_err());
    assert!(affected_keys(i64::MAX, 0, 0).is_err());
    assert!(affected_keys(256, i64::MAX, 0).is_err());
    assert!(affected_keys(256, 0, i64::MAX).is_err());
    Ok(())
}

fn compaction_ranges(
    requests: crate::engine::tests::store::RangeRequests,
    metadata_paths: &BTreeSet<String>,
) -> Result<serde_json::Value> {
    fn summary(requests: &crate::engine::tests::store::RangeRequests) -> Result<serde_json::Value> {
        let (total, unique) = crate::engine::tests::store::range_totals(requests)?;
        Ok(serde_json::json!({"gets":requests.values().sum::<usize>(),
            "bytes":total,"unique_bytes":unique,"repeated_bytes":total-unique}))
    }
    let all = summary(&requests)?;
    let (data, metadata): (
        crate::engine::tests::store::RangeRequests,
        crate::engine::tests::store::RangeRequests,
    ) = requests.into_iter().partition(|((path, _, _), _)| !metadata_paths.contains(path));
    Ok(serde_json::json!({"revision":"authenticated-index-paths-v1",
        "all":all,"data":summary(&data)?,"metadata":summary(&metadata)?}))
}

struct Compaction {
    count: u64,
    reads: usize,
    bytes: u64,
    puts: usize,
    put_bytes: usize,
    wall_us: u128,
    io_us: u128,
    ranges: serde_json::Value,
    input_bytes: u64,
    input_data_bytes: u64,
}

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
        store.record_ranges();
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
    async fn step(&mut self, epoch: i64, cycle: i64) -> Result<serde_json::Value> {
        let keys = affected_keys(self.size, epoch, cycle)?;
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
        let clock = tokio::time::Instant::now();
        let before = self.cache.stats()?;
        let output = operator().evaluate(&input, &self.trace, Limits::default()).await?;
        let evaluated = clock.elapsed().as_micros();
        let after_evaluate = self.cache.stats()?;
        assert_eq!(output.batch, Batch::from_updates(expected)?);
        self.trace.validate_delta(&output.batch).await?;
        let validated = clock.elapsed().as_micros();
        let after_validate = self.cache.stats()?;
        self.trace =
            self.trace.append_validated(Run::Memory(Arc::new(output.batch)), output.time)?;
        self.trace = self.writer.flush(&self.trace).await?;
        let after_flush = self.cache.stats()?;
        Ok(serde_json::json!({
            "evaluate_us":evaluated,"validate_us":validated-evaluated,
            "flush_us":clock.elapsed().as_micros()-validated,
            "evaluate_data_gets":after_evaluate.foreground_io.gets-before.foreground_io.gets,
            "validate_data_gets":after_validate.foreground_io.gets-after_evaluate.foreground_io.gets,
            "flush_data_gets":after_flush.foreground_io.gets-after_validate.foreground_io.gets,
            "evaluate_index_gets":after_evaluate.index_reads-before.index_reads,
            "evaluate_index_requested_bytes":after_evaluate.index_requested_bytes-before.index_requested_bytes,
            "validate_index_gets":after_validate.index_reads-after_evaluate.index_reads,
            "flush_index_gets":after_flush.index_reads-after_validate.index_reads
        }))
    }
    async fn measure_compaction(&mut self) -> Result<Compaction> {
        self.store.take_ranges()?;
        let metadata_paths = self
            .trace
            .clone()
            .into_runs()
            .into_iter()
            .filter_map(|run| {
                let Run::Object(batch) = run else { return None };
                batch.index.pack.as_ref().map(|pack| batch.reference().child_path(&pack.path))
            })
            .collect::<BTreeSet<_>>();
        let index_before = self.cache.stats()?;
        let inputs = self.store.list(None).try_collect::<Vec<_>>().await?;
        let input_bytes = inputs.iter().map(|object| object.size).sum::<u64>();
        let input_data_bytes = inputs
            .iter()
            .filter(|object| {
                object.location.to_string().contains("/pack/")
                    && !metadata_paths.contains(object.location.as_ref())
            })
            .map(|object| object.size)
            .sum::<u64>();
        let merges = self.cache.stats()?.merges.iter().sum::<u64>();
        let compaction_reads = self.store.reads.load(Ordering::SeqCst);
        let compaction_bytes = self.store.range_bytes.load(Ordering::SeqCst);
        let compaction_puts = self.store.puts.load(Ordering::SeqCst);
        let compaction_put_bytes = self.store.put_bytes.load(Ordering::SeqCst);
        let compaction_start = std::time::Instant::now();
        let compaction_clock = tokio::time::Instant::now();
        self.trace = self.writer.compact(&self.trace).await?;
        let compaction_ranges = compaction_ranges(self.store.take_ranges()?, &metadata_paths)?;
        let index_after = self.cache.stats()?;
        assert_eq!(
            compaction_ranges["metadata"]["bytes"].as_u64(),
            Some(index_after.index_read_bytes - index_before.index_read_bytes)
        );
        assert_eq!(
            compaction_ranges["metadata"]["gets"].as_u64(),
            Some(index_after.index_reads - index_before.index_reads)
        );
        let compaction_wall_us = compaction_start.elapsed().as_micros();
        let compaction_virtual_io_us = compaction_clock.elapsed().as_micros();
        let compaction_reads = self.store.reads.load(Ordering::SeqCst) - compaction_reads;
        let compaction_bytes = self.store.range_bytes.load(Ordering::SeqCst) - compaction_bytes;
        assert_eq!(compaction_ranges["all"]["bytes"].as_u64(), Some(compaction_bytes));
        assert_eq!(
            compaction_ranges["all"]["gets"].as_u64(),
            Some(u64::try_from(compaction_reads)?)
        );
        let compaction_puts = self.store.puts.load(Ordering::SeqCst) - compaction_puts;
        let compaction_put_bytes =
            self.store.put_bytes.load(Ordering::SeqCst) - compaction_put_bytes;
        let compactions = self.cache.stats()?.merges.iter().sum::<u64>() - merges;
        assert!(compactions > 0);
        Ok(Compaction {
            count: compactions,
            reads: compaction_reads,
            bytes: compaction_bytes,
            puts: compaction_puts,
            put_bytes: compaction_put_bytes,
            wall_us: compaction_wall_us,
            io_us: compaction_virtual_io_us,
            ranges: compaction_ranges,
            input_bytes,
            input_data_bytes,
        })
    }
    async fn cycle(&mut self, cycle: i64, coalesced: bool) -> Result<serde_json::Value> {
        let compaction = self.measure_compaction().await?;
        self.cache.ranges.take_diagnostics()?;
        let before = self.cache.stats()?;
        self.store.read_peak.store(0, Ordering::SeqCst);
        let reads = self.store.reads.load(Ordering::SeqCst);
        let read_bytes = self.store.range_bytes.load(Ordering::SeqCst);
        let puts = self.store.puts.load(Ordering::SeqCst);
        let put_bytes = self.store.put_bytes.load(Ordering::SeqCst);
        let start = tokio::time::Instant::now();
        let wall = std::time::Instant::now();
        let mut phases = Vec::new();
        for epoch in 0..8 {
            phases.push(self.step(epoch, cycle).await?);
        }
        let elapsed = start.elapsed().as_micros();
        let wall_us = wall.elapsed().as_micros();
        let after = self.cache.stats()?;
        let foreground_read_peak = self.store.read_peak.load(Ordering::SeqCst);
        assert!(foreground_read_peak <= crate::engine::reader::READ_CONCURRENCY);
        let resident_roots =
            self.trace.clone().into_runs().iter().try_fold(0_usize, |sum, run| {
                let charge = match run {
                    Run::Memory(_) => 0,
                    Run::Object(batch) => batch.resident_index_bytes()?,
                };
                sum.checked_add(charge).ok_or_else(|| anyhow::anyhow!("root charge overflow"))
            })?;
        assert!(after.filter_bytes <= 4 * 1024 * 1024 && after.filter_entries <= 8192);
        let mut report = serde_json::json!({"rows":self.size,"cycle":cycle,"coalesced":coalesced,
            "changes":4096,"virtual_io_us":elapsed,"wall_us":wall_us,"phases":phases,
            "compactions":compaction.count,"compaction_reads":compaction.reads,
            "compaction_read_bytes":compaction.bytes,"compaction_wall_us":compaction.wall_us,
            "compaction_virtual_io_us":compaction.io_us,
            "compaction_puts":compaction.puts,"compaction_put_bytes":compaction.put_bytes,
            "reads":self.store.reads.load(Ordering::SeqCst)-reads,
            "read_bytes":self.store.range_bytes.load(Ordering::SeqCst)-read_bytes,
            "puts":self.store.puts.load(Ordering::SeqCst)-puts,
            "put_bytes":self.store.put_bytes.load(Ordering::SeqCst)-put_bytes,
            "data_reads":after.foreground_io.gets-before.foreground_io.gets,
            "data_read_bytes":after.foreground_io.fetched_bytes-before.foreground_io.fetched_bytes,
            "decoded_bytes":after.decoded_bytes-before.decoded_bytes,
            "filter_rejections":after.filter_rejections-before.filter_rejections,
            "filter_bytes":after.filter_bytes,"filter_entries":after.filter_entries,
            "cache_bytes":after.bytes,"cache_entries":after.entries,
            "live_bytes":after.live_bytes,"pins":after.pins,"runs":self.trace.run_count(),
            "index_bytes":after.index_bytes,"index_entries":after.index_entries,
            "index_decoded_bytes":after.index_decoded_bytes,
            "index_encoded_bytes":after.index_encoded_bytes,"index_encoded_entries":after.index_encoded_entries,
            "index_reads":after.index_reads-before.index_reads,
            "index_read_bytes":after.index_read_bytes-before.index_read_bytes,
            "index_requested_bytes":after.index_requested_bytes-before.index_requested_bytes,
            "resident_root_bytes":resident_roots,
            "state_rows":self.oracle.len(),"ranges":self.cache.ranges.take_diagnostics()?});
        report["foreground_read_peak"] = foreground_read_peak.into();
        report["warmup"] = false.into();
        report["compaction_ranges"] = compaction.ranges;
        report["compaction_input_bytes"] = compaction.input_bytes.into();
        report["compaction_input_data_bytes"] = compaction.input_data_bytes.into();
        Ok(report)
    }
    async fn verify(&self) -> Result<()> {
        // Cold reopen and independent full bag comparison follow measurement.
        let cold = Arrangement::new(self.store.clone(), "growing-statistics".into(), 4096)?
            .reopen(self.trace.manifest()?)
            .await?;
        let mut cursor = cold.cursor().await?;
        for (key, count) in &self.oracle {
            let expected = value(*key, *count);
            assert_eq!(cursor.current(), Some((key, &expected, 1)));
            cursor.advance().await?;
        }
        assert!(cursor.current().is_none());
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
                fixture.verify().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
#[ignore = "explicit long-lived production state transitions with virtual idle days"]
async fn long_lived_statistics_compactions() -> Result<()> {
    let mut fixture = Fixture::new(true)?;
    let mut gc = crate::catalog::GcCache::default();
    let start = tokio::time::Instant::now();
    for size in [524_288, 1_048_576, 2_097_152, 4_194_304] {
        fixture.grow(size)?;
        for cycle in 0..36 {
            let mut report = fixture.cycle(cycle, true).await?;
            // Declared before qualification: eight warmup cycles followed by
            // twenty-eight measured cycles; all retain oracle/resource checks.
            report["warmup"] = (cycle < 8).into();
            report["retirement"] = fixture.retire(&mut gc).await?;
            // Advancing idle time exercises clock longevity, not additional
            // unexecuted traffic. Every reported change was actually evaluated.
            tokio::time::advance(std::time::Duration::from_hours(12)).await;
            report["logical_elapsed_seconds"] = start.elapsed().as_secs().into();
            println!("{report}");
        }
        fixture.verify().await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
#[ignore = "explicit four-cycle cold-state capacity screen before endurance qualification"]
async fn cold_statistics_compactions() -> Result<()> {
    let mut fixture = Fixture::new(true)?;
    let mut gc = crate::catalog::GcCache::default();
    let start = tokio::time::Instant::now();
    for size in [2_097_152, 4_194_304] {
        fixture.grow(size)?;
        for cycle in 0..4 {
            let mut report = fixture.cycle(cycle, true).await?;
            report["retirement"] = fixture.retire(&mut gc).await?;
            report["logical_elapsed_seconds"] = start.elapsed().as_secs().into();
            println!("{report}");
        }
        fixture.verify().await?;
    }
    Ok(())
}

#[test]
fn compaction_range_kinds_use_authenticated_identity_not_shared_pack_prefix() -> Result<()> {
    let requests = BTreeMap::from([
        (("pgderive/pack/data".into(), 0, 100), 1),
        (("pgderive/pack/index".into(), 0, 20), 2),
    ]);
    let result = compaction_ranges(requests, &BTreeSet::from(["pgderive/pack/index".into()]))?;
    assert_eq!(result["data"]["bytes"], 100);
    assert_eq!(result["metadata"]["bytes"], 40);
    assert_eq!(result["metadata"]["unique_bytes"], 20);
    assert_eq!(result["metadata"]["repeated_bytes"], 20);
    assert_eq!(result["all"]["gets"], 3);
    Ok(())
}
