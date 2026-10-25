//! The production freeze/upload/rebase lifecycle, paced by executed source rows.
use super::*;
use crate::engine::{
    dataflow::TimedBatch,
    execution::Sealed,
    plan::relational::{MemtableFull, Prepared, UploadedFlush},
};
use serde::Serialize;
use std::time::Duration;
type Key = program::relational::Key;
type Input = TimedBatch<Vec<Sealed<Key, Row>>>;
type Pending = tokio::task::JoinHandle<Result<UploadedFlush<Key, Row>>>;
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub memtable_bytes: usize,
    pub checkpoint_interval_ms: u64,
    pub maintenance_ticks: usize,
    pub source_rows_per_second: u64,
}
impl Config {
    pub(super) fn validate(self) -> Result<()> {
        ensure!(
            self.memtable_bytes > 0 && self.source_rows_per_second > 0,
            "invalid replay pacing"
        );
        ensure!((1..=60_000).contains(&self.checkpoint_interval_ms), "invalid replay interval");
        ensure!((1..=64).contains(&self.maintenance_ticks), "invalid replay run bound");
        Ok(())
    }
}
#[derive(Default, Serialize)]
pub(super) struct Report {
    pub logical_ms: u64,
    pub source_rows: u64,
    pub checkpoints: usize,
    pub rebased_uploads: usize,
    pub capacity_flushes: usize,
    pub restarts: usize,
    pub maintenance_ns: u128,
    pub maximum_runs: usize,
    pub maximum_memtable_bytes: usize,
    pub samples: Vec<serde_json::Value>,
    pub retirement: Vec<serde_json::Value>,
}
pub(super) struct Lifecycle {
    config: Config,
    captured_ms: u64,
    pending: Option<Pending>,
    pub report: Report,
    store: Arc<crate::engine::tests::store::Store>,
    gc: crate::catalog::GcCache,
}
impl Drop for Lifecycle {
    fn drop(&mut self) {
        if let Some(task) = self.pending.take() {
            task.abort();
        }
    }
}
impl Lifecycle {
    pub(super) fn new(config: Config, store: Arc<crate::engine::tests::store::Store>) -> Self {
        Self {
            config,
            captured_ms: 0,
            pending: None,
            report: Report::default(),
            store,
            gc: crate::catalog::GcCache::default(),
        }
    }
    async fn install(&mut self, query: &mut program::relational::Query) -> Result<()> {
        if let Some(task) = self.pending.take() {
            let uploaded = task.await??;
            self.report.rebased_uploads += usize::from(query.time() > uploaded.checkpoint().time);
            let roots = uploaded
                .checkpoint()
                .arrangements
                .iter()
                .flat_map(|member| member.trace.objects.clone())
                .collect();
            let work = query.prepare_uploaded_flush(&uploaded).await?;
            query.commit_compaction(work)?;
            drop(uploaded);
            self.report.retirement.push(
                crate::engine::tests::retire::retire(&self.store, roots, &mut self.gc).await?,
            );
        }
        Ok(())
    }
    async fn capture(&mut self, query: &mut program::relational::Query) -> Result<()> {
        ensure!(self.pending.is_none(), "replay upload already pending");
        let namespace = format!("pgderive/upload-v1/replay-{}", query.time());
        self.pending = Some(tokio::spawn(query.capture_flush(namespace).await?.upload()));
        self.captured_ms = self.report.logical_ms;
        self.report.checkpoints += 1;
        Ok(())
    }
    pub(super) async fn flush(&mut self, query: &mut program::relational::Query) -> Result<()> {
        let started = Instant::now();
        self.install(query).await?;
        self.capture(query).await?;
        self.install(query).await?;
        self.report.maintenance_ns += started.elapsed().as_nanos();
        Ok(())
    }
    pub(super) async fn prepare(
        &mut self,
        query: &mut program::relational::Query,
        input: Input,
    ) -> Result<Prepared<Key, Row>> {
        match query.prepare_memtable(input.clone(), self.config.memtable_bytes).await {
            Ok(work) => Ok(work),
            Err(error) if error.is::<MemtableFull>() => {
                self.report.capacity_flushes += 1;
                self.flush(query).await?;
                query.prepare_memtable(input, self.config.memtable_bytes).await
            }
            Err(error) => Err(error),
        }
    }
    pub(super) async fn committed(
        &mut self,
        query: &mut program::relational::Query,
        count: u64,
    ) -> Result<()> {
        self.report.source_rows += count;
        self.report.logical_ms =
            self.report.source_rows.checked_mul(1000).context("replay time overflow")?
                / self.config.source_rows_per_second;
        let elapsed = self.report.logical_ms - self.captured_ms;
        self.report.maximum_runs = self.report.maximum_runs.max(query.run_count());
        self.report.maximum_memtable_bytes =
            self.report.maximum_memtable_bytes.max(query.memtable_bytes()?);
        let started = Instant::now();
        // Ingestion committed after capture before installing this exact prefix.
        self.install(query).await?;
        if elapsed >= self.config.checkpoint_interval_ms
            || query.unsealed_run_count() >= self.config.maintenance_ticks
        {
            self.capture(query).await?;
        }
        self.report.maintenance_ns += started.elapsed().as_nanos();
        Ok(())
    }
    pub(super) fn observe(
        &mut self,
        query: &program::relational::Query,
        prepare_ns: u128,
    ) -> Result<()> {
        let cache = query.cache_stats()?;
        self.report.samples.push(serde_json::json!({
            "logical_ms":self.report.logical_ms,"source_rows":self.report.source_rows,"prepare_ns":prepare_ns,
            "runs":query.run_count(),"memtable_bytes":query.memtable_bytes()?,"maintenance_ns":self.report.maintenance_ns,
            "cache":crate::worker::health::cache_diagnostics(&cache),
            "gets":self.store.reads.load(Ordering::SeqCst),
            "read_bytes":self.store.range_bytes.load(Ordering::SeqCst),
            "puts":self.store.puts.load(Ordering::SeqCst),
            "put_bytes":self.store.put_bytes.load(Ordering::SeqCst)
        }));
        Ok(())
    }
    pub(super) async fn advance(&self, prior_ms: u64) {
        tokio::time::advance(Duration::from_millis(self.report.logical_ms - prior_ms)).await;
    }
}
