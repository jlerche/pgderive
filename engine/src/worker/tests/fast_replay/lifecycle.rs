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
struct Pending {
    task: tokio::task::JoinHandle<Result<UploadedFlush<Key, Row>>>,
    source_rows: u64,
    logical_ms: u64,
}

pub(super) fn process_cpu_ticks() -> Result<u64> {
    let text = std::fs::read_to_string("/proc/self/stat")?;
    let (_, fields) = text.rsplit_once(") ").context("missing process stat fields")?;
    let mut fields = fields.split_whitespace().skip(11);
    let user = fields.next().context("missing process user CPU")?.parse::<u64>()?;
    let system = fields.next().context("missing process system CPU")?.parse::<u64>()?;
    user.checked_add(system).context("process CPU overflow")
}
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
    pub service: Vec<crate::engine::execution::service::Report>,
    pub rebased_uploads: usize,
    pub capacity_flushes: usize,
    pub restarts: usize,
    pub maintenance_ns: u128,
    pub cpu_ticks: u64,
    pub maximum_runs: usize,
    pub maximum_memtable_bytes: usize,
    pub samples: Vec<serde_json::Value>,
    pub retirement: Vec<serde_json::Value>,
    pub final_merge_input_rows: Vec<usize>,
    pub final_maintenance: serde_json::Value,
    pub maintenance_quiescent: bool,
    pub installed_source_rows: u64,
    pub durable_roots: usize,
    pub durable_cohorts: usize,
}
pub(super) struct Lifecycle {
    config: Config,
    captured_ms: u64,
    pending: Option<Pending>,
    pub report: Report,
    store: Arc<crate::engine::tests::store::Store>,
    gc: crate::catalog::GcCache,
    deferred_merge_rows: Vec<usize>,
}
impl Drop for Lifecycle {
    fn drop(&mut self) {
        if let Some(task) = self.pending.take() {
            task.task.abort();
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
            deferred_merge_rows: Vec::new(),
        }
    }
    async fn install(&mut self, query: &mut program::relational::Query) -> Result<()> {
        if let Some(task) = self.pending.take() {
            let uploaded = task.task.await??;
            let checkpoint = uploaded.checkpoint();
            self.report.durable_roots =
                checkpoint.arrangements.iter().map(|member| member.trace.objects.len()).sum();
            self.report.durable_cohorts =
                checkpoint.arrangements.iter().map(|member| member.trace.cohorts.len()).sum();
            self.report.rebased_uploads += usize::from(query.time() > uploaded.checkpoint().time);
            let roots = uploaded
                .checkpoint()
                .arrangements
                .iter()
                .flat_map(|member| member.trace.objects.clone())
                .collect();
            let work = query.prepare_uploaded_flush(&uploaded).await?;
            query.commit_compaction(work)?;
            self.report.service.push(uploaded.service().clone());
            drop(uploaded);
            self.report.installed_source_rows = task.source_rows;
            let mut retired =
                crate::engine::tests::retire::retire(&self.store, roots, &mut self.gc).await?;
            retired["installed_checkpoints"] = serde_json::json!(self.report.retirement.len() + 1);
            retired["source_rows"] = serde_json::json!(self.report.installed_source_rows);
            // Observation time can follow the captured durable prefix.
            retired["logical_ms"] = serde_json::json!(self.report.logical_ms);
            self.report.retirement.push(retired);
        }
        Ok(())
    }
    async fn capture(&mut self, query: &mut program::relational::Query) -> Result<()> {
        ensure!(self.pending.is_none(), "replay upload already pending");
        let namespace = format!("pgderive/upload-v1/replay-{}", query.time());
        self.pending = Some(Pending {
            task: tokio::spawn(query.capture_flush(namespace).await?.upload()),
            source_rows: self.report.source_rows,
            logical_ms: self.report.logical_ms,
        });
        self.report.maintenance_quiescent = false;
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
        self.report.maintenance_quiescent = self.pending.is_none();
        Ok(())
    }
    fn work_snapshot(&self) -> Result<[u64; 5]> {
        Ok([
            u64::try_from(self.store.reads.load(Ordering::SeqCst))?,
            self.store.range_bytes.load(Ordering::SeqCst),
            u64::try_from(self.store.puts.load(Ordering::SeqCst))?,
            u64::try_from(self.store.put_bytes.load(Ordering::SeqCst))?,
            process_cpu_ticks()?,
        ])
    }
    pub(super) async fn drain(&mut self, query: &mut program::relational::Query) -> Result<()> {
        let before = self.work_snapshot()?;
        let started = Instant::now();
        self.flush(query).await?;
        let elapsed_ns = started.elapsed().as_nanos();
        let after = self.work_snapshot()?;
        self.report.final_merge_input_rows = self.take_merge_rows(query)?;
        let mut costs = serde_json::Map::new();
        for ((name, before), after) in ["gets", "read_bytes", "puts", "put_bytes", "cpu_ticks"]
            .into_iter()
            .zip(before)
            .zip(after)
        {
            costs.insert(
                name.into(),
                serde_json::json!(after.checked_sub(before).context("drain counter regressed")?),
            );
        }
        costs.insert("elapsed_ns".into(), serde_json::json!(elapsed_ns));
        costs.insert(
            "merge_input_rows".into(),
            serde_json::json!(self.report.final_merge_input_rows),
        );
        costs.insert("durable_roots".into(), serde_json::json!(self.report.durable_roots));
        costs.insert("durable_cohorts".into(), serde_json::json!(self.report.durable_cohorts));
        costs.insert(
            "installed_source_rows".into(),
            serde_json::json!(self.report.installed_source_rows),
        );
        costs.insert(
            "checkpoint_lag_rows".into(),
            serde_json::json!(self.report.source_rows - self.report.installed_source_rows),
        );
        self.report.final_maintenance = serde_json::Value::Object(costs);
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
    pub(super) fn collect_merge_rows(&mut self, query: &program::relational::Query) -> Result<()> {
        let rows = query.test_cache().take_merge_rows()?;
        ensure!(
            self.deferred_merge_rows.len() + rows.len() <= 4096,
            "replay merge event limit exceeded"
        );
        self.deferred_merge_rows.extend(rows);
        Ok(())
    }
    pub(super) fn take_merge_rows(
        &mut self,
        query: &program::relational::Query,
    ) -> Result<Vec<usize>> {
        self.collect_merge_rows(query)?;
        Ok(std::mem::take(&mut self.deferred_merge_rows))
    }
    pub(super) fn observe(
        &mut self,
        query: &program::relational::Query,
        prepare_ns: u128,
        cpu_started: u64,
    ) -> Result<()> {
        let elapsed = process_cpu_ticks()?.checked_sub(cpu_started).context("CPU regressed")?;
        self.report.cpu_ticks =
            self.report.cpu_ticks.checked_add(elapsed).context("replay CPU overflow")?;
        let cache = query.cache_stats()?;
        let merge_input_rows = self.take_merge_rows(query)?;
        self.report.samples.push(serde_json::json!({
            "logical_ms":self.report.logical_ms,"source_rows":self.report.source_rows,"prepare_ns":prepare_ns,
            "installed_checkpoints":self.report.retirement.len(),"upload_pending":self.pending.is_some(),
            "cpu_ticks":self.report.cpu_ticks,
            "merge_input_rows":merge_input_rows,
            "checkpoint_lag_rows":self.report.source_rows - self.report.installed_source_rows,
            "checkpoint_age_ms":self.pending.as_ref().map_or(0, |task| self.report.logical_ms-task.logical_ms),
            "checkpoint_target_source_rows":self.pending.as_ref().map(|task| task.source_rows),
            "installed_source_rows":self.report.installed_source_rows,
            "durable_roots":self.report.durable_roots,"durable_cohorts":self.report.durable_cohorts,
            "compressed_cache":query.test_cache().range_diagnostics()?,
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
