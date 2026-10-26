//! Flush bounded memory deltas without revisiting retained object data.
use super::{Run, TraceSnapshot};
use crate::engine::{
    BatchBuilder,
    dataflow::Arrangement,
    reader::{BatchData, BatchReader, BlockCache},
};
use anyhow::{Context, Result, ensure};

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(crate) async fn compact_with(&self, writer: &Arrangement<K, V>) -> Result<Self> {
        ensure!(self.run_count() <= super::MAX_RUNS, "compaction input run limit exceeded");
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let cache = writer.cache();
        let scan_cache = BlockCache::scan(cache.clone());
        let mut input = self.clone();
        input.share_cache(&scan_cache);
        let mut cursor = input.cursor().await?;
        let output_cache = BlockCache::output(cache.clone());
        let output_writer = writer.clone().with_cache(output_cache.clone());
        let runs = output_writer.upload_cursor(&mut cursor).await?;
        // This private construction streams exactly the validated merged input.
        // Untrusted replacements still require replace_runs' equivalence check.
        let mut next = Self::from_runs(runs, generation, self.time)?;
        if !std::sync::Arc::ptr_eq(&cache, &output_cache) {
            output_cache.promote()?;
        }
        next.share_cache(&cache);
        let rows = self.runs.iter().try_fold(0_usize, |total, run| {
            let rows = match run {
                Run::Object(object) => object.row_count(),
                Run::Memory(batch) => batch.iter().count(),
            };
            total.checked_add(rows).context("compaction row count overflow")
        })?;
        cache.record_merge(rows)?;
        next.logical_cache.clone_from(&self.logical_cache);
        Ok(next)
    }
    pub(crate) fn share_cache(&mut self, cache: &std::sync::Arc<BlockCache>) {
        if !cache.admits_points() {
            self.logical_cache = None;
        } else if self.logical_cache.is_none() {
            self.logical_cache = super::cache::State::new(cache);
        }
        for run in &mut self.runs {
            if let Run::Object(object) = run {
                object.share_cache(cache.clone());
            }
        }
    }

    pub(crate) async fn flush_memory(&self, writer: &Arrangement<K, V>) -> Result<Self> {
        ensure!(self.run_count() <= super::MAX_RUNS, "flush input run limit exceeded");
        if !self.has_memory() {
            return Ok(self.clone());
        }
        let mut memory = BatchBuilder::default();
        for run in &self.runs {
            if let Run::Memory(batch) = run {
                memory.extend(batch);
            }
        }
        let mut runs = self
            .runs
            .iter()
            .filter(|run| matches!(run, Run::Object(_)))
            .cloned()
            .collect::<Vec<_>>();
        let mut cohorts = self.cohorts.clone();
        let mut remaining = crate::engine::reader::WriteLimits::default().output_roots;
        // A memory subset can overflow even when cancellation with durable state
        // makes the whole trace valid. Upload its canonical runs separately;
        // checkpointing must never scan old objects to resolve that cancellation.
        match memory.finish() {
            Ok(delta) => {
                append_upload(writer, &delta, &mut runs, &mut cohorts, &mut remaining).await?;
            }
            Err(_) => {
                for run in &self.runs {
                    if let Run::Memory(batch) = run {
                        append_upload(writer, batch, &mut runs, &mut cohorts, &mut remaining)
                            .await?;
                    }
                }
            }
        }
        ensure!(runs.len() <= super::MAX_RUNS, "checkpoint root limit requires consolidation");
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let mut next = Self::from_runs(runs, generation, self.time)?;
        next.cohorts = cohorts;
        next.logical_cache.clone_from(&self.logical_cache);
        Ok(next)
    }
}
async fn append_upload<K: BatchData, V: BatchData>(
    writer: &Arrangement<K, V>,
    batch: &crate::engine::Batch<K, V>,
    runs: &mut Vec<Run<K, V>>,
    cohorts: &mut Vec<usize>,
    remaining: &mut usize,
) -> Result<()> {
    if batch.iter().next().is_some() {
        let uploaded = writer.upload_flush(batch).await?;
        ensure!(
            uploaded.len() <= *remaining,
            crate::engine::execution::CapacityExceeded(
                "checkpoint memory output exceeds root budget"
            )
        );
        *remaining -= uploaded.len();
        if !uploaded.is_empty() {
            cohorts.push(uploaded.len());
            runs.extend(uploaded);
        }
    }
    Ok(())
}
