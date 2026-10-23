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
        let scan_writer = writer.clone().with_cache(scan_cache);
        let runs = scan_writer.upload_cursor(&mut cursor).await?;
        // This private construction streams exactly the validated merged input.
        // Untrusted replacements still require replace_runs' equivalence check.
        let mut next = Self::from_runs(runs, generation, self.time)?;
        next.share_cache(&cache);
        Ok(next)
    }
    fn share_cache(&mut self, cache: &std::sync::Arc<BlockCache>) {
        for run in &mut self.runs {
            if let Run::Object(object) = run {
                object.share_cache(cache.clone());
            }
        }
    }

    pub(crate) async fn flush_memory(&self, writer: &Arrangement<K, V>) -> Result<Self> {
        ensure!(self.run_count() <= super::MAX_RUNS, "flush input run limit exceeded");
        if !self.has_memory() {
            return if self.run_count() > 128 {
                writer.compact(self).await
            } else {
                Ok(self.clone())
            };
        }
        let mut memory = BatchBuilder::default();
        for run in &self.runs {
            if let Run::Memory(batch) = run {
                memory.extend(batch);
            }
        }
        let Ok(delta) = memory.finish() else {
            // A subset may overflow i64 even though cancellation with retained
            // objects makes the complete trace representable. Narrow only the
            // complete trace in this case.
            return writer.compact(self).await;
        };
        let mut runs = self
            .runs
            .iter()
            .filter_map(|run| match run {
                Run::Object(_) => Some(run.clone()),
                Run::Memory(_) => None,
            })
            .collect::<Vec<_>>();
        let entries = delta.iter().count();
        if entries > 0 {
            // Dense deltas otherwise add nearly a full run's worth of lookup
            // work. Resident fences supply physical row counts without I/O;
            // duplicate references count separately, just as they do in merges.
            let rows = runs.iter().try_fold(0_usize, |total, run| {
                let rows = match run {
                    Run::Object(object) => object.row_count(),
                    Run::Memory(_) => 0,
                };
                total.checked_add(rows).context("flush row count overflow")
            })?;
            if runs.len() >= 8 || (rows > 0 && entries > rows / 2) {
                return writer.compact(self).await;
            }
            runs.extend(writer.upload_flush(&delta).await?);
        }
        if runs.len() > 128 {
            return writer.compact(self).await;
        }
        // The writer uploads this exact consolidated memory delta. Retained
        // objects are unchanged, so equivalence follows from construction and
        // requires no redundant full-state read-back.
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        Self::from_runs(runs, generation, self.time)
    }
}
