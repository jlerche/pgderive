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
        Ok(next)
    }
    pub(crate) fn share_cache(&mut self, cache: &std::sync::Arc<BlockCache>) {
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
        self.flush_tier(writer, delta).await
    }
}
