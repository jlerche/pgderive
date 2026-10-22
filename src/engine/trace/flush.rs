//! Flush bounded memory deltas without revisiting retained object data.
use super::{Run, TraceSnapshot};
use crate::engine::{BatchBuilder, dataflow::Arrangement, reader::BatchData};
use anyhow::{Context, Result, ensure};

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(crate) async fn flush_memory(&self, writer: &Arrangement<K, V>) -> Result<Self> {
        ensure!(self.run_count() <= 128, "flush input run limit exceeded");
        if !self.has_memory() {
            return Ok(self.clone());
        }
        // Bound merge fan-in independently of query topology and data values.
        // A full compaction remains necessary periodically to consolidate runs.
        let objects = self.runs.iter().filter(|run| matches!(run, Run::Object(_))).count();
        if objects >= 8 {
            return writer.compact(self).await;
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
        if delta.iter().next().is_some() {
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
