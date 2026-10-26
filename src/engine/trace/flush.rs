//! Flush bounded memory deltas without revisiting retained object data.
use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    batch_size::BatchSize,
    dataflow::Arrangement,
    reader::{BatchData, BlockCache},
    zset::CanonicalBuilder,
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
        let output_cache = BlockCache::output(cache.clone());
        let output_writer = writer.clone().with_cache(output_cache.clone());
        let runs = output_writer.compact_runs(&input.runs).await?;
        // This private construction streams exactly the validated merged input.
        // Untrusted replacements still require replace_runs' equivalence check.
        let mut next = Self::from_runs(runs, generation, self.time)?;
        if !std::sync::Arc::ptr_eq(&cache, &output_cache) {
            output_cache.promote()?;
        }
        next.share_cache(&cache);
        let rows = self.runs.iter().try_fold(0_usize, |total, run| {
            let rows = run.row_count();
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
            return if self.run_count() > 128 {
                writer.compact(self).await
            } else {
                Ok(self.clone())
            };
        }
        let delta = match self.memory_delta().await {
            Ok(delta) => delta,
            Err(error) if error.is::<super::merged::CoefficientOverflow>() => {
                // A memory subset may overflow even when retained object weights
                // make the complete trace representable. Narrow only that trace.
                return writer.compact(self).await;
            }
            Err(error) => return Err(error),
        };
        self.flush_tier(writer, delta).await
    }
    async fn memory_delta(&self) -> Result<Batch<K, V>> {
        let runs = self
            .runs
            .iter()
            .filter(|run| matches!(run, Run::Memory(_)))
            .cloned()
            .collect::<Vec<_>>();
        if let [Run::Memory(batch)] = runs.as_slice() {
            return Ok(batch.as_ref().clone());
        }
        let mut cursor = super::merged::MergedCursor::open(&runs).await?;
        let mut rows = CanonicalBuilder::new();
        let mut size = BatchSize { entries: 0, bytes: 0, largest: 0 };
        while let Some((key, value, weight)) = cursor.current() {
            crate::engine::reader::work::spend(0).await;
            let row = ((key.clone(), value.clone()), weight);
            let bytes = crate::engine::execution::record_size(&row, usize::MAX)?;
            crate::engine::reader::work::spend(bytes).await;
            size.entries = size.entries.checked_add(1).context("memory row count overflow")?;
            size.bytes = size
                .bytes
                .checked_add(u64::try_from(bytes)?)
                .context("memory byte count overflow")?;
            size.largest = size.largest.max(bytes);
            rows.push(row)?;
            cursor.advance().await?;
        }
        let batch = rows.finish();
        let _ = batch.sizing.set(size);
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::BatchBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn memory_delta_shares_single_run_and_sizes_exact_signed_merge() -> Result<()> {
        let first = Arc::new(Batch::from_updates([((1, 1), i64::MIN), ((1, 2), 2), ((2, 1), -3)])?);
        let single = TraceSnapshot::from_runs(vec![Run::Memory(first.clone())], 0, 0)?;
        let shared = single.memory_delta().await?;
        assert!(Arc::ptr_eq(&first.shared_entries(), &shared.shared_entries()));
        let second =
            Arc::new(Batch::from_updates([((1, 1), i64::MAX), ((1, 2), -2), ((1, 3), 4)])?);
        let mut oracle = BatchBuilder::default();
        oracle.extend(&first);
        oracle.extend(&second);
        let trace = TraceSnapshot::from_runs(vec![Run::Memory(first), Run::Memory(second)], 0, 0)?;
        let merged = trace.memory_delta().await?;
        let expected = oracle.finish()?;
        assert_eq!(merged, expected);
        let size = merged.sizing.get().context("missing collected batch size")?;
        let expected_size = expected.encoded_size(usize::MAX, u64::MAX)?;
        assert_eq!(size.entries, expected_size.entries);
        assert_eq!(size.bytes, expected_size.bytes);
        assert_eq!(size.largest, expected_size.largest);
        assert_eq!(merged.max_weight(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn memory_delta_preserves_final_coefficient_overflow_boundary() -> Result<()> {
        let runs = [i64::MAX, 1]
            .map(|weight| Ok(Run::Memory(Arc::new(Batch::from_updates([((1, 1), weight)])?))))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let trace = TraceSnapshot::from_runs(runs, 0, 0)?;
        let error = trace.memory_delta().await.err().context("expected coefficient overflow")?;
        assert!(error.is::<super::super::merged::CoefficientOverflow>());
        Ok(())
    }
    #[tokio::test(flavor = "current_thread")]
    async fn memory_merge_preparation_yields_before_the_first_object_put() -> Result<()> {
        use crate::engine::tests::store::Store;
        use std::{
            future::{Future as _, poll_fn},
            sync::atomic::Ordering,
            task::Poll,
        };
        let store = Arc::new(Store::unpaced());
        let first = Arc::new(Batch::from_updates((0..4096_i64).map(|key| ((key * 2, key), 1)))?);
        let second =
            Arc::new(Batch::from_updates((0..4096_i64).map(|key| ((key * 2 + 1, key), -1)))?);
        let mut oracle = BatchBuilder::default();
        oracle.extend(&first);
        oracle.extend(&second);
        let expected = oracle.finish()?;
        let trace = TraceSnapshot::from_runs(vec![Run::Memory(first), Run::Memory(second)], 2, 2)?;
        let writer =
            Arrangement::<i64, i64>::new(store.clone(), "cooperative-prepare".into(), 4096)?;
        let mut upload = Box::pin(writer.flush(&trace));
        poll_fn(|cx| match upload.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(Ok(())),
            Poll::Ready(_) => {
                Poll::Ready(Err(anyhow::anyhow!("memory preparation failed to yield")))
            }
        })
        .await?;
        assert_eq!(store.puts.load(Ordering::SeqCst), 0);
        assert_eq!(trace.run_count(), 2);
        assert_eq!(trace.time(), 2);
        let flushed = upload.await?;
        assert_eq!(flushed.materialize().await?, expected);
        assert_eq!(trace.materialize().await?, expected);
        Ok(())
    }
}
