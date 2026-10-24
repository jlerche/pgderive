//! A successful full-tuple bag check tied to one immutable trace and batch.
use super::TraceSnapshot;
use crate::engine::{Batch, reader::BatchData};
use anyhow::{Result, ensure};
use std::sync::Arc;

pub struct ValidatedBag<'a, K: BatchData, V: BatchData> {
    trace: &'a TraceSnapshot<K, V>,
    batch: Arc<Batch<K, V>>,
}
impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    /// Only the private committed-source execution policy may issue this proof.
    /// Complete source keys prove 0/1 retained multiplicity independently of
    /// navigation prefixes; the batch and immutable boundary remain exact.
    pub(crate) fn certify_source_batch(
        &self,
        batch: Arc<Batch<K, V>>,
    ) -> Result<ValidatedBag<'_, K, V>> {
        ensure!(
            batch.iter().all(|(_, weight)| matches!(weight, -1 | 1)),
            "source-key proof requires unit tuple deltas"
        );
        Ok(ValidatedBag { trace: self, batch })
    }

    pub(crate) async fn validate_bag_batch(
        &self,
        batch: Arc<Batch<K, V>>,
    ) -> Result<ValidatedBag<'_, K, V>> {
        self.validate_bag_delta(&batch).await?;
        Ok(ValidatedBag { trace: self, batch })
    }
}
impl<K: BatchData, V: BatchData> ValidatedBag<'_, K, V> {
    pub(crate) fn matches(&self, trace: &TraceSnapshot<K, V>, batch: &Batch<K, V>) -> bool {
        std::ptr::eq(self.trace, trace) && std::ptr::eq(self.batch.as_ref(), batch)
    }
}
