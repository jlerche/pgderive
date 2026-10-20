//! Direct binary differentiation against synchronized immutable prior arrangements.
use super::{PartitionWork, TimedBatch};
use crate::engine::{Batch, execution::Limits, reader::BatchData, trace::TraceSnapshot};
use anyhow::{Result, ensure};
use std::{future::Future, pin::Pin, sync::Arc};
type Inputs<'a, K, V> = TimedBatch<(&'a Batch<K, V>, &'a Batch<K, V>)>;
type Priors<'a, K, V> = (&'a TraceSnapshot<K, V>, &'a TraceSnapshot<K, V>);
/// Two complete same-tick deltas and their old committed arrangements.
pub struct BinaryInput<'a, K: BatchData, V: BatchData> {
    /// Left data changes.
    pub left: &'a Batch<K, V>,
    /// Right summary changes.
    pub right: &'a Batch<K, V>,
    /// Old left arrangement.
    pub prior_left: &'a TraceSnapshot<K, V>,
    /// Old right arrangement.
    pub prior_right: &'a TraceSnapshot<K, V>,
    /// Resource limits.
    pub limits: Limits,
}
/// Asynchronous direct delta result.
pub type BinaryFuture<'a, K, V> = Pin<Box<dyn Future<Output = Result<Batch<K, V>>> + Send + 'a>>;
type Transform<K, V> = dyn for<'a> Fn(BinaryInput<'a, K, V>, &'a mut PartitionWork) -> BinaryFuture<'a, K, V>
    + Send
    + Sync;
/// Caller-defined direct binary operator; callbacks must emit Q(new)-Q(old).
#[derive(Clone)]
pub struct Binary<K: BatchData, V: BatchData> {
    transform: Arc<Transform<K, V>>,
}
impl<K: BatchData, V: BatchData> Binary<K, V> {
    /// Bind a deterministic direct evaluator.
    #[must_use]
    pub fn new(
        transform: impl for<'a> Fn(
            BinaryInput<'a, K, V>,
            &'a mut PartitionWork,
        ) -> BinaryFuture<'a, K, V>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { transform: Arc::new(transform) }
    }
    /// Validate old boundaries and changed full identities before direct evaluation.
    ///
    /// # Errors
    /// Returns boundary, membership, resource, callback or storage failures.
    pub async fn evaluate(
        &self,
        input: &Inputs<'_, K, V>,
        priors: Priors<'_, K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(
            priors.0.time() == priors.1.time()
                && priors.0.time().checked_add(1) == Some(input.time),
            "inconsistent binary boundary"
        );
        limits.check_batch(input.batch.0)?;
        limits.check_batch(input.batch.1)?;
        priors.0.validate_bag_delta(input.batch.0).await?;
        priors.1.validate_bag_delta(input.batch.1).await?;
        let mut work = PartitionWork::new(limits.contributions);
        let batch = (self.transform)(
            BinaryInput {
                left: input.batch.0,
                right: input.batch.1,
                prior_left: priors.0,
                prior_right: priors.1,
                limits,
            },
            &mut work,
        )
        .await?;
        limits.check_batch(&batch)?;
        Ok(TimedBatch { time: input.time, batch })
    }
}
