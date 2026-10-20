//! Fused bounded expansion and retained additive statistics without expanded state.
use super::{Expand, Statistics, TimedBatch};
use crate::engine::{Batch, execution::Limits, reader::BatchData, trace::TraceSnapshot};
use anyhow::Result;
/// Callers validate complete original input identities before indexing contributions.
#[derive(Clone)]
pub struct IndexedStatistics<K: BatchData, V: BatchData> {
    expand: Expand<K, V, K, V>,
    statistics: Statistics<K, V>,
}
impl<K: BatchData, V: BatchData> IndexedStatistics<K, V> {
    /// Bind contribution expansion and exact retained statistics.
    #[must_use]
    pub const fn new(expand: Expand<K, V, K, V>, statistics: Statistics<K, V>) -> Self {
        Self { expand, statistics }
    }
    /// Map the signed tick into index contributions and update retained summaries.
    ///
    /// # Errors
    /// Returns callback, resource, clock and retained-state failures.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        let mapped = self.expand.evaluate(input, limits)?;
        self.statistics.evaluate(&mapped, prior, limits).await
    }
}
