//! Retained ordered selection callbacks for executable circuit nodes.
use super::{OrderedFirst, OrderedTopK, TimedBatch};
use crate::engine::{Batch, execution::Limits, reader::BatchData, trace::TraceSnapshot};
use anyhow::Result;
/// Ordered callbacks share retained input/output staging and synchronized clocks.
/// Their selectors distinguish first full identity from K row occurrences.
pub enum OrderedSelection<K: BatchData, V: BatchData> {
    /// First positive full identity, projected with unit membership.
    First(OrderedFirst<K, V>),
    /// First K occurrences, including multiplicity clipping at the cutoff.
    TopK(OrderedTopK<K, V>),
}
impl<K: BatchData, V: BatchData> OrderedSelection<K, V> {
    /// Evaluate against the same immutable prior input and output boundary.
    ///
    /// # Errors
    /// Returns clock, membership, storage, callback or resource errors.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        output: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        match self {
            Self::First(operator) => operator.evaluate(input, prior, output, limits).await,
            Self::TopK(operator) => operator.evaluate(input, prior, output, limits).await,
        }
    }
}
