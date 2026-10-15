use super::TimedBatch;
use crate::engine::{Batch, execution::Limits, reader::BatchData, trace::TraceSnapshot};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

/// Object-persisted sufficient statistics for SQL bag GROUP BY SUM semantics.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SumState {
    /// Total multiplicity, including NULL measures; distinguishes zero from absence.
    pub rows: i64,
    /// Multiplicity of non-NULL measures; zero yields SQL NULL.
    pub non_null: i64,
    /// Exact weighted sum narrowed at the complete transaction boundary.
    pub sum: i64,
}
fn validate(state: &SumState) -> Result<()> {
    ensure!(
        state.rows >= 0 && state.non_null >= 0 && state.non_null <= state.rows,
        "invalid SQL bag group multiplicity"
    );
    ensure!(state.non_null != 0 || state.sum == 0, "sum without non-NULL contributions");
    Ok(())
}
/// Complete transaction changes to internal statistics and visible SUM rows.
pub struct SumDelta<K: BatchData> {
    /// Unit-weight retract/replace delta for the object-backed aggregate trace.
    pub state: TimedBatch<Batch<K, SumState>>,
    /// SQL-visible SUM rows; None is SQL NULL, zero is a present numeric zero.
    pub output: TimedBatch<Batch<K, Option<i64>>>,
}
/// Reusable grouped weighted sum over a valid nonnegative SQL input bag.
///
/// Input deltas may be negative; callers maintain valid full-tuple input bags.
/// Final grouped statistics are checked, but do not prove per-tuple validity.
pub struct GroupSum<K: BatchData, V: BatchData> {
    statistics: super::Statistics<K, V, SumState>,
}
impl<K: BatchData, V: BatchData> GroupSum<K, V> {
    /// Bind a pure deterministic numeric projection; None contributes SQL NULL.
    pub fn new(measure: impl Fn(&K, &V) -> Result<Option<i64>> + Send + Sync + 'static) -> Self {
        Self {
            statistics: super::Statistics::with_measure(
                3,
                move |key, value| {
                    let value = measure(key, value)?;
                    Ok(vec![
                        BigInt::from(1),
                        BigInt::from(u8::from(value.is_some())),
                        BigInt::from(value.unwrap_or_default()),
                    ])
                },
                |state: &SumState| {
                    validate(state)?;
                    ensure!(state.rows > 0, "invalid aggregate arrangement");
                    Ok(vec![state.rows.into(), state.non_null.into(), state.sum.into()])
                },
                |_, values| {
                    let state = SumState {
                        rows: i64::try_from(values[0].clone())
                            .context("group row count overflow")?,
                        non_null: i64::try_from(values[1].clone())
                            .context("group non-NULL count overflow")?,
                        sum: i64::try_from(values[2].clone()).context("group sum overflow")?,
                    };
                    validate(&state)?;
                    Ok((state.rows != 0).then_some(state))
                },
            ),
        }
    }
    /// Sum exact weight×value contributions and prior statistics before narrowing.
    ///
    /// # Errors
    /// Returns tick/read/callback errors, invalid final counts, or final overflow.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, SumState>,
    ) -> Result<SumDelta<K>> {
        self.evaluate_with_limits(input, prior, Limits::default()).await
    }
    /// Evaluate sufficient statistics with exact spillable intermediate arithmetic.
    ///
    /// # Errors
    /// Returns clock, callback, resource, scratch, read, or final arithmetic errors.
    pub async fn evaluate_with_limits<B: Borrow<Batch<K, V>> + Sync>(
        &self,
        input: &TimedBatch<B>,
        prior: &TraceSnapshot<K, SumState>,
        limits: Limits,
    ) -> Result<SumDelta<K>> {
        let state = self.statistics.evaluate(input, prior, limits).await?;
        let output = Batch::from_updates(
            state
                .batch
                .iter()
                .map(|((key, value), weight)| ((key.clone(), visible(value)), *weight)),
        )?;
        Ok(SumDelta { state, output: TimedBatch { time: input.time, batch: output } })
    }
}
fn visible(state: &SumState) -> Option<i64> {
    (state.non_null != 0).then_some(state.sum)
}
