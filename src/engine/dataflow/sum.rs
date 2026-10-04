use super::Stream;
use crate::engine::{
    Batch, BatchBuilder,
    reader::{BatchData, BatchReader},
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
type Measure<K, V> = dyn Fn(&K, &V) -> Result<Option<i64>> + Send + Sync;

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
#[derive(Default)]
struct Total {
    rows: BigInt,
    non_null: BigInt,
    sum: BigInt,
}
impl Total {
    fn add(&mut self, weight: i64, value: Option<i64>) {
        self.rows += weight;
        if let Some(value) = value {
            self.non_null += weight;
            self.sum += BigInt::from(weight) * value;
        }
    }
    fn prior(&mut self, state: &SumState) {
        self.rows += state.rows;
        self.non_null += state.non_null;
        self.sum += state.sum;
    }
    fn finish(self) -> Result<SumState> {
        let state = SumState {
            rows: i64::try_from(self.rows).context("group row count overflow")?,
            non_null: i64::try_from(self.non_null).context("group non-NULL count overflow")?,
            sum: i64::try_from(self.sum).context("group sum overflow")?,
        };
        validate(&state)?;
        Ok(state)
    }
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
    pub state: Stream<Batch<K, SumState>>,
    /// SQL-visible SUM rows; None is SQL NULL, zero is a present numeric zero.
    pub output: Stream<Batch<K, Option<i64>>>,
}
/// Reusable grouped weighted sum over a valid nonnegative SQL input bag.
///
/// Input deltas may be negative; callers maintain valid full-tuple input bags.
/// Final grouped statistics are checked, but do not prove per-tuple validity.
pub struct GroupSum<K: BatchData, V: BatchData> {
    measure: Arc<Measure<K, V>>,
}
impl<K: BatchData, V: BatchData> GroupSum<K, V> {
    /// Bind a pure deterministic numeric projection; None contributes SQL NULL.
    pub fn new(measure: impl Fn(&K, &V) -> Result<Option<i64>> + Send + Sync + 'static) -> Self {
        Self { measure: Arc::new(measure) }
    }
    /// Sum exact weight×value contributions and prior statistics before narrowing.
    ///
    /// # Errors
    /// Returns tick/read/callback errors, invalid final counts, or final overflow.
    pub async fn evaluate(
        &self,
        input: &Stream<Batch<K, V>>,
        prior: &TraceSnapshot<K, SumState>,
    ) -> Result<SumDelta<K>> {
        ensure!(prior.time().checked_add(1) == Some(input.time), "out-of-order aggregate tick");
        let mut totals = BTreeMap::<K, Total>::new();
        for ((key, value), weight) in input.batch.iter() {
            totals.entry(key.clone()).or_default().add(*weight, (self.measure)(key, value)?);
        }
        let mut cursor = prior.cursor().await?;
        let mut state = BatchBuilder::default();
        let mut output = BatchBuilder::default();
        for (key, mut total) in totals {
            cursor.seek_key(&key).await?;
            let before = if let Some((group, value, weight)) = cursor.current()
                && group == &key
            {
                ensure!(weight == 1 && value.rows > 0, "invalid aggregate arrangement");
                validate(value)?;
                let value = value.clone();
                cursor.advance().await?;
                ensure!(
                    cursor.current().is_none_or(|(group, _, _)| group != &key),
                    "multiple aggregate states for group"
                );
                Some(value)
            } else {
                None
            };
            if let Some(before) = &before {
                total.prior(before);
            }
            let next = total.finish()?;
            let after = (next.rows != 0).then_some(next);
            if before == after {
                continue;
            }
            if let Some(before) = before {
                state.push(key.clone(), before.clone(), -1);
                output.push(key.clone(), visible(&before), -1);
            }
            if let Some(after) = after {
                state.push(key.clone(), after.clone(), 1);
                output.push(key, visible(&after), 1);
            }
        }
        Ok(SumDelta {
            state: Stream { time: input.time, batch: state.finish()? },
            output: Stream { time: input.time, batch: output.finish()? },
        })
    }
}
fn visible(state: &SumState) -> Option<i64> {
    (state.non_null != 0).then_some(state.sum)
}
