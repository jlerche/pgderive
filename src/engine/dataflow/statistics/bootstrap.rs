//! Bounded raw additive state; SQL encoding occurs only at snapshot completion.
use super::{
    Additive,
    algebra::{Algebra, Budget},
};
use crate::engine::{
    execution::{Consolidator, Limits, Sealed},
    reader::BatchData,
};
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc};
pub trait SnapshotFold<K: BatchData, V: BatchData, S: BatchData>: Send {
    fn observe(&mut self, key: &K, row: &V, weight: i64) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<Sealed<K, S>>;
}
pub(super) struct Grouped<K: BatchData, V: BatchData, S: BatchData, A: Additive> {
    algebra: Arc<Algebra<K, V, S, A>>,
    groups: BTreeMap<K, Resident<A>>,
    bytes: usize,
    contributions: usize,
    spilled: bool,
    partials: Consolidator<(K, String)>,
    limits: Limits,
    partial_bytes: usize,
}
struct Resident<A> {
    values: Vec<A>,
    charge: usize,
}
impl<K: BatchData, V: BatchData, S: BatchData, A: Additive> Grouped<K, V, S, A> {
    pub(super) fn new(algebra: Arc<Algebra<K, V, S, A>>, limits: Limits) -> Result<Self> {
        ensure!((1..=129).contains(&algebra.width), "statistics width unsupported");
        let partial_bytes = (limits.resident_bytes / 4).min(1024 * 1024).max(limits.record_bytes);
        ensure!(
            partial_bytes < limits.resident_bytes,
            crate::engine::execution::CapacityExceeded(
                "snapshot statistics workspace cannot hold a record and spill buffer"
            )
        );
        let partials = Consolidator::new(Limits {
            resident_bytes: partial_bytes,
            output_entries: limits.contributions,
            output_bytes: limits.scratch_bytes,
            ..limits
        })?;
        Ok(Self {
            algebra,
            groups: BTreeMap::new(),
            bytes: 0,
            contributions: 0,
            spilled: false,
            partials,
            limits,
            partial_bytes,
        })
    }
    fn spill(&mut self) -> Result<()> {
        for (key, group) in std::mem::take(&mut self.groups) {
            self.partials.add((key, serde_json::to_string(&group.values)?), 1)?;
        }
        self.bytes = 0;
        self.spilled = true;
        Ok(())
    }
}
impl<K: BatchData, V: BatchData, S: BatchData, A: Additive> SnapshotFold<K, V, S>
    for Grouped<K, V, S, A>
{
    fn observe(&mut self, key: &K, row: &V, weight: i64) -> Result<()> {
        ensure!(
            self.contributions
                .checked_add(self.algebra.width)
                .is_some_and(|n| n <= self.limits.contributions),
            crate::engine::execution::CapacityExceeded(
                "snapshot statistics contribution limit exceeded"
            )
        );
        self.contributions += self.algebra.width;
        let measured = (self.algebra.measure)(key, row)?;
        ensure!(measured.len() == self.algebra.width, "statistics measure width mismatch");
        let values = self.groups.entry(key.clone()).or_insert_with(|| Resident {
            values: vec![A::default(); self.algebra.width],
            charge: 0,
        });
        for (total, value) in values.values.iter_mut().zip(measured) {
            total.add_scaled(&value, weight)?;
        }
        let charge = crate::engine::execution::record_size(
            &(key, &values.values),
            self.limits.record_bytes,
        )?;
        ensure!(
            charge <= self.limits.resident_bytes - self.partial_bytes,
            crate::engine::execution::CapacityExceeded(
                "snapshot statistic exceeds resident budget"
            )
        );
        self.bytes = self.bytes - values.charge + charge;
        values.charge = charge;
        if self.bytes >= self.limits.resident_bytes - self.partial_bytes
            || self.groups.len() >= self.limits.resident_entries
        {
            self.spill()?;
        }
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<Sealed<K, S>> {
        let mut output =
            Consolidator::new(Limits { resident_bytes: self.partial_bytes, ..self.limits })?;
        let mut budget = Budget::new(self.limits);
        if self.spilled {
            self.spill()?;
            let mut current: Option<(K, Vec<A>)> = None;
            self.partials.finish(|(key, encoded), weight| {
                if current.as_ref().is_some_and(|(prior, _)| *prior != key)
                    && let Some((prior, values)) = current.take()
                {
                    emit(&self.algebra, &mut budget, &mut output, prior, &values)?;
                }
                let (_, values) = current
                    .get_or_insert_with(|| (key.clone(), vec![A::default(); self.algebra.width]));
                let partial: Vec<A> = serde_json::from_str(&encoded)?;
                ensure!(
                    partial.len() == values.len(),
                    "snapshot partial statistics width mismatch"
                );
                let weight = i64::try_from(weight)?;
                for (total, value) in values.iter_mut().zip(partial) {
                    total.add_scaled(&value, weight)?;
                }
                Ok(())
            })?;
            if let Some((key, values)) = current {
                emit(&self.algebra, &mut budget, &mut output, key, &values)?;
            }
        } else {
            self.partials
                .finish(|_, _| anyhow::bail!("unexpected unspilled statistics partial"))?;
            for (key, group) in self.groups {
                emit(&self.algebra, &mut budget, &mut output, key, &group.values)?;
            }
        }
        output.seal()
    }
}

fn emit<K: BatchData, V: BatchData, S: BatchData, A: Additive>(
    algebra: &Algebra<K, V, S, A>,
    budget: &mut Budget,
    output: &mut Consolidator<(K, S)>,
    key: K,
    values: &[A],
) -> Result<()> {
    budget.finish(&key, values)?;
    if let Some(row) = (algebra.encode)(&key, values)? {
        output.add((key, row), 1)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        dataflow::Statistics,
        execution::Limits,
        reader::{BatchCursor, BatchReader},
    };
    use anyhow::Result;
    use num_bigint::BigInt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[tokio::test]
    async fn raw_partial_spills_merge_before_any_finalization() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let encoded = calls.clone();
        let operator = Statistics::with_measure(
            2,
            |_: &i64, v: &i64| Ok(vec![BigInt::from(1), BigInt::from(*v)]),
            |_: &String| Ok(vec![BigInt::from(0); 2]),
            move |_: &i64, v: &[BigInt]| {
                encoded.fetch_add(1, Ordering::SeqCst);
                Ok(Some(format!("{}:{}", v[0], v[1])))
            },
        );
        let limits = Limits {
            resident_bytes: 1024,
            resident_entries: 2,
            record_bytes: 512,
            spill_runs: 2,
            output_entries: 1000,
            ..Limits::default()
        };
        let mut fold = operator.snapshot_fold(limits)?;
        for n in 0..1000 {
            fold.observe(&(n % 100), &1, 1)?;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let output = fold.finish()?;
        let mut cursor = output.cursor().await?;
        for key in 0..100 {
            assert_eq!(cursor.current(), Some((&key, &"10:10".to_owned(), 1)));
            cursor.advance().await?;
        }
        assert!(cursor.current().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 100);
        Ok(())
    }
}
