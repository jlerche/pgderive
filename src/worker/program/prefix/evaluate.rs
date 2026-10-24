use super::{Key, codec, order, rows::Rows, split};
use crate::{
    compiler::partition::{Partition, statistics},
    engine::{
        Batch,
        dataflow::{BinaryInput, PartitionWork},
        execution::Consolidator,
        trace::TraceSnapshot,
    },
    transaction::Row,
    worker::program::aggregate::Linear,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::collections::BTreeMap;
type Replacements = BTreeMap<Key, Option<Row>>;
fn replacements(delta: &Batch<Key, Row>, work: &mut PartitionWork) -> Result<Replacements> {
    let mut result = Replacements::new();
    for ((key, row), weight) in delta.iter() {
        work.charge(1)?;
        ensure!(matches!(weight, -1 | 1), "invalid prefix summary delta multiplicity");
        let entry = result.entry(key.clone()).or_default();
        if *weight > 0 {
            ensure!(entry.is_none(), "multiple new prefix summaries");
            *entry = Some(row.clone());
        }
    }
    Ok(result)
}
async fn read(prior: &TraceSnapshot<Key, Row>, key: &Key) -> Result<Option<Row>> {
    let mut cursor = prior.key_cursor(key).await?;
    let Some((_, row, weight)) = cursor.current() else { return Ok(None) };
    ensure!(weight == 1, "invalid prefix summary state multiplicity");
    let row = row.clone();
    cursor.advance().await?;
    ensure!(cursor.current().is_none(), "multiple prefix summary states");
    Ok(Some(row))
}
struct Summaries<'a> {
    native: &'a crate::worker::program::native::Integers,
    prior: &'a TraceSnapshot<Key, Row>,
    changed: Replacements,
}
impl Summaries<'_> {
    async fn fields(
        &self,
        spec: &Partition,
        key: &Key,
        new: bool,
        work: &mut PartitionWork,
    ) -> Result<Row> {
        let (base, value) = split(key)?;
        let mut total = vec![BigInt::from(0); codec::width(spec)];
        for prefix in codec::cover(value) {
            work.charge(1)?;
            let key = codec::node(&base, prefix);
            let row = if new && let Some(changed) = self.changed.get(&key) {
                changed.clone()
            } else {
                read(self.prior, &key).await?
            };
            if let Some(row) = row {
                let values =
                    self.native.decode(&row, |row| codec::decode(row, codec::width(spec)))?;
                for (total, value) in total.iter_mut().zip(values.iter()) {
                    *total += value;
                }
            }
        }
        codec::validate(&total)?;
        ensure!(total[0] > 0.into(), "prefix output without input rows");
        let mut fields = Row::new();
        for (i, aggregate) in spec.aggregates.iter().enumerate() {
            work.charge(1)?;
            let state = Linear {
                count: total[statistics::count(i)].clone(),
                sum: total[statistics::sum(i)].clone(),
            };
            fields.insert(aggregate.field.clone(), state.result(aggregate)?);
        }
        Ok(fields)
    }
}
struct Evaluation<'a> {
    spec: &'a Partition,
    input: BinaryInput<'a, Key, Row>,
    summaries: Summaries<'a>,
}
impl Evaluation<'_> {
    async fn emit(
        &self,
        change: &Changes,
        new: bool,
        output: &mut Consolidator<(Key, Row)>,
        work: &mut PartitionWork,
    ) -> Result<()> {
        let (lower, upper) = change.bounds();
        let delta = if new { change.entries.as_slice() } else { &[] };
        let mut rows = Rows::open(self.input.prior_left, delta, (&lower, &upper)).await?;
        while let Some(((key, mut row), weight)) = rows.next(work).await? {
            let (base, value) = split(&key)?;
            ensure!(value == order(self.spec, &row)?, "prefix retained row order mismatch");
            for (field, value) in self.summaries.fields(self.spec, &key, new, work).await? {
                ensure!(row.insert(field, value).is_none(), "prefix aggregate field collision");
            }
            output.add((base, row), BigInt::from(weight) * if new { 1 } else { -1 })?;
        }
        Ok(())
    }
}
pub(super) async fn evaluate<'a>(
    spec: &'a Partition,
    input: BinaryInput<'a, Key, Row>,
    work: &mut PartitionWork,
    native: &'a crate::worker::program::native::Integers,
) -> Result<Batch<Key, Row>> {
    let starts = changes(input.left, work)?;
    let summaries =
        Summaries { native, prior: input.prior_right, changed: replacements(input.right, work)? };
    let mut output = Consolidator::new(input.limits)?;
    let evaluator = Evaluation { spec, input, summaries };
    for change in starts.values() {
        evaluator.emit(change, false, &mut output, work).await?;
        evaluator.emit(change, true, &mut output, work).await?;
    }
    output.finish_batch()
}

struct Changes {
    base: Key,
    first: u64,
    entries: Vec<super::rows::Entry>,
}
impl Changes {
    fn bounds(&self) -> (Key, Key) {
        let mut lower = self.base.clone();
        lower.push(format!("{:016x}", self.first));
        let mut upper = self.base.clone();
        upper.push("ffffffffffffffff".into());
        (lower, upper)
    }
}
fn changes(delta: &Batch<Key, Row>, work: &mut PartitionWork) -> Result<BTreeMap<Key, Changes>> {
    let mut result = BTreeMap::new();
    for ((key, row), weight) in delta.iter() {
        work.charge(1)?;
        let (base, value) = split(key)?;
        let group = result.entry(base.clone()).or_insert_with(|| Changes {
            base,
            first: value,
            entries: Vec::new(),
        });
        group.first = group.first.min(value);
        group.entries.push(((key.clone(), row.clone()), *weight));
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partition_delta_is_routed_once_and_each_range_receives_only_its_own_rows() -> Result<()> {
        for count in [1, 8, 64, 1024] {
            let input = Batch::from_updates((0..count).map(|id| {
                ((vec![id.to_string(), "8000000000000000".into()].into(), Row::new()), 1)
            }))?;
            let mut work = PartitionWork::new(usize::try_from(count)?);
            let groups = changes(&input, &mut work)?;
            assert_eq!(work.remaining_for_test(), 0);
            assert_eq!(groups.len(), usize::try_from(count)?);
            for (base, group) in groups {
                assert_eq!(group.entries.len(), 1);
                assert!(group.entries.iter().all(|((key, _), _)| key.starts_with(&base)));
            }
        }
        Ok(())
    }
}
