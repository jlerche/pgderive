//! Transient exact statistics over monotone frame ranges; no persisted index.
use super::{Ordered, argument_value, framed_fields, integral_result};
use crate::{
    compiler::partition::{Function, Partition},
    engine::{dataflow::PartitionWork, execution::Limits},
    transaction::Row,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::ops::Range;
#[derive(Default)]
struct State {
    count: BigInt,
    sum: BigInt,
}
#[derive(Default)]
struct Sliding {
    range: Range<usize>,
    states: Vec<State>,
}
pub(super) struct Frames(Option<Sliding>);
impl Frames {
    pub(super) fn new(spec: &Partition) -> Self {
        let eligible = !spec.aggregates.is_empty()
            && spec.aggregates.iter().all(|value| {
                matches!(value.function, Function::Count)
                    || matches!(value.function, Function::Sum | Function::Average)
                        && value
                            .argument
                            .as_ref()
                            .is_some_and(|column| matches!(column.oid, 20 | 21 | 23))
            });
        Self(eligible.then(|| Sliding {
            range: 0..0,
            states: spec.aggregates.iter().map(|_| State::default()).collect(),
        }))
    }
    #[cfg(test)]
    pub(super) const fn scan() -> Self {
        Self(None)
    }
    pub(super) fn fields(
        &mut self,
        spec: &Partition,
        rows: &[Ordered],
        range: Range<usize>,
        context: (Limits, &mut PartitionWork),
    ) -> Result<Row> {
        let Some(sliding) = &mut self.0 else {
            return framed_fields(spec, &rows[range], context);
        };
        sliding.advance(spec, rows, range, context.1)?;
        let mut output = Row::new();
        for (aggregate, state) in spec.aggregates.iter().zip(&sliding.states) {
            context.1.charge(1)?;
            output.insert(
                aggregate.field.clone(),
                integral_result(aggregate, &state.count, &state.sum)?,
            );
        }
        Ok(output)
    }
}
impl Sliding {
    fn advance(
        &mut self,
        spec: &Partition,
        rows: &[Ordered],
        range: Range<usize>,
        work: &mut PartitionWork,
    ) -> Result<()> {
        ensure!(
            range.start >= self.range.start
                && range.end >= self.range.end
                && range.start <= range.end
                && range.end <= rows.len(),
            "window frame ranges must advance monotonically"
        );
        for (_, row) in &rows[self.range.start..range.start.min(self.range.end)] {
            self.change(spec, row, -1, work)?;
        }
        for (_, row) in &rows[range.start.max(self.range.end)..range.end] {
            self.change(spec, row, 1, work)?;
        }
        self.range = range;
        Ok(())
    }
    fn change(
        &mut self,
        spec: &Partition,
        row: &Row,
        sign: i64,
        work: &mut PartitionWork,
    ) -> Result<()> {
        for (aggregate, state) in spec.aggregates.iter().zip(&mut self.states) {
            work.charge(1)?;
            if aggregate
                .filter
                .as_ref()
                .map(|filter| filter.qualifies(row))
                .transpose()?
                .is_some_and(|value| !value)
            {
                continue;
            }
            let value = argument_value(aggregate, row)?;
            if aggregate.argument.is_some() && value.is_none() {
                continue;
            }
            state.count += sign;
            if !matches!(aggregate.function, Function::Count) {
                let integer = value
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("missing sliding measure"))?
                    .parse::<i64>()?;
                state.sum += BigInt::from(integer) * sign;
            }
        }
        Ok(())
    }
}
