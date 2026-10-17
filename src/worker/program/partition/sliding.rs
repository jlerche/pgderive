//! Transient exact statistics over monotone frame ranges; no persisted index.
use super::{Ordered, framed_fields};
use crate::worker::program::aggregate::Linear;
use crate::{
    compiler::partition::{Function, Partition},
    engine::{dataflow::PartitionWork, execution::Limits},
    transaction::Row,
};
use anyhow::{Result, ensure};
use std::ops::Range;
#[derive(Default)]
struct Sliding {
    range: Range<usize>,
    states: Vec<Linear>,
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
            states: spec.aggregates.iter().map(|_| Linear::default()).collect(),
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
            output.insert(aggregate.field.clone(), state.result(aggregate)?);
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
            state.observe(aggregate, row, sign)?;
        }
        Ok(())
    }
}
