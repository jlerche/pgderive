//! Inclusive integral RANGE boundaries over a sorted partition.
use super::Ordered;
use crate::{
    compiler::partition::{Frame, Order},
    engine::dataflow::PartitionWork,
};
use anyhow::{Context, Result};
use std::{cmp::Ordering, ops::Range};

#[derive(Default)]
pub(super) struct Cursor {
    head: usize,
    tail: usize,
}

impl Cursor {
    pub(super) fn bounds(
        &mut self,
        rows: &[Ordered],
        specification: (&Frame, &Order),
        current: usize,
        work: &mut PartitionWork,
    ) -> Result<Range<usize>> {
        let (frame, order) = specification;
        let key = order_value(&rows[current], order)?;
        if let Some(distance) = frame.start {
            advance(
                &mut self.head,
                rows,
                order,
                key.map(|key| key + i128::from(distance)),
                (false, work),
            )?;
        }
        if let Some(distance) = frame.end {
            advance(
                &mut self.tail,
                rows,
                order,
                key.map(|key| key + i128::from(distance)),
                (true, work),
            )?;
        } else {
            self.tail = rows.len();
        }
        Ok(self.head.min(self.tail)..self.tail)
    }
}

fn order_value(row: &Ordered, order: &Order) -> Result<Option<i128>> {
    let value = row.0.first().context("missing integral RANGE key")?;
    // Orient the domain in sort order and allow boundaries outside the int8 domain.
    Ok(value.map(|value| {
        let value = i128::from(value);
        if order.descending { -value } else { value }
    }))
}

fn advance(
    cursor: &mut usize,
    rows: &[Ordered],
    order: &Order,
    boundary: Option<i128>,
    context: (bool, &mut PartitionWork),
) -> Result<()> {
    let (inclusive, work) = context;
    while let Some(row) = rows.get(*cursor) {
        work.charge(1)?;
        let key = order_value(row, order)?;
        let comparison = match (key, boundary) {
            (Some(key), Some(boundary)) => key.cmp(&boundary),
            _ if order.nulls_first => key.cmp(&boundary),
            _ => boundary.cmp(&key),
        };
        if comparison == Ordering::Greater || (!inclusive && comparison == Ordering::Equal) {
            break;
        }
        *cursor += 1;
    }
    Ok(())
}
