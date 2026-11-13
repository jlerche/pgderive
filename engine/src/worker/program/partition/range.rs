//! Inclusive RANGE boundaries over a sorted partition.
use super::Ordered;
use crate::{
    compiler::partition::{Frame, Order, TemporalRangeFrame, TemporalRangeOffset},
    engine::{dataflow::PartitionWork, execution::EvaluationFailed},
    temporal::{Interval, Timestamp},
};
use anyhow::{Context, Result};
use std::{cmp::Ordering, ops::Range};

#[derive(Clone, Copy)]
enum Offset {
    Integral(i64),
    Interval { value: Interval, preceding: bool },
}

impl Offset {
    fn timestamp(offset: &TemporalRangeOffset) -> Result<Self> {
        Ok(match offset {
            TemporalRangeOffset::Current => Self::Integral(0),
            TemporalRangeOffset::Interval { value, preceding } => {
                Self::Interval { value: Interval::parse(value)?, preceding: *preceding }
            }
        })
    }

    fn boundary(self, current: i64, descending: bool, tail: bool) -> Result<i128> {
        match self {
            Self::Integral(distance) => Ok(oriented(current, descending) + i128::from(distance)),
            Self::Interval { value, preceding } => {
                let subtract = preceding != descending;
                if matches!(value, Interval::PositiveInfinity)
                    && current == if subtract { i64::MAX } else { i64::MIN }
                {
                    // PostgreSQL treats the indeterminate infinity comparison as true.
                    return Ok(if tail { i128::MAX } else { i128::MIN });
                }
                let value =
                    if subtract { value.negate().context(EvaluationFailed)? } else { value };
                let shifted = Timestamp::from_sort_value(current)
                    .add_interval(value)
                    .context(EvaluationFailed)?;
                Ok(oriented(shifted.sort_value(), descending))
            }
        }
    }
}

struct Boundary {
    index: usize,
    offset: Option<Offset>,
}

pub(super) struct Cursor {
    head: Boundary,
    tail: Boundary,
}

impl Cursor {
    pub(super) fn integral(frame: &Frame) -> Self {
        Self {
            head: Boundary { index: 0, offset: frame.start.map(Offset::Integral) },
            tail: Boundary { index: 0, offset: frame.end.map(Offset::Integral) },
        }
    }

    pub(super) fn temporal(frame: &TemporalRangeFrame) -> Result<Self> {
        Ok(Self {
            head: Boundary {
                index: 0,
                offset: frame.start.as_ref().map(Offset::timestamp).transpose()?,
            },
            tail: Boundary {
                index: 0,
                offset: frame.end.as_ref().map(Offset::timestamp).transpose()?,
            },
        })
    }

    pub(super) fn bounds(
        &mut self,
        rows: &[Ordered],
        order: &Order,
        current: usize,
        work: &mut PartitionWork,
    ) -> Result<Range<usize>> {
        let current = order_value(&rows[current])?;
        // Like PostgreSQL, keep both positions forward-only, even if clipping a
        // calendar month moves the calculated timestamp backwards.
        self.head.advance(rows, order, current, (false, work))?;
        self.tail.advance(rows, order, current, (true, work))?;
        Ok(self.head.index.min(self.tail.index)..self.tail.index)
    }
}

impl Boundary {
    fn advance(
        &mut self,
        rows: &[Ordered],
        order: &Order,
        current: Option<i64>,
        context: (bool, &mut PartitionWork),
    ) -> Result<()> {
        let (tail, work) = context;
        let Some(offset) = self.offset else {
            self.index = if tail { rows.len() } else { 0 };
            return Ok(());
        };
        let mut boundary = None;
        while let Some(row) = rows.get(self.index) {
            work.charge(1)?;
            let key = order_value(row)?;
            let comparison = match (key, current) {
                (Some(key), Some(current)) => {
                    let threshold = if let Some(value) = boundary {
                        value
                    } else {
                        let value = offset.boundary(current, order.descending, tail)?;
                        boundary = Some(value);
                        value
                    };
                    oriented(key, order.descending).cmp(&threshold)
                }
                _ if order.nulls_first => key.cmp(&current),
                _ => current.cmp(&key),
            };
            if comparison == Ordering::Greater || (!tail && comparison == Ordering::Equal) {
                break;
            }
            self.index += 1;
        }
        Ok(())
    }
}

fn order_value(row: &Ordered) -> Result<Option<i64>> {
    row.0.first().copied().context("missing RANGE key")
}

fn oriented(value: i64, descending: bool) -> i128 {
    let value = i128::from(value);
    if descending { -value } else { value }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::ColumnRef;

    #[test]
    fn timestamp_overflow_is_a_recoverable_evaluation_error() -> Result<()> {
        let order = Order {
            column: ColumnRef {
                name: "observed".into(),
                right: false,
                oid: 1114,
                nullable: true,
                collation: 0,
                modifier: None,
            },
            descending: false,
            nulls_first: false,
        };
        let frame = TemporalRangeFrame {
            exclusion: crate::compiler::partition::Exclusion::None,
            start: Some(TemporalRangeOffset::Current),
            end: Some(TemporalRangeOffset::Interval {
                value: "P0M1DT0H0S".into(),
                preceding: false,
            }),
        };
        let key = Timestamp::parse(Timestamp::LAST_FINITE, 1114)?.sort_value();
        let mut work = PartitionWork::new(100);
        let mut cursor = Cursor::temporal(&frame)?;
        let rows = [(vec![None], crate::transaction::Row::new())];
        assert_eq!(cursor.bounds(&rows, &order, 0, &mut work)?, 0..1);
        let mut cursor = Cursor::temporal(&frame)?;
        let rows = [(vec![Some(key)], crate::transaction::Row::new())];
        assert!(matches!(cursor.bounds(&rows, &order, 0, &mut work),
            Err(error) if error.is::<EvaluationFailed>()));
        Ok(())
    }
}
