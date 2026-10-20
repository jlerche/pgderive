//! Frame-local integral extrema retain duplicate values until their last departure.
use crate::{
    compiler::partition::{Aggregate, Function},
    transaction::Row,
    worker::program::aggregate::{Contribution, Linear, contribution},
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;

pub(super) enum Statistics {
    Linear(Linear),
    Extrema(BTreeMap<i64, u64>),
}
impl Statistics {
    pub(super) fn new(aggregate: &Aggregate) -> Self {
        if matches!(aggregate.function, Function::Min | Function::Max) {
            Self::Extrema(BTreeMap::new())
        } else {
            Self::Linear(Linear::default())
        }
    }
    pub(super) fn observe(&mut self, aggregate: &Aggregate, row: &Row, sign: i64) -> Result<()> {
        let values = match self {
            Self::Linear(linear) => return linear.observe(aggregate, row, sign),
            Self::Extrema(values) => values,
        };
        let Some(Contribution::Value(text)) = contribution(aggregate, row)? else {
            return Ok(());
        };
        let value = text.parse::<i64>()?;
        let count = values
            .get(&value)
            .copied()
            .unwrap_or(0)
            .checked_add_signed(sign)
            .context("sliding extremum multiplicity overflow or underflow")?;
        if count == 0 {
            values.remove(&value);
        } else {
            values.insert(value, count);
        }
        Ok(())
    }
    pub(super) fn result(&self, aggregate: &Aggregate) -> Result<Option<String>> {
        match self {
            Self::Linear(linear) => linear.result(aggregate),
            Self::Extrema(values) => {
                let extremum = if matches!(aggregate.function, Function::Min) {
                    values.first_key_value()
                } else {
                    values.last_key_value()
                };
                Ok(extremum.map(|(value, _)| value.to_string()))
            }
        }
    }
}
