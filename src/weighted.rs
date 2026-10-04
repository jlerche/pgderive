use crate::engine::ZSet;
use crate::transaction::{Change, Operation, Row};
use anyhow::{Result, ensure};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Tuple {
    pub(super) schema: String,
    pub(super) table: String,
    pub(super) row: Row,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Update {
    pub(super) tuple: Tuple,
    pub(super) weight: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Batch {
    pub(super) updates: Vec<Update>,
}

impl Batch {
    pub(super) fn from_changes(changes: &[Change]) -> Result<Self> {
        let mut weights = Vec::new();
        for change in changes {
            validate(change)?;
            if let Some(row) = &change.old {
                accumulate(&mut weights, change, row, -1);
            }
            if let Some(row) = &change.new {
                accumulate(&mut weights, change, row, 1);
            }
        }
        let weights = ZSet::from_updates(weights)?;
        let updates = weights
            .iter()
            .map(|(tuple, weight)| Update { tuple: tuple.clone(), weight: *weight })
            .collect();
        Ok(Self { updates })
    }
}

fn validate(change: &Change) -> Result<()> {
    let valid = match change.operation {
        Operation::Insert => change.old.is_none() && change.new.is_some(),
        Operation::Update => change.old.is_some() && change.new.is_some(),
        Operation::Delete => change.old.is_some() && change.new.is_none(),
    };
    ensure!(valid, "weighted input requires complete row images for {:?}", change.operation);
    Ok(())
}

fn accumulate(weights: &mut Vec<(Tuple, i64)>, change: &Change, row: &Row, delta: i64) {
    let tuple =
        Tuple { schema: change.schema.clone(), table: change.table.clone(), row: row.clone() };
    weights.push((tuple, delta));
}

#[cfg(test)]
mod tests;
