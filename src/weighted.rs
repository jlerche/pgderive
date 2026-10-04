use crate::engine::execution::{Consolidator, Limits};
use crate::transaction::{Change, Operation, Row};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    #[cfg(test)]
    pub(super) fn from_changes(changes: &[Change]) -> Result<Self> {
        Self::from_changes_with_limits(changes, Limits::default())
    }
    pub(super) fn from_changes_with_limits(changes: &[Change], limits: Limits) -> Result<Self> {
        let mut weights = Consolidator::new(limits)?;
        for change in changes {
            validate(change)?;
            if let Some(row) = &change.old {
                accumulate(&mut weights, change, row, -1)?;
            }
            if let Some(row) = &change.new {
                accumulate(&mut weights, change, row, 1)?;
            }
        }
        let mut updates = Vec::new();
        weights.finish(|tuple, weight| {
            updates.push(Update { tuple, weight: i64::try_from(weight)? });
            Ok(())
        })?;
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

fn accumulate(
    weights: &mut Consolidator<Tuple>,
    change: &Change,
    row: &Row,
    delta: i64,
) -> Result<()> {
    let tuple =
        Tuple { schema: change.schema.clone(), table: change.table.clone(), row: row.clone() };
    weights.add(tuple, delta)
}

#[cfg(test)]
mod tests;
