use crate::transaction::{Change, Operation, Row};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::collections::BTreeMap;

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
        let mut weights = BTreeMap::<Tuple, i64>::new();
        for change in changes {
            validate(change)?;
            if let Some(row) = &change.old {
                accumulate(&mut weights, change, row, -1)?;
            }
            if let Some(row) = &change.new {
                accumulate(&mut weights, change, row, 1)?;
            }
        }
        let updates = weights
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .map(|(tuple, weight)| Update { tuple, weight })
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

fn accumulate(
    weights: &mut BTreeMap<Tuple, i64>,
    change: &Change,
    row: &Row,
    delta: i64,
) -> Result<()> {
    let tuple =
        Tuple { schema: change.schema.clone(), table: change.table.clone(), row: row.clone() };
    let weight = weights.entry(tuple).or_default();
    *weight = weight.checked_add(delta).context("tuple weight overflow")?;
    Ok(())
}

#[cfg(test)]
mod tests;
