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
mod tests {
    use super::Batch;
    use crate::transaction::{Change, Operation, Row};

    fn row(value: Option<&str>) -> Row {
        Row::from([("id".into(), Some("1".into())), ("value".into(), value.map(str::to_owned))])
    }

    fn change(operation: Operation, old: Option<Row>, new: Option<Row>) -> Change {
        Change { schema: "source".into(), table: "rows".into(), operation, old, new }
    }

    #[test]
    fn updates_preserve_full_tuple_identity() -> anyhow::Result<()> {
        let batch = Batch::from_changes(&[change(
            Operation::Update,
            Some(row(Some("a"))),
            Some(row(None)),
        )])?;
        assert_eq!(batch.updates.len(), 2);
        assert!(
            batch
                .updates
                .iter()
                .any(|update| update.tuple.row == row(Some("a")) && update.weight == -1)
        );
        assert!(
            batch.updates.iter().any(|update| update.tuple.row == row(None) && update.weight == 1)
        );
        Ok(())
    }

    #[test]
    fn consolidate_multiplicity_and_cancel_within_commit() -> anyhow::Result<()> {
        let insert = change(Operation::Insert, None, Some(row(Some("a"))));
        let delete = change(Operation::Delete, Some(row(Some("a"))), None);
        let batch = Batch::from_changes(&[insert.clone(), insert.clone(), delete.clone()])?;
        assert_eq!(batch.updates.len(), 1);
        assert_eq!(batch.updates[0].weight, 1);
        assert!(Batch::from_changes(&[insert, delete])?.updates.is_empty());
        assert!(
            Batch::from_changes(&[change(Operation::Update, Some(row(None)), Some(row(None)))])?
                .updates
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn relation_identity_and_order_are_deterministic() -> anyhow::Result<()> {
        let first = change(Operation::Insert, None, Some(row(None)));
        let mut second = first.clone();
        second.table = "other".into();
        let mut third = first.clone();
        third.schema = "other".into();
        let forward = Batch::from_changes(&[first.clone(), second.clone(), third.clone()])?;
        let reverse = Batch::from_changes(&[third, second, first])?;
        assert_eq!(forward, reverse);
        assert_eq!(forward.updates.len(), 3);
        Ok(())
    }

    #[test]
    fn incomplete_images_are_rejected() {
        for invalid in [
            change(Operation::Update, None, Some(row(None))),
            change(Operation::Delete, None, None),
            change(Operation::Insert, Some(row(None)), Some(row(None))),
        ] {
            assert!(Batch::from_changes(&[invalid]).is_err());
        }
    }
}
