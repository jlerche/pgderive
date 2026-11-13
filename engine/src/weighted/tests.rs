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
    let batch =
        Batch::from_changes(&[change(Operation::Update, Some(row(Some("a"))), Some(row(None)))])?;
    assert_eq!(batch.updates.len(), 2);
    assert!(
        batch
            .updates
            .iter()
            .any(|update| update.tuple.row == row(Some("a")) && update.weight == -1)
    );
    assert!(batch.updates.iter().any(|update| update.tuple.row == row(None) && update.weight == 1));
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
