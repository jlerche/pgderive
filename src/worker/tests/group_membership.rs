//! Group-only SQL retains distinct membership, then preserves projected bag collisions.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

type Bag = Batch<relational::Key, Row>;
fn oracle(state: &BTreeMap<Row, i64>) -> Result<Bag> {
    let memberships = state
        .iter()
        .filter(|(_, weight)| **weight > 0)
        .map(|(row, _)| (row["auction"].clone(), row["price"].clone()))
        .collect::<BTreeSet<_>>();
    Batch::from_updates(memberships.into_iter().map(|(auction, _)| {
        (
            (
                crate::worker::program::relational::Key::new(),
                [("0:auction".into(), auction)].into(),
            ),
            1,
        )
    }))
}

#[tokio::test]
async fn group_only_membership_preserves_duplicates_nulls_and_restart() -> Result<()> {
    let native = contract();
    let sql = "SELECT b.auction FROM source.bid b GROUP BY b.auction,b.price";
    let compiled = compile(sql, &native)?;
    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", Some("7"))]);
    let moved = row(&[("id", Some("2")), ("auction", Some("9")), ("price", Some("7"))]);
    super::check_row_history(
        (&native, &compiled, &options),
        [
            batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 3), ("bid", c.clone(), 1)]),
            batch(vec![("bid", a.clone(), -1), ("bid", b.clone(), -2)]),
            batch(vec![("bid", a.clone(), -1), ("bid", b, -1), ("bid", moved.clone(), 1)]),
            batch(vec![("bid", c, -1), ("bid", moved, -1)]),
        ],
        oracle,
        |index| index == 1,
    )
    .await?;
    Ok(())
}

#[test]
fn group_only_identity_and_scope_validation() -> Result<()> {
    let native = contract();
    let sql = "SELECT b.auction FROM source.bid b GROUP BY b.auction,b.price";
    let compiled = compile(sql, &native)?;

    let equivalent =
        compile(&sql.replace("b.", "\"B\".").replace("bid b", "bid AS \"B\""), &native)?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
    let changed = compile("SELECT b.auction FROM source.bid b GROUP BY b.auction", &native)?;
    assert_ne!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&changed)?);
    assert!(compile("SELECT b.id FROM source.bid b GROUP BY b.auction", &native).is_err());
    let derived = format!("SELECT q.auction FROM({sql}) q WHERE q.auction IS NULL");
    assert!(!compile(&derived, &native)?.relational().nodes.is_empty());
    let projection = compile("SELECT b.auction FROM source.bid b", &native)?;
    assert!(
        !projection
            .relational()
            .nodes
            .iter()
            .any(|node| matches!(node, crate::compiler::relational::Node::Statistics { .. }))
    );

    Ok(())
}
