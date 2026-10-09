use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
const SQL: &str = "SELECT a.category AS c,b.price AS p FROM source.auction a JOIN source.bid b ON a.id=b.auction WHERE b.price IS NULL OR b.price<0";
fn oracle(
    left: &BTreeMap<Row, i64>,
    right: &BTreeMap<Row, i64>,
) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (a, wa) in left {
        for (b, wb) in right {
            if b["auction"].is_some()
                && a["id"] == b["auction"]
                && b["price"].as_ref().is_none_or(|v| v.parse::<i64>().is_ok_and(|v| v < 0))
            {
                let output = [
                    ("0:category".into(), a["category"].clone()),
                    ("1:price".into(), b["price"].clone()),
                ]
                .into();
                updates.push(((Vec::new(), output), wa * wb));
            }
        }
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn compiled_join_deltas_cold_restart_and_atomic_failure() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational().context("missing IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("-2"))]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("-2"))]);
    let mut changed = a.clone();
    changed.insert("category".into(), Some("changed".into()));
    let histories = [
        batch(vec![("auction", a.clone(), 1), ("bid", b.clone(), 1), ("bid", b2.clone(), 1)]),
        batch(vec![
            ("auction", a.clone(), -1),
            ("auction", changed.clone(), 1),
            ("bid", b.clone(), -1),
        ]),
        batch(vec![("auction", changed, -1), ("auction", a, 1), ("bid", b, 1)]),
        batch(vec![("bid", b2, -1)]),
        batch(vec![]),
    ];
    let mut left = BTreeMap::new();
    let mut right = BTreeMap::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&left, &right)?;
        for change in &changes.updates {
            let state = if change.tuple.table == "auction" { &mut left } else { &mut right };
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&left, &right)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, expected);
        relational::deltas(&work.output().batch, &ir.output)?;
        query.prepared_checkpoint(&work)?;
        query.commit(work)?;
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    let before = query.checkpoint()?;
    let malformed = batch(vec![("auction", row(&[("category", None)]), 1)]);
    assert!(query.prepare(relational::inputs(&malformed, ir, query.time() + 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    let mut incomplete = relational::inputs(&batch(vec![]), ir, query.time() + 1)?;
    incomplete.batch.pop();
    assert!(query.prepare(incomplete).await.is_err());
    let work = query.prepare(relational::inputs(&batch(vec![]), ir, query.time() + 1)?).await?;
    let mut foreign = relational::build(&native, &compiled, &options)?;
    assert!(foreign.prepared_checkpoint(&work).is_err());
    assert!(foreign.commit(work).is_err());
    Ok(())
}
#[test]
fn normalized_join_identity_and_fail_closed_resolution() -> Result<()> {
    let native = contract();
    let first = compile(SQL, &native)?;
    let alias = compile(
        &SQL.replace("a.", "\"X\".")
            .replace("auction a ", "auction AS \"X\" ")
            .replace("a.id=b.auction", "b.auction=\"X\".id"),
        &native,
    )?;
    assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&alias)?);
    for sql in [
        SQL.replace("a.category AS c", "id AS c"),
        SQL.replace("a.id=b.auction", "a.id=b.price"),
        SQL.replace("a.id=b.auction", "a.id=a.id"),
        format!("{SQL} ORDER BY p"),
        SQL.replace("JOIN", "LEFT JOIN"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    assert_ne!(
        serde_json::to_vec(&first)?,
        serde_json::to_vec(&compile(&SQL.replace("b.price<0", "b.price<1"), &native)?)?
    );
    Ok(())
}
