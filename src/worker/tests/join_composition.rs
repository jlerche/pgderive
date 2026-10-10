//! Joined full-tuple deltas feeding retained SQL scopes.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type WeightedRows = Vec<(Row, i64)>;
type Bag = Batch<relational::Key, Row>;
const INNER: &str = "SELECT a.id AS aid,b.id AS bid,b.price AS p FROM source.auction a JOIN source.bid b ON a.id=b.auction WHERE b.price IS NULL OR b.price<0";
fn sql(winner: bool) -> String {
    if winner {
        format!(
            "SELECT w.aid,w.bid,w.p FROM(SELECT q.aid,q.bid,q.p,row_number() OVER(PARTITION BY q.aid ORDER BY q.p DESC NULLS LAST,q.bid,q.aid) AS r FROM({INNER}) q) w WHERE w.r<=1"
        )
    } else {
        format!("SELECT q.aid,COUNT(*) AS n,MAX(q.p) AS m FROM({INNER}) q GROUP BY q.aid")
    }
}
fn oracle(left: &BTreeMap<Row, i64>, right: &BTreeMap<Row, i64>, winner: bool) -> Result<Bag> {
    let mut groups: BTreeMap<String, WeightedRows> = BTreeMap::new();
    for (a, wa) in left {
        for (b, wb) in right {
            if *wa > 0
                && *wb > 0
                && b["auction"].is_some()
                && a["id"] == b["auction"]
                && b["price"].as_ref().is_none_or(|p| p.parse::<i64>().is_ok_and(|p| p < 0))
            {
                groups
                    .entry(a["id"].clone().context("missing auction id")?)
                    .or_default()
                    .push((b.clone(), wa * wb));
            }
        }
    }
    let mut outputs = Vec::new();
    for (id, mut rows) in groups {
        let output = if winner {
            rows.sort_by_key(|(row, _)| {
                let price = row["price"].as_ref().and_then(|p| p.parse::<i64>().ok());
                (
                    price.is_none(),
                    price.map(std::cmp::Reverse),
                    row["id"].as_ref().and_then(|id| id.parse::<i64>().ok()),
                )
            });
            let value = &rows[0].0;
            [
                ("0:id".into(), Some(id)),
                ("1:id".into(), value["id"].clone()),
                ("1:price".into(), value["price"].clone()),
            ]
            .into()
        } else {
            let count: i64 = rows.iter().map(|(_, weight)| weight).sum();
            let maximum =
                rows.iter().filter_map(|(row, _)| row["price"].as_ref()?.parse::<i64>().ok()).max();
            [
                ("0:id".into(), Some(id)),
                ("@aggregate_0".into(), Some(count.to_string())),
                ("@aggregate_1".into(), maximum.map(|p| p.to_string())),
            ]
            .into()
        };
        outputs.push(((Vec::new(), output), 1));
    }
    Batch::from_updates(outputs)
}
async fn history(winner: bool) -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(winner), &native)?;
    let ir = compiled.relational().context("missing joined IR")?;
    assert!(ir.nodes.iter().any(|node| matches!(node, crate::compiler::relational::Node::Filter { id, .. } if id == "joined_source_filter")));
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("-2"))]);
    let c = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let mut changed = b.clone();
    changed.insert("price".into(), Some("7".into()));
    let mut left = BTreeMap::new();
    let mut right = BTreeMap::new();
    for changes in [
        batch(vec![("auction", a.clone(), 2), ("bid", b.clone(), 3), ("bid", c.clone(), 1)]),
        batch(vec![
            ("auction", a.clone(), -1),
            ("bid", b.clone(), -3),
            ("bid", changed.clone(), 3),
        ]),
        batch(vec![("auction", a.clone(), 1), ("bid", changed, -3), ("bid", b.clone(), 3)]),
        batch(vec![("bid", b, -3), ("bid", c, -1), ("auction", a, -2)]),
        batch(vec![]),
    ] {
        let before = oracle(&left, &right, winner)?;
        for change in &changes.updates {
            let state = if change.tuple.table == "auction" { &mut left } else { &mut right };
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&left, &right, winner)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint).await?;
    }
    Ok(())
}
#[tokio::test]
async fn joined_group_recomputes_weighted_cross_term_and_restores() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn joined_top_one_selects_occurrences_and_restores() -> Result<()> {
    history(true).await
}
#[test]
fn joined_scopes_reject_incomplete_order_hidden_and_terminal_columns() -> Result<()> {
    let native = contract();
    for invalid in [
        sql(true).replace(",q.bid,q.aid)", ",q.aid)"),
        sql(true).replace(",q.bid,q.aid)", ",q.bid)"),
        sql(false).replace("MAX(q.p)", "MAX(q.auction)"),
        sql(false).replace("b.price AS p", "abs(b.price) AS p"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    let original = compile(&sql(true), &native)?;
    let alias = sql(true).replace("a.", "\"A\".").replace("auction a ", "auction AS \"A\" ");
    assert_eq!(serde_json::to_vec(&original)?, serde_json::to_vec(&compile(&alias, &native)?)?);
    assert_ne!(
        serde_json::to_vec(&original)?,
        serde_json::to_vec(&compile(&sql(true).replace("b.price<0", "b.price<1"), &native)?)?
    );
    Ok(())
}
