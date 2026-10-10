use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
fn sql(numbered: bool) -> String {
    let function = if numbered { "row_number" } else { "rank" };
    let tie = if numbered { ",b.id" } else { "" };
    format!(
        "SELECT q.price FROM(SELECT b.id,b.auction,b.price,{function}() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST{tie}) AS r FROM source.bid b) q WHERE q.r<=3 AND(q.price>0 OR q.price IS NULL)"
    )
}
fn oracle(state: &BTreeMap<Row, i64>, numbered: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let price = |row: &Row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok());
    let mut outputs = Vec::new();
    for mut rows in groups.into_values() {
        rows.sort_by_key(|row| {
            (
                price(row).is_some(),
                price(row).map(std::cmp::Reverse),
                row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()),
            )
        });
        for (index, row) in rows.iter().enumerate() {
            let rank = if numbered {
                index + 1
            } else {
                rows.iter().take_while(|other| price(other) != price(row)).count() + 1
            };
            if rank <= 3 && price(row).is_none_or(|value| value > 0) {
                outputs.push(((Vec::new(), [("0:price".into(), row["price"].clone())].into()), 1));
            }
        }
    }
    Batch::from_updates(outputs)
}
async fn history(numbered: bool) -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(numbered), &native)?;
    let equivalent = compile(
        &sql(numbered).replace("q.", "\"Q\".").replace(") q WHERE", ") AS \"Q\" WHERE"),
        &native,
    )?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
    let ir = compiled.relational().context("missing derived IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("7"))]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", Some("7"))]);
    let d = row(&[("id", Some("4")), ("auction", None), ("price", Some("-2"))]);
    let moved = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let mut state = BTreeMap::new();
    for (index, changes) in [
        batch(vec![
            ("bid", a.clone(), 2),
            ("bid", b.clone(), 3),
            ("bid", c.clone(), 1),
            ("bid", d.clone(), 1),
        ]),
        batch(vec![("bid", b, -3), ("bid", moved.clone(), 3)]),
        batch(vec![("bid", a.clone(), -1), ("bid", d, -1)]),
        batch(vec![("bid", c, -1), ("bid", moved, -3), ("bid", a, -1)]),
    ]
    .into_iter()
    .enumerate()
    {
        let before = oracle(&state, numbered)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state, numbered)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn peer_top_k_keeps_ties_and_projection_multiplicities() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn occurrence_top_k_retracts_and_restores() -> Result<()> {
    history(true).await
}
#[test]
fn derived_scope_rejects_ambiguous_and_deferred_values() -> Result<()> {
    let native = contract();
    for query in [
        sql(false).replace("b.price,rank", "b.price AS r,rank"),
        sql(false).replace("q.r<=3", "b.id<=3"),
        sql(false).replace("q.r<=3", "q.missing<=3"),
        sql(false).replace(") q WHERE", ") q(x) WHERE"),
        sql(false).replace("FROM(", "FROM LATERAL("),
        "SELECT q.g FROM(SELECT b.auction AS g,AVG(b.price) AS a FROM source.bid b GROUP BY b.auction) q WHERE q.g IS NULL".into(),
        "SELECT q.id FROM(SELECT b.id FROM source.bid b) q WHERE q.id>0".into(),
    ] { assert!(compile(&query, &native).is_err(), "{query}"); }
    assert_ne!(
        serde_json::to_vec(&compile(&sql(false), &native)?)?,
        serde_json::to_vec(&compile(&sql(false).replace("<=3", "<=2"), &native)?)?
    );
    Ok(())
}
