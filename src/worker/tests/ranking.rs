use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
const PEERS: &str = "SELECT b.id,rank() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST) AS r,dense_rank() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST) AS d FROM source.bid b";
const NUMBER: &str = "SELECT b.id,row_number() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST,b.id) AS r FROM source.bid b";
fn oracle(state: &BTreeMap<Row, i64>, numbered: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut output = Vec::new();
    for mut rows in groups.into_values() {
        let price = |row: &Row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok());
        rows.sort_by_key(|row| {
            (
                price(row).is_some(),
                price(row).map(std::cmp::Reverse),
                row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()),
            )
        });
        let mut distinct = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let preceding = rows.iter().take_while(|prior| price(prior) != price(row)).count();
            if !distinct.contains(&price(row)) {
                distinct.push(price(row));
            }
            let mut result: Row = [
                ("0:id".into(), row["id"].clone()),
                (
                    "@aggregate_0".into(),
                    Some(if numbered { index + 1 } else { preceding + 1 }.to_string()),
                ),
            ]
            .into();
            if !numbered {
                result.insert("@aggregate_1".into(), Some(distinct.len().to_string()));
            }
            output.push(((Vec::new(), result), 1));
        }
    }
    Batch::from_updates(output)
}
async fn history(numbered: bool) -> Result<()> {
    let native = contract();
    let sql = if numbered { NUMBER } else { PEERS };
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing ranking IR")?;
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
async fn weighted_peer_ranks_revise_and_restart() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn weighted_row_numbers_revise_and_restart() -> Result<()> {
    history(true).await
}
#[test]
fn rejects_unsupported_ranking_and_binds_order() -> Result<()> {
    let native = contract();
    for sql in [
        NUMBER.replace(",b.id", ""),
        PEERS.replace("rank()", "rank(b.id)"),
        PEERS.replace("rank() OVER", "rank() FILTER(WHERE b.price>0) OVER"),
        PEERS.replace("NULLS FIRST)", "NULLS FIRST ROWS UNBOUNDED PRECEDING)"),
        PEERS.replace("rank()", "ntile()"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    let empty_order = compile(&PEERS.replace(" ORDER BY b.price DESC NULLS FIRST", ""), &native)?;
    assert!(empty_order.relational().is_some());
    assert_ne!(
        serde_json::to_vec(&compile(PEERS, &native)?)?,
        serde_json::to_vec(&compile(&PEERS.replace("DESC", "ASC"), &native)?)?
    );
    Ok(())
}
