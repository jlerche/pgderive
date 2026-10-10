use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
fn sql(case: usize) -> String {
    let order = if case == 4 { "" } else { " ORDER BY b.price DESC NULLS FIRST" };
    let frame = match case {
        1 => " GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        2 => " RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
        3 => " GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING",
        _ => "",
    };
    let window = format!("OVER(PARTITION BY b.auction{order}{frame})");
    format!("SELECT b.id,COUNT(*) {window} AS n,SUM(b.price) {window} AS s FROM source.bid b")
}
fn oracle(state: &BTreeMap<Row, i64>, case: usize) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let price = |row: &Row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok());
    let mut updates = Vec::new();
    for rows in groups.into_values() {
        let mut keys = rows.iter().map(price).collect::<Vec<_>>();
        keys.sort_by_key(|key| (key.is_some(), key.map(std::cmp::Reverse)));
        keys.dedup();
        for row in &rows {
            let current = keys.iter().position(|key| *key == price(row)).context("missing peer")?;
            let mut values = Vec::new();
            let mut count = 0;
            for other in &rows {
                let group =
                    keys.iter().position(|key| *key == price(other)).context("missing peer")?;
                let selected = match case {
                    1 => group.abs_diff(current) <= 1,
                    2 => group == current,
                    3 => false,
                    4 => true,
                    _ => group <= current,
                };
                if selected {
                    count += 1;
                    values.extend(price(other));
                }
            }
            let output = [
                ("0:id".into(), row["id"].clone()),
                ("@aggregate_0".into(), Some(count.to_string())),
                (
                    "@aggregate_1".into(),
                    (!values.is_empty()).then(|| values.iter().sum::<i64>().to_string()),
                ),
            ]
            .into();
            updates.push(((Vec::new(), output), 1));
        }
    }
    Batch::from_updates(updates)
}
async fn history(case: usize) -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(case), &native)?;
    let ir = compiled.relational().context("missing peer IR")?;
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
        let before = oracle(&state, case)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state, case)?;
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
async fn weighted_peer_frames_match_independent_histories() -> Result<()> {
    for case in 0..5 {
        history(case).await?;
    }
    Ok(())
}
#[test]
fn rejects_unimplemented_distance_and_exclusions() -> Result<()> {
    for sql in [
        sql(1).replace("GROUPS", "RANGE"),
        sql(1).replace(" ORDER BY b.price DESC NULLS FIRST", ""),
        sql(2).replace("CURRENT ROW)", "CURRENT ROW EXCLUDE TIES)"),
    ] {
        assert!(compile(&sql, &contract()).is_err(), "{sql}");
    }
    let default = compile(&sql(0), &contract())?;
    let explicit = compile(
        &sql(0).replace("NULLS FIRST)", "NULLS FIRST RANGE UNBOUNDED PRECEDING)"),
        &contract(),
    )?;
    assert_eq!(serde_json::to_vec(&default)?, serde_json::to_vec(&explicit)?);
    assert_ne!(serde_json::to_vec(&default)?, serde_json::to_vec(&compile(&sql(2), &contract())?)?);
    Ok(())
}
