use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
const SQL: &str = "SELECT b.auction,COUNT(*),COUNT(b.price) AS present,SUM(b.price),AVG(b.price),COUNT(*) FILTER(WHERE b.price<0) AS negative FROM source.bid b GROUP BY b.auction";
fn oracle(state: &BTreeMap<Row, i64>) -> Result<Bag> {
    let mut groups = BTreeMap::<Option<String>, (i64, i64, i128, i64)>::new();
    for (row, weight) in state {
        if *weight == 0 {
            continue;
        }
        let values = groups.entry(row["auction"].clone()).or_default();
        values.0 += weight;
        if let Some(price) = &row["price"] {
            let price = price.parse::<i64>()?;
            values.1 += weight;
            values.2 += i128::from(price) * i128::from(*weight);
            if price < 0 {
                values.3 += weight;
            }
        }
    }
    Batch::from_updates(groups.into_iter().filter(|(_, values)| values.0 != 0).map(
        |(key, (rows, present, sum, negative))| {
            let output: Row = [
                ("0:auction".into(), key),
                ("@aggregate_0".into(), Some(rows.to_string())),
                ("@aggregate_1".into(), Some(present.to_string())),
                ("@aggregate_2".into(), (present != 0).then(|| sum.to_string())),
                ("@aggregate_3".into(), (present != 0).then(|| format!("{sum}/{present}"))),
                ("@aggregate_4".into(), Some(negative.to_string())),
            ]
            .into();
            ((Vec::new(), output), 1)
        },
    ))
}
#[tokio::test]
async fn linear_statistics_match_full_bag_oracle_through_retractions_and_restart() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational().context("missing linear IR")?;
    assert!(
        compiled
            .revision
            .as_deref()
            .is_some_and(|revision| revision.contains("exact-linear-statistics-v1"))
    );
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", Some("7"))]);
    let moved = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("3"))]);
    let histories = [
        vec![("bid", a.clone(), 3), ("bid", b.clone(), 2), ("bid", c.clone(), 1)],
        vec![("bid", a.clone(), -2), ("bid", moved.clone(), 2)],
        vec![("bid", b.clone(), -2), ("bid", a.clone(), -1)],
        vec![("bid", c, -1), ("bid", moved, -2)],
        vec![],
    ];
    let mut state = BTreeMap::new();
    for (index, updates) in histories.into_iter().enumerate() {
        let before = oracle(&state)?;
        let delta = batch(updates);
        for change in &delta.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&delta, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    assert_eq!(query.time(), 5);
    Ok(())
}
#[tokio::test]
async fn linear_statistics_update_large_group_under_delta_budget() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction,COUNT(*),SUM(b.price) FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let ir = compiled.relational().context("missing linear IR")?;
    let mut options = settings();
    options.block_rows = 64;
    let mut query = relational::build(&native, &compiled, &options)?;
    let source = (0..256)
        .map(|id| {
            row(&[
                ("id", Some(&id.to_string())),
                ("auction", Some("1")),
                ("price", Some(&id.to_string())),
            ])
        })
        .collect::<Vec<_>>();
    let initial = batch(source.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    query.commit(prepared)?;
    let checkpoint = query.checkpoint()?;
    options.limits.contributions = 16;
    options.limits.resident_entries = 1;
    query = relational::build(&native, &compiled, &options)?;
    query.restore_checkpoint(checkpoint).await?;
    let replacement = row(&[("id", Some("0")), ("auction", Some("1")), ("price", Some("1000"))]);
    let delta = batch(vec![("bid", source[0].clone(), -1), ("bid", replacement, 1)]);
    let prepared = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
    let output = |sum: i64| -> Row {
        [
            ("0:auction".into(), Some("1".into())),
            ("@aggregate_0".into(), Some("256".into())),
            ("@aggregate_1".into(), Some(sum.to_string())),
        ]
        .into()
    };
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([((Vec::new(), output(32640)), -1), ((Vec::new(), output(33640)), 1)])?
    );
    query.commit(prepared)?;
    let checkpoint = query.checkpoint()?;
    let absent = row(&[("id", Some("absent")), ("auction", Some("1")), ("price", Some("1"))]);
    let inserted = row(&[("id", Some("new")), ("auction", Some("1")), ("price", Some("1"))]);
    let cancelled = batch(vec![("bid", absent, -1), ("bid", inserted, 1)]);
    assert!(query.prepare(relational::inputs(&cancelled, ir, 3)?).await.is_err());
    let invalid = batch(vec![("bid", source[1].clone(), -300)]);
    assert!(query.prepare(relational::inputs(&invalid, ir, 3)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, serde_json::to_vec(&checkpoint)?);
    Ok(())
}
