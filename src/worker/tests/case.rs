//! Independent weighted occurrence oracle for lazy boundary flags.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
fn sql(running: bool) -> String {
    let inner = "SELECT b.id,b.auction,b.price,lag(b.price) OVER(PARTITION BY b.auction ORDER BY b.price NULLS FIRST,b.id) AS previous FROM source.bid b";
    let flags = format!(
        "SELECT q.id,q.auction,q.price,CASE WHEN q.previous IS NULL THEN 1 WHEN q.price=q.previous THEN 0 WHEN q.price-q.previous>=INTERVAL '10 seconds' THEN 1 ELSE 0 END AS flag FROM({inner}) q"
    );
    if running {
        format!(
            "SELECT z.id,z.auction,z.price,SUM(z.flag) OVER(PARTITION BY z.auction ORDER BY z.price NULLS FIRST,z.id ROWS UNBOUNDED PRECEDING) AS session FROM({flags}) z"
        )
    } else {
        flags
    }
}
fn instant(value: &str) -> Result<i64> {
    match value {
        "-infinity" => Ok(i64::MIN),
        "infinity" => Ok(i64::MAX),
        _ => Ok(value.get(17..).context("invalid oracle time")?.parse::<i64>()? * 1_000_000),
    }
}
type Occurrence = (Option<i64>, Row);
fn ordered(rows: Vec<Row>) -> Result<Vec<Occurrence>> {
    let mut values = rows
        .into_iter()
        .map(|row| {
            let time = row["price"].as_deref().map(instant).transpose()?;
            let id: i64 = row["id"].as_deref().context("missing oracle id")?.parse()?;
            Ok((time, id, row))
        })
        .collect::<Result<Vec<_>>>()?;
    values.sort_by_key(|(time, id, _)| (*time, *id));
    Ok(values.into_iter().map(|(time, _, row)| (time, row)).collect())
}

fn oracle(
    state: &BTreeMap<Row, i64>,
    field: &str,
    running: bool,
) -> Result<Batch<relational::Key, Row>> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut outputs = Vec::new();
    for rows in groups.into_values() {
        let mut previous = None;
        let mut sum = 0;
        for (time, value) in ordered(rows)? {
            let flag = match (previous, time) {
                (None, _) => 1,
                (Some(a), Some(b)) if a == b => 0,
                (Some(a), Some(b)) if i128::from(b) - i128::from(a) >= 10_000_000 => 1,
                _ => 0,
            };
            previous = time;
            sum += flag;
            let mut output: Row = value
                .into_iter()
                .filter(|(name, _)| name != "previous")
                .map(|(name, value)| (format!("0:{name}"), value))
                .collect();
            output.insert(field.into(), Some(if running { sum } else { flag }.to_string()));
            outputs.push(((Vec::new(), output), 1));
        }
    }
    Batch::from_updates(outputs)
}
async fn history(running: bool) -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    let compiled = compile(&sql(running), &native)?;
    let ir = compiled.relational().context("missing CASE IR")?;
    let field = &ir.output.columns[3].column.name;
    assert!(
        compiled
            .revision
            .as_deref()
            .is_some_and(|revision| revision.contains("pg-timestamp-gap-v1"))
    );
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let null_time = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let negative = row(&[("id", Some("2")), ("auction", None), ("price", Some("-infinity"))]);
    let first =
        row(&[("id", Some("3")), ("auction", None), ("price", Some("2000-01-01 00:00:10"))]);
    let later =
        row(&[("id", Some("4")), ("auction", None), ("price", Some("2000-01-01 00:00:20"))]);
    let positive = row(&[("id", Some("5")), ("auction", None), ("price", Some("infinity"))]);
    let bridge =
        row(&[("id", Some("6")), ("auction", None), ("price", Some("2000-01-01 00:00:15"))]);
    let mut state = BTreeMap::new();
    for (index, changes) in [
        batch(vec![
            ("bid", null_time.clone(), 1),
            ("bid", negative.clone(), 2),
            ("bid", first.clone(), 1),
            ("bid", later.clone(), 1),
            ("bid", positive.clone(), 2),
        ]),
        batch(vec![("bid", bridge.clone(), 1), ("bid", null_time.clone(), -1)]),
        batch(vec![("bid", bridge, -1), ("bid", negative.clone(), -1)]),
        batch(vec![
            ("bid", negative, -1),
            ("bid", first, -1),
            ("bid", later, -1),
            ("bid", positive, -2),
        ]),
    ]
    .into_iter()
    .enumerate()
    {
        let before = oracle(&state, field, running)?;
        for update in &changes.updates {
            *state.entry(update.tuple.row.clone()).or_default() += update.weight;
        }
        let after = oracle(&state, field, running)?;
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
async fn lazy_gap_flags_preserve_signed_occurrence_bags() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn running_boundary_totals_merge_split_and_restore() -> Result<()> {
    history(true).await
}
#[test]
fn case_rejects_unsupported_arms_and_gap_types() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    for invalid in [
        sql(false).replace("THEN 1", "THEN 'x'"),
        sql(false).replace("CASE WHEN", "CASE q.price WHEN"),
        sql(false).replace("q.price-q.previous", "q.id-q.auction"),
        sql(false).replace("INTERVAL '10 seconds'", "INTERVAL '1 month'"),
        sql(false).replace("INTERVAL '10 seconds'", "INTERVAL '0 seconds'"),
        sql(false)
            .replace("THEN 1", "THEN NULL")
            .replace("THEN 0", "THEN NULL")
            .replace("ELSE 0", "ELSE NULL"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    assert_ne!(
        serde_json::to_vec(&compile(&sql(false), &native)?)?,
        serde_json::to_vec(&compile(&sql(false).replace("10 seconds", "11 seconds"), &native)?)?
    );
    Ok(())
}

#[tokio::test]
async fn gap_errors_discard_all_staged_state_before_retry() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    let unsafe_sql = sql(false).replace("WHEN q.price=q.previous THEN 0 ", "");
    let compiled = compile(&unsafe_sql, &native)?;
    let ir = compiled.relational().context("missing gap IR")?;
    for (initial, bad_values, retry_value) in [
        ("2000-01-01 00:00:00", vec!["infinity", "infinity"], "infinity"),
        ("4714-11-24 00:00:00 BC", vec!["294276-12-31 23:59:59.999999"], "2000-01-01 00:00:00"),
    ] {
        let options = settings();
        let mut query = relational::build(&native, &compiled, &options)?;
        let first = row(&[("id", Some("1")), ("auction", None), ("price", Some(initial))]);
        let changes = batch(vec![("bid", first, 1)]);
        let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
        query.commit(prepared)?;
        let prior = serde_json::to_vec(&query.checkpoint()?)?;
        let changes = batch(
            bad_values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    (
                        "bid",
                        row(&[
                            ("id", Some(&(index + 2).to_string())),
                            ("auction", None),
                            ("price", Some(value)),
                        ]),
                        1,
                    )
                })
                .collect(),
        );
        let error = query
            .prepare(relational::inputs(&changes, ir, 2)?)
            .await
            .err()
            .context("gap must fail")?;
        assert!(format!("{error:#}").contains("interval out of range"), "{error:#}");
        assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, prior);
        let retry = row(&[("id", Some("2")), ("auction", None), ("price", Some(retry_value))]);
        let changes = batch(vec![("bid", retry, 1)]);
        let prepared = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
        query.commit(prepared)?;
        assert_eq!(query.time(), 2);
    }
    Ok(())
}
