use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
const GROUPED: &str = "SELECT b.auction AS g,COUNT(*) AS n,COUNT(b.price) AS present,SUM(b.price) AS total,MIN(b.price) AS lo,MAX(b.price) AS hi,COUNT(*) FILTER(WHERE b.price<0) AS negative FROM source.bid b GROUP BY b.auction";
const WINDOW: &str = "SELECT b.id AS id,b.auction AS g,COUNT(*) OVER (PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n,SUM(b.price) OVER (PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS total FROM source.bid b";
type Bag = Batch<relational::Key, Row>;
fn oracle(state: &BTreeMap<Row, i64>, window: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        if *weight > 0 {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut outputs = Vec::new();
    for (key, mut rows) in groups {
        rows.sort_by_key(|row| row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()));
        for index in 0..if window { rows.len() } else { 1 } {
            let frame = if window {
                &rows[index.saturating_sub(1)..rows.len().min(index + 2)]
            } else {
                &rows[..]
            };
            let prices = frame
                .iter()
                .filter_map(|row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok()))
                .collect::<Vec<_>>();
            let mut output: Row = [
                ("0:auction".into(), key.clone()),
                ("@aggregate_0".into(), Some(frame.len().to_string())),
            ]
            .into();
            if window {
                output.insert("0:id".into(), rows[index]["id"].clone());
                output.insert(
                    "@aggregate_1".into(),
                    (!prices.is_empty()).then(|| prices.iter().sum::<i64>().to_string()),
                );
            } else {
                output.insert("@aggregate_1".into(), Some(prices.len().to_string()));
                output.insert(
                    "@aggregate_2".into(),
                    (!prices.is_empty()).then(|| prices.iter().sum::<i64>().to_string()),
                );
                output.insert("@aggregate_3".into(), prices.iter().min().map(ToString::to_string));
                output.insert("@aggregate_4".into(), prices.iter().max().map(ToString::to_string));
                output.insert(
                    "@aggregate_5".into(),
                    Some(prices.iter().filter(|value| **value < 0).count().to_string()),
                );
            }
            outputs.push(((Vec::new(), output), 1));
        }
    }
    Batch::from_updates(outputs)
}
async fn history(window: bool) -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let sql = if window { WINDOW } else { GROUPED };
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing partition IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", Some("7"))]);
    let changed = row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let histories = [
        batch(vec![("bid", a.clone(), 1), ("bid", b.clone(), 1), ("bid", c.clone(), 1)]),
        batch(vec![("bid", a.clone(), -1), ("bid", changed.clone(), 1)]),
        batch(vec![("bid", c, -1)]),
        batch(vec![("bid", changed, -1), ("bid", a, 1)]),
        batch(vec![("bid", b, -1)]),
        batch(vec![]),
    ];
    let mut state = BTreeMap::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&state, window)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state, window)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let candidate = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(candidate.output().batch, expected);
        relational::deltas(&candidate.output().batch, &ir.output)?;
        query.commit(candidate)?;
        if index == 2 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn grouped_null_filters_extrema_and_retractions() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn rows_frames_revise_neighbors_and_cold_restart() -> Result<()> {
    history(true).await
}
#[test]
fn partition_binding_rejects_unsupported_semantics() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    for sql in [
        WINDOW.replace("ROWS", "RANGE"),
        WINDOW.replace("ORDER BY b.id", "ORDER BY b.price"),
        WINDOW.replace("1 PRECEDING", "NULL PRECEDING"),
        WINDOW.replace("1 FOLLOWING", "2 FOLLOWING EXCLUDE CURRENT ROW"),
        GROUPED.replace("SUM(b.price)", "AVG(DISTINCT b.price)"),
        GROUPED.replace("COUNT(*) AS n", "COUNT(DISTINCT b.price) AS n"),
        GROUPED.replace("b.auction AS g", "b.id AS g"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    let first = compile(WINDOW, &native)?;
    let alias = compile(&WINDOW.replace("b.", "\"B\".").replace("bid b", "bid AS \"B\""), &native)?;
    assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&alias)?);
    Ok(())
}

#[tokio::test]
async fn accepted_frame_boundaries_match_independent_positions() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let cases = [
        ("UNBOUNDED PRECEDING AND CURRENT ROW", None, Some(0), false),
        ("CURRENT ROW AND UNBOUNDED FOLLOWING", Some(0), None, true),
        ("2 PRECEDING AND 1 PRECEDING", Some(-2), Some(-1), false),
        ("1 FOLLOWING AND 2 FOLLOWING", Some(1), Some(2), true),
        ("2 FOLLOWING AND 1 FOLLOWING", Some(2), Some(1), false),
        ("UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", None, None, true),
    ];
    let source = [(1, None), (2, Some(-7)), (3, Some(9)), (4, None)];
    let changes = batch(
        source
            .iter()
            .map(|(id, price)| {
                (
                    "bid",
                    [
                        ("id".into(), Some(id.to_string())),
                        ("auction".into(), None),
                        ("price".into(), price.map(|price| price.to_string())),
                    ]
                    .into(),
                    1,
                )
            })
            .collect(),
    );
    for (frame, start, end, descending) in cases {
        let direction = if descending { "DESC NULLS FIRST" } else { "ASC NULLS LAST" };
        let sql = format!(
            "SELECT b.id,COUNT(*) OVER (ORDER BY b.price {direction},b.id ROWS BETWEEN {frame}) FROM source.bid b"
        );
        let compiled = compile(&sql, &native)?;
        let ir = compiled.relational().context("missing IR")?;
        let mut query = relational::build(&native, &compiled, &settings())?;
        let mut order = source.to_vec();
        order.sort_by_key(|(id, price)| {
            if descending {
                (price.is_some(), -price.unwrap_or(0), *id)
            } else {
                (price.is_none(), price.unwrap_or(0), *id)
            }
        });
        let expected = Batch::from_updates(order.iter().enumerate().map(|(index, (id, _))| {
            let left =
                start.map_or(0, |offset| (i64::try_from(index).unwrap_or(0) + offset).clamp(0, 4));
            let right = end
                .map_or(4, |offset| (i64::try_from(index).unwrap_or(0) + offset + 1).clamp(0, 4));
            let output = [
                ("0:id".into(), Some(id.to_string())),
                ("@aggregate_0".into(), Some((right - left).max(0).to_string())),
            ]
            .into();
            ((Vec::new(), output), 1)
        }))?;
        let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
        assert_eq!(prepared.output().batch, expected, "{sql}");
        query.commit(prepared)?;
    }
    Ok(())
}
#[tokio::test]
async fn window_resource_failure_preserves_root_and_time() -> Result<()> {
    let native = contract();
    let compiled = compile(&WINDOW.replace("SUM(b.price)", "COUNT(b.price)"), &native)?;
    let ir = compiled.relational().context("missing IR")?;
    let mut options = settings();
    options.limits.output_bytes = 200;
    let query = relational::build(&native, &compiled, &options)?;
    let before = query.checkpoint()?;
    let value = row(&[("id", Some("1")), ("auction", None), ("price", Some("2"))]);
    let weighted = batch(vec![("bid", value, 100)]);
    assert!(query.prepare(relational::inputs(&weighted, ir, 1)?).await.is_err());
    assert_eq!(query.time(), 0);
    assert_eq!(query.checkpoint()?, before);
    let mut options = settings();
    options.limits.contributions = 3;
    let query = relational::build(&native, &compiled, &options)?;
    let before = query.checkpoint()?;
    let rows = batch(vec![
        ("bid", row(&[("id", Some("1")), ("auction", None), ("price", None)]), 1),
        ("bid", row(&[("id", Some("2")), ("auction", Some("2")), ("price", None)]), 1),
    ]);
    assert!(query.prepare(relational::inputs(&rows, ir, 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[tokio::test]
async fn integer_numeric_statistics_preserve_exact_weighted_state() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction,AVG(b.price),SUM(b.price) FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    assert!(compiled.validate_bound().is_err());
    let ir = compiled.relational().context("missing numeric IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let large =
        row(&[("id", Some("1")), ("auction", None), ("price", Some("9223372036854775807"))]);
    let negative = row(&[("id", Some("2")), ("auction", None), ("price", Some("-1"))]);
    let null = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let changes = batch(vec![
        ("bid", large.clone(), 2),
        ("bid", negative.clone(), 3),
        ("bid", null.clone(), 1),
    ]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    let total = num_bigint::BigInt::from(i64::MAX) * 2_i32 - 3_i32;
    let expected: Row = [
        ("0:auction".into(), None),
        ("@aggregate_0".into(), Some(format!("{total}/5"))),
        ("@aggregate_1".into(), Some(total.to_string())),
    ]
    .into();
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([((Vec::new(), expected.clone()), 1)])?
    );
    relational::deltas(&candidate.output().batch, &ir.output)?;
    query.commit(candidate)?;
    let checkpoint = query.checkpoint()?;
    query = relational::build(&native, &compiled, &options)?;
    query.restore_checkpoint(checkpoint).await?;
    let changes = batch(vec![("bid", large, -2), ("bid", negative, -3)]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    let empty: Row =
        [("0:auction".into(), None), ("@aggregate_0".into(), None), ("@aggregate_1".into(), None)]
            .into();
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([((Vec::new(), expected), -1), ((Vec::new(), empty), 1)])?
    );
    query.commit(candidate)?;
    let before = serde_json::to_vec(&query.checkpoint()?)?;
    let changes = batch(vec![("bid", null, -2)]);
    assert!(query.prepare(relational::inputs(&changes, ir, 3)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, before);
    Ok(())
}
