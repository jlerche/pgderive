//! Independent recomputation for multiple retained SQL stages.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
fn sql(layered: bool) -> String {
    let grouped = "SELECT b.auction AS g,COUNT(*) AS n FROM source.bid b WHERE b.price IS NULL OR b.price>0 GROUP BY b.auction";
    let window = format!("SELECT q.g,q.n,MAX(q.n) OVER() AS m FROM({grouped}) q WHERE q.n>=2");
    if layered {
        format!("SELECT z.m,COUNT(*) AS winners FROM({window}) z WHERE z.n=z.m GROUP BY z.m")
    } else {
        window
    }
}
fn oracle(state: &BTreeMap<Row, i64>, layered: bool) -> Result<Bag> {
    let mut counts: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for (row, weight) in state {
        if row["price"]
            .as_ref()
            .is_none_or(|value| value.parse::<i64>().is_ok_and(|price| price > 0))
        {
            *counts.entry(row["auction"].clone()).or_default() += weight;
        }
    }
    counts.retain(|_, count| *count >= 2);
    let Some(maximum) = counts.values().max().copied() else {
        return Batch::from_updates([]);
    };
    let outputs = if layered {
        vec![(
            row(&[
                ("@aggregate_1", Some(&maximum.to_string())),
                (
                    "@aggregate_2",
                    Some(&counts.values().filter(|count| **count == maximum).count().to_string()),
                ),
            ]),
            1,
        )]
    } else {
        counts
            .into_iter()
            .map(|(group, count)| {
                (
                    [
                        ("0:auction".into(), group),
                        ("@aggregate_0".into(), Some(count.to_string())),
                        ("@aggregate_1".into(), Some(maximum.to_string())),
                    ]
                    .into(),
                    1,
                )
            })
            .collect()
    };
    Batch::from_updates(outputs.into_iter().map(|(row, weight)| ((Vec::new(), row), weight)))
}
async fn history(layered: bool) -> Result<()> {
    let native = contract();
    let query_sql = sql(layered);
    let compiled = compile(&query_sql, &native)?;
    let ir = compiled.relational().context("missing composed IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let b = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("7"))]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", Some("-2"))]);
    let moved = row(&[("id", Some("2")), ("auction", None), ("price", Some("7"))]);
    let mut state = BTreeMap::new();
    for (index, changes) in [
        batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 3), ("bid", c.clone(), 1)]),
        batch(vec![("bid", b, -3), ("bid", moved.clone(), 3)]),
        batch(vec![("bid", a.clone(), -1), ("bid", c, -1)]),
        batch(vec![("bid", moved, -3), ("bid", a, -1)]),
    ]
    .into_iter()
    .enumerate()
    {
        let before = oracle(&state, layered)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state, layered)?;
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
async fn grouped_then_windowed_deltas_preserve_scope_filters() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn grouped_window_grouped_restarts_all_retained_stages() -> Result<()> {
    history(true).await
}
#[test]
fn composed_scopes_reject_hidden_ambiguous_and_deferred_inputs() -> Result<()> {
    let native = contract();
    for invalid in [
        sql(false).replace("MAX(q.n)", "MAX(b.price)"),
        sql(false).replace("q.n>=2", "q.price>0"),
        sql(false).replace("b.auction AS g", "b.auction AS n"),
        sql(false).replace("COUNT(*) AS n", "SUM(b.price) AS n"),
        sql(false).replace("MAX(q.n) OVER()", "row_number() OVER(ORDER BY q.n)"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    assert_ne!(
        serde_json::to_vec(&compile(&sql(false), &native)?)?,
        serde_json::to_vec(&compile(&sql(false).replace(">=2", ">=3"), &native)?)?
    );
    let quoted = sql(true).replace("q.", "\"Q\".").replace(") q WHERE", ") AS \"Q\" WHERE");
    assert_eq!(
        serde_json::to_vec(&compile(&sql(true), &native)?)?,
        serde_json::to_vec(&compile(&quoted, &native)?)?
    );
    Ok(())
}

#[tokio::test]
async fn repeated_date_bin_maps_keep_scope_fields_distinct() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    let bin = "date_bin(INTERVAL '10 seconds',b.price,TIMESTAMP '2000-01-01 00:00:00')";
    let inner = format!("SELECT b.id,b.price,{bin} AS bucket FROM source.bid b");
    let outer = bin.replace("b.", "q.");
    let sql = format!("SELECT {outer} AS bucket,COUNT(*) AS n FROM({inner}) q GROUP BY {outer}");
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational().context("missing bin IR")?;
    let names = ir
        .nodes
        .iter()
        .filter_map(|node| match node {
            crate::compiler::relational::Node::Map { computed, .. } => {
                Some(computed[0].column.name.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 2);
    assert_ne!(names[0], names[1]);
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("2000-01-01 00:00:01"))]);
    let changes = batch(vec![("bid", a.clone(), 2)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    let expected = Batch::from_updates([(
        (
            Vec::new(),
            [
                (names[1].into(), Some("2000-01-01 00:00:00".into())),
                ("@aggregate_0".into(), Some("2".into())),
            ]
            .into(),
        ),
        1,
    )])?;
    assert_eq!(prepared.output().batch, expected);
    query.commit(prepared)?;
    let checkpoint = query.checkpoint()?;
    query = relational::build(&native, &compiled, &options)?;
    query.restore_checkpoint(checkpoint).await?;
    let changes = batch(vec![("bid", a, -2)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates(expected.iter().map(|(tuple, weight)| (tuple.clone(), -*weight)))?
    );
    Ok(())
}
