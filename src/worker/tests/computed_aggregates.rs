//! Computed operands preserve FILTER laziness and full source multiplicity.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{compile, relational::Node},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
type Bag = Batch<relational::Key, Row>;
const SQL: &str = "SELECT auction,COUNT(abs(price)),SUM(abs(price)),AVG(abs(price)),COUNT(DISTINCT abs(price)) FROM source.bid WHERE id>0 GROUP BY auction";
fn oracle(state: &BTreeMap<Row, i64>) -> Result<Bag> {
    let mut groups = BTreeMap::<Option<String>, (i64, i128, BTreeSet<i64>)>::new();
    for (row, weight) in
        state.iter().filter(|(row, weight)| **weight > 0 && row["id"].as_deref() != Some("0"))
    {
        let group = groups.entry(row["auction"].clone()).or_default();
        if let Some(price) = &row["price"] {
            let price = price.parse::<i64>()?.abs();
            group.0 += weight;
            group.1 += i128::from(price) * i128::from(*weight);
            group.2.insert(price);
        }
    }
    Batch::from_updates(groups.into_iter().map(|(key, (count, sum, distinct))| {
        let output = row(&[
            ("0:auction", key.as_deref()),
            ("@aggregate_0", Some(&count.to_string())),
            ("@aggregate_1", (count != 0).then(|| sum.to_string()).as_deref()),
            ("@aggregate_2", (count != 0).then(|| format!("{sum}/{count}")).as_deref()),
            ("@aggregate_3", Some(&distinct.len().to_string())),
        ]);
        ((Vec::new(), output), 1)
    }))
}
#[tokio::test]
async fn computed_distinct_and_statistics_match_memory_with_collisions_and_restart() -> Result<()> {
    let native = contract();
    let compiled = super::raw_statistics_compiled(compile(SQL, &native)?);
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("2"))]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("7"))]);
    let excluded =
        row(&[("id", Some("0")), ("auction", Some("2")), ("price", Some("-9223372036854775808"))]);
    let histories = [
        vec![
            ("bid", a.clone(), 3),
            ("bid", b.clone(), 1),
            ("bid", c.clone(), 1),
            ("bid", excluded.clone(), 1),
        ],
        vec![("bid", a.clone(), -2), ("bid", b, -1), ("bid", moved.clone(), 1)],
        vec![("bid", a, -1)],
        vec![("bid", moved, -1), ("bid", c, -1), ("bid", excluded, -1)],
    ];
    let mut state = BTreeMap::new();
    for updates in histories {
        let before = oracle(&state)?;
        let delta = batch(updates);
        super::apply_rows(&mut state, &delta);
        let expected = super::bag_delta(&before, &oracle(&state)?)?;
        let prepared = query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
#[tokio::test]
async fn filters_guard_errors_and_keep_empty_groups_in_all_aggregate_modes() -> Result<()> {
    for aggregates in [
        "COUNT(abs(price)),SUM(abs(price)),AVG(abs(price))",
        "COUNT(DISTINCT abs(price)),COUNT(abs(price))",
        "MIN(abs(price)),MAX(abs(price))",
        "MIN(abs(price)),COUNT(abs(price))",
    ] {
        let aggregates = aggregates
            .split(',')
            .map(|value| format!("{value} FILTER(WHERE price>0)"))
            .collect::<Vec<_>>()
            .join(",");
        let native = contract();
        let compiled = compile(
            &format!("SELECT auction,{aggregates} FROM source.bid GROUP BY auction"),
            &native,
        )?;
        let options = settings();
        let mut query = relational::build(&native, &compiled, &options)?;
        let excluded = row(&[
            ("id", Some("1")),
            ("auction", Some("1")),
            ("price", Some("-9223372036854775808")),
        ]);
        let unknown = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
        let delta = batch(vec![("bid", excluded.clone(), 1), ("bid", unknown.clone(), 1)]);
        let prepared = query.prepare(relational::inputs(&delta, compiled.relational(), 1)?).await?;
        assert_eq!(prepared.output().batch.iter().count(), 1);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
        let delta = batch(vec![("bid", excluded, -1), ("bid", unknown, -1)]);
        let prepared = query.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await?;
        assert_eq!(prepared.output().batch.iter().count(), 1);
        query.commit(prepared)?;
    }
    Ok(())
}
#[tokio::test]
async fn included_error_does_not_change_durable_state() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let query = relational::build(&native, &compiled, &settings())?;
    let prior = serde_json::to_vec(&query.checkpoint()?)?;
    let delta = batch(vec![(
        "bid",
        row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("-9223372036854775808"))]),
        1,
    )]);
    assert!(query.prepare(relational::inputs(&delta, compiled.relational(), 1)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, prior);
    assert_eq!(query.time(), 0);
    Ok(())
}
#[test]
fn aggregate_maps_intern_by_scope_expression_and_filter() -> Result<()> {
    let native = contract();
    for (operands, expected) in [
        ("SUM(abs(price)),COUNT(abs(price)),AVG(abs(price))", 2),
        ("SUM(abs(price)) FILTER(WHERE price>0),COUNT(abs(price)) FILTER(WHERE price>0)", 1),
        (
            "SUM(abs(price)) FILTER(WHERE price>0),COUNT(abs(price)) FILTER(WHERE price<0),COUNT(abs(price))",
            3,
        ),
    ] {
        let compiled = compile(
            &format!("SELECT auction,{operands} FROM source.bid GROUP BY auction"),
            &native,
        )?;
        let fields: usize = compiled
            .relational()
            .nodes
            .iter()
            .filter_map(|node| match node {
                Node::Map { computed, .. } => Some(computed.len()),
                _ => None,
            })
            .sum();
        assert_eq!(fields, expected);
    }
    for sql in [
        "SELECT auction,SUM(NULL) FROM source.bid GROUP BY auction",
        "SELECT auction,SUM(lower(price)) FROM source.bid GROUP BY auction",
        "SELECT auction,SUM(SUM(price)) FROM source.bid GROUP BY auction",
        "SELECT SUM(abs(price)) FROM source.bid",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    for sql in [
        "SELECT abs(auction),COUNT(NULL),COUNT(true),COUNT('x'),COUNT(NULL::bigint) FROM source.bid GROUP BY abs(auction)",
        "SELECT g,SUM(abs(v)) FROM (SELECT auction AS g,MAX(price) AS v FROM source.bid GROUP BY auction) d GROUP BY g",
    ] {
        compile(sql, &native)?.validate_execution()?;
    }
    Ok(())
}

#[tokio::test]
async fn root_average_finalizes_nonterminating_nulls_retractions_and_cold_reopen() -> Result<()> {
    use crate::compiler::Cell;
    let native = contract();
    let compiled =
        compile("SELECT auction,AVG(price) AS value FROM source.bid GROUP BY auction", &native)?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let one = row(&[("id", Some("1")), ("auction", None), ("price", Some("1"))]);
    let zero = row(&[("id", Some("2")), ("auction", None), ("price", Some("0"))]);
    let null = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let third = Some(Cell::Text("0.33333333333333333333".into()));
    for (changes, expected) in [
        (
            vec![("bid", one.clone(), 1), ("bid", zero.clone(), 2), ("bid", null.clone(), 1)],
            vec![(((), vec![None, third.clone()]), 1)],
        ),
        (
            vec![("bid", one, -1), ("bid", zero, -2)],
            vec![(((), vec![None, third]), -1), (((), vec![None, None]), 1)],
        ),
        (vec![("bid", null, -1)], vec![(((), vec![None, None]), -1)]),
    ] {
        let delta = batch(changes);
        let prepared = query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await?;
        assert_eq!(
            super::projected_batch(&prepared.output().batch, &compiled)?,
            Batch::from_updates(expected)?
        );
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
