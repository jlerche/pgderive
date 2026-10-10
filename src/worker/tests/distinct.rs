//! Distinct cardinality oracle counts value presence, not source occurrences.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
const SQL: &str = "SELECT b.auction AS g,COUNT(*) AS rows,COUNT(DISTINCT b.price) AS values,COUNT(DISTINCT b.price) FILTER(WHERE b.price<0) AS negative FROM source.bid b GROUP BY b.auction";
type Counts = (i64, BTreeSet<i64>);
fn oracle(state: &BTreeMap<Row, i64>) -> Result<Batch<relational::Key, Row>> {
    let mut groups: BTreeMap<Option<String>, Counts> = BTreeMap::new();
    for (row, weight) in state.iter().filter(|(_, weight)| **weight > 0) {
        let group = groups.entry(row["auction"].clone()).or_default();
        group.0 += weight;
        if let Some(value) = &row["price"] {
            group.1.insert(value.parse()?);
        }
    }
    Batch::from_updates(groups.into_iter().map(|(key, (count, values))| {
        let output = row(&[
            ("0:auction", key.as_deref()),
            ("@aggregate_0", Some(&count.to_string())),
            ("@aggregate_1", Some(&values.len().to_string())),
            ("@aggregate_2", Some(&values.iter().filter(|value| **value < 0).count().to_string())),
        ]);
        ((Vec::new(), output), 1)
    }))
}
#[tokio::test]
async fn distinct_retracts_only_the_last_occurrence_and_restores() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational().context("missing distinct IR")?;
    assert!(
        compiled
            .revision
            .as_deref()
            .is_some_and(|value| value.contains("pg-integral-distinct-count-v1"))
    );
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("-2"))]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("auction", Some("9")), ("price", Some("7"))]);
    let mut state = BTreeMap::new();
    for changes in [
        batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 1), ("bid", c.clone(), 1)]),
        batch(vec![("bid", a.clone(), -1)]),
        batch(vec![("bid", b.clone(), -1), ("bid", moved.clone(), 1)]),
        batch(vec![("bid", a.clone(), -1)]),
        batch(vec![("bid", moved.clone(), -1), ("bid", b.clone(), 1)]),
        batch(vec![("bid", c.clone(), -1), ("bid", b.clone(), -1)]),
        batch(vec![]),
    ] {
        let before = oracle(&state)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state)?;
        let delta = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, delta);
        query.commit(prepared)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint).await?;
    }
    let prior = serde_json::to_vec(&query.checkpoint()?)?;
    let changes = batch(vec![("bid", a, -1)]);
    assert!(query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, prior);
    Ok(())
}
#[test]
fn distinct_rejects_unqualified_types_modifiers_and_window_forms() -> Result<()> {
    let native = contract();
    for query in [
        "SELECT b.auction,COUNT(DISTINCT b.price) OVER(PARTITION BY b.auction) FROM source.bid b GROUP BY b.auction",
        "SELECT b.auction,SUM(DISTINCT b.price) FROM source.bid b GROUP BY b.auction",
        "SELECT b.auction,COUNT(DISTINCT b.price,b.id) FROM source.bid b GROUP BY b.auction",
        "SELECT b.auction,COUNT(DISTINCT b.price ORDER BY b.price) FROM source.bid b GROUP BY b.auction",
        "SELECT b.auction,COUNT(DISTINCT b.price+1) FROM source.bid b GROUP BY b.auction",
        "SELECT a.id,COUNT(DISTINCT a.category) FROM source.auction a GROUP BY a.id",
    ] {
        assert!(compile(query, &native).is_err(), "{query}");
    }
    for oid in [16, 1114, 1184, 1700, 2950] {
        let mut changed = contract();
        changed.relations[1].columns[2].oid = oid;
        assert!(compile(SQL, &changed).is_err());
    }
    assert_ne!(
        serde_json::to_vec(&compile(SQL, &native)?)?,
        serde_json::to_vec(&compile(&SQL.replace("COUNT(DISTINCT", "COUNT("), &native)?)?
    );
    Ok(())
}
