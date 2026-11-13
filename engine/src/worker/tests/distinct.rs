//! Distinct aggregate oracle over integral value membership.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
const SQL: &str = "SELECT b.auction AS g,COUNT(*) AS rows,COUNT(DISTINCT b.price) AS values,COUNT(DISTINCT b.price) FILTER(WHERE b.price<0) AS negative,SUM(DISTINCT b.price) AS total,AVG(DISTINCT b.price) FILTER(WHERE b.price<0) AS mean FROM source.bid b GROUP BY b.auction";
type Counts = (i64, BTreeSet<i64>);
fn oracle(state: &BTreeMap<Row, i64>, mean_field: &str) -> Result<Batch<relational::Key, Row>> {
    let mut groups: BTreeMap<Option<String>, Counts> = BTreeMap::new();
    for (row, weight) in state.iter().filter(|(_, weight)| **weight > 0) {
        let group = groups.entry(row["auction"].clone()).or_default();
        group.0 += weight;
        if let Some(value) = &row["price"] {
            group.1.insert(value.parse()?);
        }
    }
    Batch::from_updates(groups.into_iter().map(|(key, (count, values))| {
        let negatives = values.iter().copied().filter(|value| *value < 0).collect::<Vec<_>>();
        let total = (!values.is_empty()).then(|| values.iter().sum::<i64>().to_string());
        let mean = (!negatives.is_empty()).then_some("-2.0000000000000000");
        let output = row(&[
            ("0:auction", key.as_deref()),
            ("@aggregate_0", Some(&count.to_string())),
            ("@aggregate_1", Some(&values.len().to_string())),
            ("@aggregate_2", Some(&negatives.len().to_string())),
            ("@aggregate_3", total.as_deref()),
            (mean_field, mean),
        ]);
        ((crate::worker::program::relational::Key::new(), output), 1)
    }))
}
#[tokio::test]
async fn distinct_retracts_only_the_last_occurrence_and_restores() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational();

    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("-2"))]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("auction", Some("9")), ("price", Some("7"))]);
    let query = super::check_row_history(
        (&native, &compiled, &options),
        [
            batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 1), ("bid", c.clone(), 1)]),
            batch(vec![("bid", a.clone(), -1)]),
            batch(vec![("bid", b.clone(), -1), ("bid", moved.clone(), 1)]),
            batch(vec![("bid", a.clone(), -1)]),
            batch(vec![("bid", moved.clone(), -1), ("bid", b.clone(), 1)]),
            batch(vec![("bid", c.clone(), -1), ("bid", b.clone(), -1)]),
            batch(vec![]),
        ],
        |state| oracle(state, &ir.output.columns[5].column.name),
        |_| true,
    )
    .await?;
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
        "SELECT b.auction,SUM(DISTINCT b.price::numeric) FROM source.bid b GROUP BY b.auction",
        "SELECT MIN(DISTINCT 1.0)",
        "SELECT AVG(DISTINCT 1) OVER()",
        "SELECT bool_or(DISTINCT true) OVER()",
        "SELECT b.auction,COUNT(DISTINCT b.price,b.id) FROM source.bid b GROUP BY b.auction",
        "SELECT b.auction,COUNT(DISTINCT b.price ORDER BY b.price) FROM source.bid b GROUP BY b.auction",
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

#[tokio::test]
async fn unread_distinct_arguments_do_not_run_in_derived_groups() -> Result<()> {
    let native = contract();
    let options = settings();
    let changes = batch(vec![(
        "bid",
        row(&[("id", Some("1")), ("auction", Some("7")), ("price", Some("0"))]),
        1,
    )]);
    for retained in ["", ",MIN(price) AS minimum"] {
        let sql = format!(
            "SELECT q.g,q.total FROM (SELECT auction AS g,SUM(DISTINCT 10 / price) AS unused,
             SUM(DISTINCT price) AS total{retained} FROM source.bid GROUP BY auction) q"
        );
        let compiled = compile(&sql, &native)?;
        let worker = relational::build(&native, &compiled, &options)?;
        let prepared =
            worker.prepare(relational::inputs(&changes, compiled.relational(), 1)?).await?;
        let columns = &compiled.relational().output.columns;
        let expected =
            row(&[(&columns[0].column.name, Some("7")), (&columns[1].column.name, Some("0"))]);
        assert_eq!(
            prepared.output().batch,
            Batch::from_updates([((relational::Key::new(), expected), 1)])?
        );
    }
    Ok(())
}
