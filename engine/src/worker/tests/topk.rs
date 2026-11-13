//! Compiled ordered occurrence selection must preserve SQL bags through restart.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{compile, relational::Node},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::Result;
use std::collections::BTreeMap;
fn query(condition: &str) -> String {
    format!(
        "SELECT q.auction,q.price FROM(SELECT b.id,b.auction,b.price,ROW_NUMBER() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS LAST,b.id) AS rn FROM source.bid b) q WHERE {condition}"
    )
}
#[test]
fn top_k_specialization_requires_one_bounded_occurrence_cutoff() -> Result<()> {
    for (condition, expected) in [
        ("q.rn<=3", true),
        ("3>=q.rn", true),
        ("q.rn<4", true),
        ("q.rn=3", true),
        ("q.rn<=0", true),
        ("q.rn<=1025", false),
        ("q.rn>3", false),
        ("q.rn<=3 AND q.price>0", false),
    ] {
        let compiled = compile(&query(condition), &contract())?;
        assert_eq!(
            compiled.relational().nodes.iter().any(|node| matches!(node, Node::TopK { .. })),
            expected,
            "{condition}"
        );
    }
    let ranked = compile(&query("q.rn<=3").replace("ROW_NUMBER()", "RANK()"), &contract())?;
    assert!(!ranked.relational().nodes.iter().any(|node| matches!(node, Node::TopK { .. })));
    Ok(())
}
fn oracle(
    state: &BTreeMap<Row, i64>,
    limit: usize,
    equality: bool,
) -> Result<Batch<relational::Key, Row>> {
    let mut groups = BTreeMap::<Option<String>, Vec<Row>>::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut expected = Vec::new();
    for mut rows in groups.into_values() {
        let parsed = |row: &Row| row["price"].as_ref().and_then(|text| text.parse::<i64>().ok());
        rows.sort_by_key(|row| {
            (
                parsed(row).is_none(),
                parsed(row).map(std::cmp::Reverse),
                row["id"].as_ref().and_then(|text| text.parse::<i64>().ok()),
            )
        });
        for (index, row) in rows.into_iter().enumerate().take(limit) {
            if !equality || index + 1 == limit {
                expected.push((
                    (
                        crate::worker::program::relational::Key::new(),
                        [
                            ("0:auction".into(), row["auction"].clone()),
                            ("0:price".into(), row["price"].clone()),
                        ]
                        .into(),
                    ),
                    1,
                ));
            }
        }
    }
    Batch::from_updates(expected)
}
#[tokio::test]
async fn compiled_top_k_weighted_cutoff_retractions_and_cold_reopen() -> Result<()> {
    for condition in ["q.rn<=3", "q.rn=3", "q.rn<=0"] {
        history(condition).await?;
    }
    Ok(())
}
async fn history(condition: &str) -> Result<()> {
    let native = contract();
    let options = settings();
    let compiled = compile(&query(condition), &native)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("7"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("7"))]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("-9"))]);
    let limit = if condition.ends_with('0') { 0 } else { 3 };
    let equality = condition == "q.rn=3";
    super::History::new((&native, &compiled, &options))
        .run(
            [
                batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 3), ("bid", c.clone(), 1)]),
                batch(vec![("bid", b, -3), ("bid", moved.clone(), 3)]),
                batch(vec![("bid", a.clone(), -1), ("bid", c.clone(), -1)]),
                batch(vec![("bid", a, -1), ("bid", moved, -3)]),
            ],
            |state: &super::RowState| oracle(state, limit, equality),
            super::apply_rows,
            |index| index == 1,
        )
        .await?;

    Ok(())
}
