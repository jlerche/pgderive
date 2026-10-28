//! Side-local ON maps retain collisions and simultaneous-input cross terms.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{compile, relational::Node},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::Result;
use std::collections::BTreeMap;
const SQL: &str =
    "SELECT a.category,b.price FROM source.auction a JOIN source.bid b ON abs(a.id)=abs(b.auction)";
fn oracle(
    left: &BTreeMap<Row, i64>,
    right: &BTreeMap<Row, i64>,
) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (a, aw) in left.iter().filter(|(_, weight)| **weight > 0) {
        for (b, bw) in right.iter().filter(|(_, weight)| **weight > 0) {
            if let Some(key) = &b["auction"]
                && a["id"].as_deref().map(str::parse::<i32>).transpose()?.map(i32::abs)
                    == Some(key.parse::<i32>()?.abs())
            {
                updates.push((
                    (
                        crate::worker::program::relational::Key::new(),
                        row(&[
                            ("0:category", a["category"].as_deref()),
                            ("1:price", b["price"].as_deref()),
                        ]),
                    ),
                    aw * bw,
                ));
            }
        }
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn computed_join_histories_match_full_source_oracle_and_restart() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let options = settings();
    let a = row(&[("id", Some("1")), ("category", Some("same")), ("other", None)]);
    let a2 = row(&[("id", Some("-1")), ("category", Some("same")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("-1")), ("price", Some("7"))]);
    let b2 = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("category", Some("moved")), ("other", None)]);
    let changed = row(&[("id", Some("1")), ("auction", Some("-2")), ("price", Some("7"))]);
    let query = super::History::new((&native, &compiled, &options))
        .run(
            [
                batch(vec![
                    ("auction", a.clone(), 2),
                    ("auction", a2.clone(), 1),
                    ("bid", b.clone(), 3),
                    ("bid", b2.clone(), 1),
                ]),
                batch(vec![
                    ("auction", a.clone(), -2),
                    ("auction", moved.clone(), 2),
                    ("bid", b, -3),
                    ("bid", changed.clone(), 3),
                ]),
                batch(vec![("auction", a2, -1), ("bid", b2, -1)]),
                batch(vec![("auction", moved, -2), ("bid", changed, -3)]),
            ],
            |state: &super::JoinedState| oracle(&state.0, &state.1),
            super::apply_joined,
            |_| true,
        )
        .await?;

    // Both inputs are empty: eager ON-map errors still preserve the last boundary.
    let before = query.checkpoint()?;
    let overflow = batch(vec![(
        "bid",
        row(&[("id", Some("99")), ("auction", Some("-2147483648")), ("price", None)]),
        1,
    )]);
    assert!(
        query
            .prepare(relational::inputs(&overflow, compiled.relational(), query.time() + 1)?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}
#[test]
fn join_keys_resolve_direction_and_reject_unproven_evaluation_domains() -> Result<()> {
    let native = contract();
    for condition in [
        "abs(a.id)=abs(b.auction)",
        "abs(b.auction)=abs(a.id)",
        "a.id=abs(b.auction)",
        "length(a.category)=b.auction",
    ] {
        compile(
            &format!("SELECT a.id FROM source.auction a JOIN source.bid b ON {condition}"),
            &native,
        )?
        .validate_execution()?;
    }
    for condition in [
        "a.id+b.id=b.auction",
        "abs(a.id)=abs(a.id)",
        "1=abs(b.auction)",
        "abs(a.id)::numeric=b.price::numeric",
        "abs(a.id)>abs(b.auction)",
    ] {
        assert!(
            compile(
                &format!("SELECT a.id FROM source.auction a JOIN source.bid b ON {condition}"),
                &native
            )
            .is_err(),
            "{condition}"
        );
    }
    compile(&SQL.replace(" JOIN ", " LEFT JOIN "), &native)?.validate_execution()?;
    let compiled = compile(&format!("{SQL} AND abs(a.id)=abs(b.auction)"), &native)?;
    let maps: usize = compiled
        .relational()
        .nodes
        .iter()
        .filter_map(|node| match node {
            Node::Map { computed, .. } => Some(computed.len()),
            _ => None,
        })
        .sum();
    assert_eq!(maps, 2);
    Ok(())
}

#[test]
fn unproven_window_and_hidden_dependency_shapes_remain_rejected() {
    for sql in [
        "SELECT id,SUM(abs(price)) OVER(PARTITION BY auction ORDER BY abs(id) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM source.bid",
        "SELECT id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE abs(a.id+b.id)=b.auction)",
        "SELECT now() FROM source.bid",
        "SELECT random() FROM source.bid",
    ] {
        assert!(compile(sql, &contract()).is_err(), "{sql}");
    }
}
