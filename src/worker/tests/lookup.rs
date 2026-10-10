use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type State = BTreeMap<Row, i64>;
type Bag = Batch<relational::Key, Row>;
const SQL: &str = "SELECT l.category,p.price FROM source.auction l LEFT JOIN LATERAL(SELECT r.price,r.id FROM source.bid r WHERE r.auction=l.k AND r.at<=l.at ORDER BY r.at DESC,r.id DESC LIMIT 1) p ON true WHERE p.price IS NULL OR p.price>=0";
fn native(oid: u32) -> crate::source::Contract {
    let mut native = contract();
    native.relations[0].columns.push(crate::source::Column {
        position: 4,
        name: "k".into(),
        oid: 23,
        modifier: -1,
        nullable: true,
        primary: false,
        collation: 0,
    });
    for relation in &mut native.relations {
        relation.columns.push(crate::source::Column {
            position: if relation.table == "auction" { 5 } else { 4 },
            name: "at".into(),
            oid,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 0,
        });
    }
    native
}
fn oracle(left: &State, right: &State, inclusive: bool) -> Result<Bag> {
    let number =
        |value: &Option<String>| value.as_deref().and_then(|value| value.parse::<i64>().ok());
    let mut outputs = Vec::new();
    for (left, weight) in left.iter().filter(|(_, weight)| **weight > 0) {
        let selected = right
            .iter()
            .filter(|(right, weight)| {
                **weight > 0
                    && left["k"].is_some()
                    && left["k"] == right["auction"]
                    && number(&left["at"])
                        .zip(number(&right["at"]))
                        .is_some_and(|(lhs, rhs)| rhs < lhs || inclusive && rhs == lhs)
            })
            .max_by_key(|(right, _)| (number(&right["at"]), number(&right["id"])));
        let price = selected.and_then(|(right, _)| right["price"].clone());
        if number(&price).is_none_or(|value| value >= 0) {
            let output =
                [("0:category".into(), left["category"].clone()), ("1:price".into(), price)].into();
            outputs.push(((Vec::new(), output), *weight));
        }
    }
    Batch::from_updates(outputs)
}
async fn history(inclusive: bool) -> Result<()> {
    let native = native(20);
    let sql = if inclusive { SQL.into() } else { SQL.replace("r.at<=l.at", "r.at<l.at") };
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational().context("missing lookup IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let left = |id, key, at| {
        row(&[("id", Some(id)), ("category", None), ("other", None), ("k", key), ("at", at)])
    };
    let right = |id, key, at, price| {
        row(&[("id", Some(id)), ("auction", key), ("at", at), ("price", price)])
    };
    let probe = left("1", Some("1"), Some("10"));
    let null_key = left("2", None, Some("10"));
    let null_time = left("3", Some("1"), None);
    let earlier = right("1", Some("1"), Some("9"), Some("7"));
    let null_neighbor = right("2", Some("1"), Some("10"), None);
    let tie = right("3", Some("1"), Some("10"), Some("4"));
    let negative = right("3", Some("1"), Some("10"), Some("-7"));
    let moved = left("1", Some("2"), Some("8"));
    let next = right("4", Some("2"), Some("8"), Some("9"));
    let mut lhs = State::new();
    let mut rhs = State::new();
    for (index, changes) in [
        batch(vec![
            ("auction", probe.clone(), 2),
            ("auction", null_key, 1),
            ("auction", null_time, 1),
            ("bid", earlier.clone(), 3),
        ]),
        batch(vec![("bid", earlier.clone(), -1)]),
        batch(vec![("bid", null_neighbor.clone(), 1), ("bid", tie.clone(), 1)]),
        batch(vec![("bid", tie, -1), ("bid", negative.clone(), 1)]),
        batch(vec![
            ("auction", probe, -2),
            ("auction", moved.clone(), 2),
            ("bid", next.clone(), 2),
            ("bid", null_neighbor, -1),
        ]),
        batch(vec![("bid", next, -1), ("bid", earlier, -2), ("bid", negative, -1)]),
        batch(vec![]),
    ]
    .into_iter()
    .enumerate()
    {
        let before = oracle(&lhs, &rhs, inclusive)?;
        for update in &changes.updates {
            let state = if update.tuple.table == "auction" { &mut lhs } else { &mut rhs };
            *state.entry(update.tuple.row.clone()).or_default() += update.weight;
        }
        let after = oracle(&lhs, &rhs, inclusive)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        if index == 2 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    let checkpoint = query.checkpoint()?;
    let absent =
        row(&[("id", Some("1")), ("auction", Some("2")), ("at", Some("8")), ("price", Some("9"))]);
    assert!(
        query
            .prepare(relational::inputs(&batch(vec![("bid", absent, -1)]), ir, query.time() + 1)?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, checkpoint);
    Ok(())
}
#[tokio::test]
async fn predecessor_preserves_limit_one_bags_and_simultaneous_inputs() -> Result<()> {
    history(true).await
}
#[tokio::test]
async fn strict_predecessor_retracts_and_restores() -> Result<()> {
    history(false).await
}
#[test]
fn lookup_resolution_normalizes_correlations_and_rejects_unsupported_sql() -> Result<()> {
    let native = native(1114);
    let compiled = compile(SQL, &native)?;
    let normalized = SQL
        .replace("r.auction=l.k AND r.at<=l.at", "l.at>=r.at AND l.k=auction")
        .replace("l.", "\"L\".")
        .replace("auction l ", "auction AS \"L\" ");
    assert_eq!(
        serde_json::to_vec(&compiled)?,
        serde_json::to_vec(&compile(&normalized, &native)?)?
    );
    for invalid in [
        SQL.replace("LIMIT 1", "LIMIT 2"),
        SQL.replace("LIMIT 1", "LIMIT 1 OFFSET 1"),
        SQL.replace("LEFT JOIN LATERAL", "LEFT JOIN"),
        SQL.replace("ON true", "ON p.id=l.id"),
        SQL.replace("r.id DESC", "r.price DESC"),
        SQL.replace("r.at DESC", "r.at ASC"),
        SQL.replace("p.price", "r.price"),
        SQL.replace("r.at<=l.at", "r.at>=l.at"),
        SQL.replace("r.auction=l.k", "r.auction=auction"),
        SQL.replace("SELECT r.price,r.id", "SELECT abs(r.price),r.id"),
        SQL.replace("LIMIT 1", "FETCH FIRST 1 ROW WITH TIES"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    assert!(compile(&SQL.replace("SELECT l.category,p.price", "SELECT id"), &native).is_err());
    assert_ne!(
        serde_json::to_vec(&compiled)?,
        serde_json::to_vec(&compile(&SQL.replace("<=l.at", "<l.at"), &native)?)?
    );
    let mut incompatible = native;
    incompatible.relations[1].columns.last_mut().context("missing bound")?.oid = 1184;
    assert!(compile(SQL, &incompatible).is_err());
    Ok(())
}

#[tokio::test]
async fn lookup_work_failure_retains_prior_and_can_retry() -> Result<()> {
    let native = native(20);
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational().context("missing lookup IR")?;
    let mut limits = settings();
    limits.limits.contributions = 20;
    let query = relational::build(&native, &compiled, &limits)?;
    let checkpoint = query.checkpoint()?;
    let probe = row(&[
        ("id", Some("1")),
        ("category", None),
        ("other", None),
        ("k", Some("1")),
        ("at", Some("1")),
    ]);
    let mut changes = vec![("auction", probe, 1)];
    for id in ["1", "2", "3", "4"] {
        changes.push((
            "bid",
            row(&[
                ("id", Some(id)),
                ("auction", Some("1")),
                ("at", Some("10")),
                ("price", Some("7")),
            ]),
            1,
        ));
    }
    let changes = batch(changes);
    let failed = query.prepare(relational::inputs(&changes, ir, 1)?).await;
    assert!(failed.is_err());
    assert_eq!(query.checkpoint()?, checkpoint);
    let mut retry = relational::build(&native, &compiled, &settings())?;
    let prepared = retry.prepare(relational::inputs(&changes, ir, 1)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([(
            (Vec::new(), [("0:category".into(), None), ("1:price".into(), None)].into()),
            1
        )])?
    );
    retry.commit(prepared)?;
    Ok(())
}

#[tokio::test]
async fn derived_lookup_filter_runs_after_match_selection() -> Result<()> {
    let native = native(20);
    let compiled = compile(
        &format!("SELECT q.category,q.price FROM({SQL}) q WHERE q.price IS NULL"),
        &native,
    )?;
    let ir = compiled.relational().context("missing derived lookup IR")?;
    let mut query = relational::build(&native, &compiled, &settings())?;
    let left = row(&[
        ("id", Some("1")),
        ("category", None),
        ("other", None),
        ("k", Some("1")),
        ("at", Some("20")),
    ]);
    let right =
        row(&[("id", Some("1")), ("auction", Some("1")), ("at", Some("19")), ("price", Some("7"))]);
    let changes = batch(vec![("auction", left, 2), ("bid", right.clone(), 3)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    assert!(prepared.output().batch.iter().next().is_none());
    query.commit(prepared)?;
    let changes = batch(vec![("bid", right, -3)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([(
            (Vec::new(), [("0:category".into(), None), ("1:price".into(), None)].into()),
            2
        )])?
    );
    Ok(())
}
