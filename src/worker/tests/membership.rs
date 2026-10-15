//! Native DISTINCT/existence against independent raw-row bags through restart.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
use std::collections::BTreeMap;
type State = BTreeMap<Row, i64>;
fn oracle(
    left: &State,
    right: &State,
    anti: Option<bool>,
    distinct: bool,
) -> Result<Batch<relational::Key, Row>> {
    let mut output = BTreeMap::new();
    for (a, weight) in left {
        if *weight == 0 {
            continue;
        }
        let present = right
            .iter()
            .any(|(b, weight)| *weight > 0 && a["other"].is_some() && a["other"] == b["auction"]);
        if anti.is_some_and(|anti| present == anti) {
            continue;
        }
        let row = [("0:category".into(), a["category"].clone())].into();
        *output.entry((Vec::new(), row)).or_insert(0_i64) += weight;
    }
    Batch::from_updates(
        output.into_iter().map(|(tuple, weight)| (tuple, if distinct { 1 } else { weight })),
    )
}
#[tokio::test]
async fn membership_signed_history_matches_raw_rows_and_object_backed_restart() -> Result<()> {
    for (anti, distinct) in [
        (None, true),
        (Some(false), false),
        (Some(true), false),
        (Some(false), true),
        (Some(true), true),
    ] {
        history(anti, distinct).await?;
    }
    Ok(())
}
async fn history(anti: Option<bool>, distinct: bool) -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let clause = anti.map_or_else(String::new, |anti| {
        format!(
            " WHERE {}EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=a.other)",
            if anti { "NOT " } else { "" }
        )
    });
    let sql = format!(
        "SELECT {}a.category FROM source.auction a{clause}",
        if distinct { "DISTINCT " } else { "" }
    );
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", Some("1"))]);
    let a2 = row(&[("id", Some("2")), ("category", None), ("other", Some("1"))]);
    let null = row(&[("id", Some("3")), ("category", Some("null")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let bn = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let histories = [
        batch(vec![
            ("auction", a.clone(), 2),
            ("auction", a2.clone(), 1),
            ("auction", null.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", b2.clone(), 1),
            ("bid", bn.clone(), 1),
        ]),
        batch(vec![("auction", a.clone(), -1), ("bid", b.clone(), -1)]),
        batch(vec![("bid", b2.clone(), -1)]),
        batch(vec![("auction", a2.clone(), -1), ("bid", b.clone(), 1)]),
        batch(vec![("auction", a, -1), ("auction", null, -1), ("bid", b, -1), ("bid", bn, -1)]),
    ];
    let mut left = State::new();
    let mut right = State::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&left, &right, anti, distinct)?;
        for change in &changes.updates {
            let state = if change.tuple.table == "auction" { &mut left } else { &mut right };
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&left, &right, anti, distinct)?;
        let expected = super::bag_delta(&before, &after)?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, expected, "{sql}, tick {index}");
        query.commit(work)?;
        if index == 2 {
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        }
    }
    let before = query.checkpoint()?;
    let absent = row(&[("id", Some("999")), ("category", None), ("other", Some("1"))]);
    assert!(
        query
            .prepare(relational::inputs(
                &batch(vec![("auction", absent, -1)]),
                ir,
                query.time() + 1
            )?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}
#[test]
fn unsupported_membership_shapes_fail_during_compilation() {
    let native = contract();
    for sql in [
        "SELECT DISTINCT ON (id) id FROM source.auction",
        "SELECT DISTINCT abs(id) FROM source.auction",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction>a.id)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id OR b.id=a.id)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=b.id)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id LIMIT 1)",
        "SELECT a.id FROM source.auction a WHERE a.id NOT IN(SELECT auction FROM source.bid)",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn compound_self_existence_and_repeated_distinct_columns_preserve_null_semantics()
-> Result<()> {
    let native = contract();
    let sql = "SELECT DISTINCT l.auction,l.auction AS again FROM source.bid l WHERE EXISTS(SELECT 1 FROM source.bid r WHERE r.auction=l.auction AND r.price=l.price)";
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("4"))]);
    let b = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("4"))]);
    let null = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let null_key = row(&[("id", Some("4")), ("auction", None), ("price", Some("4"))]);
    let histories = [
        batch(vec![
            ("bid", a.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", null.clone(), 1),
            ("bid", null_key, 1),
        ]),
        batch(vec![("bid", a, -1)]),
        batch(vec![]),
        batch(vec![("bid", b, -1), ("bid", null, -1)]),
    ];
    let mut state = State::new();
    let mut previous = Batch::from_updates([])?;
    for changes in histories {
        super::apply_rows(&mut state, &changes);
        let rows = state.iter().filter(|(left, weight)| {
            **weight > 0
                && state.iter().any(|(right, count)| {
                    *count > 0
                        && ["auction", "price"]
                            .iter()
                            .all(|field| left[*field].is_some() && left[*field] == right[*field])
                })
        });
        let expected = rows
            .map(|(row, _)| {
                ((Vec::new(), [("0:auction".into(), row["auction"].clone())].into()), 1)
            })
            .collect::<BTreeMap<_, _>>();
        let current = Batch::from_updates(expected)?;
        let delta = Batch::from_updates(
            previous
                .iter()
                .map(|(tuple, w)| (tuple.clone(), -*w))
                .chain(current.iter().map(|(tuple, w)| (tuple.clone(), *w))),
        )?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, delta);
        query.commit(work)?;
        previous = current;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}

#[test]
fn membership_identity_normalizes_aliases_and_distinguishes_anti_and_distinct() -> Result<()> {
    let native = contract();
    let semi = "SELECT a.category FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id)";
    let renamed = "SELECT l.category FROM source.auction l WHERE EXISTS(SELECT 1 FROM source.bid r WHERE l.id=r.auction)";
    let compiled = compile(semi, &native)?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&compile(renamed, &native)?)?);
    let identity = |sql: &str| -> Result<_> {
        let compiled = compile(sql, &native)?;
        Ok(relational::build(&native, &compiled, &settings())?.plan().identity().to_string())
    };
    assert_ne!(identity(semi)?, identity(&semi.replace("WHERE EXISTS", "WHERE NOT EXISTS"))?);
    assert_ne!(identity(semi)?, identity(&semi.replacen("SELECT ", "SELECT DISTINCT ", 1))?);
    Ok(())
}

#[tokio::test]
async fn distinct_threshold_retains_full_rows_without_narrowing_occurrence_totals() -> Result<()> {
    let native = contract();
    let compiled = compile("SELECT DISTINCT category FROM source.auction", &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("2")), ("category", None), ("other", None)]);
    let phantom = row(&[("id", Some("999")), ("category", None), ("other", None)]);
    let histories = [
        (batch(vec![("auction", a.clone(), i64::MAX), ("auction", b.clone(), i64::MAX)]), 1),
        (batch(vec![("auction", a.clone(), -1)]), 0),
        (batch(vec![("auction", a, -(i64::MAX - 1))]), 0),
        (batch(vec![("auction", b, -i64::MAX)]), -1),
    ];
    for (index, (changes, weight)) in histories.into_iter().enumerate() {
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        let expected = if weight == 0 {
            Batch::from_updates([])?
        } else {
            Batch::from_updates([((Vec::new(), [("0:category".into(), None)].into()), weight)])?
        };
        assert_eq!(work.output().batch, expected, "DISTINCT tick {index}");
        query.commit(work)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint.clone()).await?;
        if index < 3 {
            // A projected collision cannot make an absent source row retractable.
            let invalid = batch(vec![("auction", phantom.clone(), -1)]);
            assert!(
                query.prepare(relational::inputs(&invalid, ir, query.time() + 1)?).await.is_err()
            );
            assert_eq!(query.checkpoint()?, checkpoint);
        }
    }
    Ok(())
}
