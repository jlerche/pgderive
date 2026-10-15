//! Native left joins preserve full rows, NULL nonmatches and synchronized changes.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
const SQL: &str = "SELECT a.category AS c,b.price AS p FROM source.auction a LEFT JOIN source.bid b ON a.other=b.auction WHERE b.price IS NULL OR b.price<0";
fn oracle(
    left: &BTreeMap<Row, i64>,
    right: &BTreeMap<Row, i64>,
) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (a, weight) in left {
        if *weight == 0 {
            continue;
        }
        let matches = right
            .iter()
            .filter(|(b, weight)| {
                **weight > 0 && a["other"].is_some() && a["other"] == b["auction"]
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            updates.push((
                (
                    Vec::new(),
                    [("0:category".into(), a["category"].clone()), ("1:price".into(), None)].into(),
                ),
                *weight,
            ));
        }
        for (b, multiplicity) in matches {
            if b["price"].as_ref().is_none_or(|value| value.starts_with('-')) {
                updates.push((
                    (
                        Vec::new(),
                        [
                            ("0:category".into(), a["category"].clone()),
                            ("1:price".into(), b["price"].clone()),
                        ]
                        .into(),
                    ),
                    weight * multiplicity,
                ));
            }
        }
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn native_left_join_signed_history_nulls_restart_and_atomic_rejection() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let compiled = compile(SQL, &native)?;
    let ir = compiled.relational().context("missing join IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", Some("1"))]);
    let a2 = row(&[("id", Some("2")), ("category", Some("nullkey")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("-2"))]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("-2"))]);
    let null = row(&[("id", Some("3")), ("auction", None), ("price", Some("-3"))]);
    let mut changed = a.clone();
    changed.insert("category".into(), Some("changed".into()));
    let mut positive = b2.clone();
    positive.insert("price".into(), Some("5".into()));
    let histories = [
        batch(vec![
            ("auction", a.clone(), 1),
            ("auction", a2.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", b2.clone(), 1),
            ("bid", null.clone(), 1),
        ]),
        batch(vec![
            ("auction", a.clone(), -1),
            ("auction", changed.clone(), 1),
            ("bid", b.clone(), -1),
            ("bid", b2.clone(), -1),
            ("bid", positive.clone(), 1),
        ]),
        batch(vec![("bid", positive, -1)]),
        batch(vec![("auction", changed, -1), ("auction", a.clone(), 1), ("bid", b, 1)]),
        batch(vec![("auction", a, -1), ("auction", a2, -1), ("bid", null, -1)]),
    ];
    let mut left = BTreeMap::new();
    let mut right = BTreeMap::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&left, &right)?;
        for change in &changes.updates {
            let state = if change.tuple.table == "auction" { &mut left } else { &mut right };
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&left, &right)?;
        let expected = super::bag_delta(&before, &after)?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, expected);
        query.commit(work)?;
        if index == 2 {
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        }
    }
    let before = query.checkpoint()?;
    let absent = row(&[("id", Some("999")), ("auction", Some("1")), ("price", Some("-99"))]);
    assert!(
        query
            .prepare(relational::inputs(&batch(vec![("bid", absent, -1)]), ir, query.time() + 1)?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[tokio::test]
async fn self_and_multiway_joins_preserve_every_simultaneous_cross_term() -> Result<()> {
    for multiway in [false, true] {
        repeated_history(multiway).await?;
    }
    Ok(())
}
async fn repeated_history(multiway: bool) -> Result<()> {
    let sql = if multiway {
        "SELECT a.id AS l,b.id AS r,c.category FROM source.bid a JOIN source.bid b ON a.auction=b.auction JOIN source.auction c ON b.auction=c.id"
    } else {
        "SELECT a.id AS l,b.id AS r FROM source.bid a JOIN source.bid b ON a.auction=b.auction"
    };
    let native = contract();
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing repeated-source graph")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let a2 = row(&[("id", Some("2")), ("category", Some("two")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("-2"))]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("-2"))]);
    let mut moved = a.clone();
    moved.insert("category".into(), Some("moved".into()));
    let mut changed = b2.clone();
    changed.insert("auction".into(), Some("2".into()));
    let histories = [
        batch(vec![
            ("auction", a.clone(), 1),
            ("auction", a2.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", b2.clone(), 1),
        ]),
        batch(vec![
            ("auction", a.clone(), -1),
            ("auction", moved.clone(), 1),
            ("bid", b2, -1),
            ("bid", changed.clone(), 1),
        ]),
        batch(vec![("auction", moved, -1)]),
        batch(vec![
            ("auction", a, 1),
            ("bid", row(&[("id", Some("3")), ("auction", None), ("price", None)]), 1),
        ]),
        batch(vec![("auction", a2, -1), ("bid", changed, -1)]),
    ];
    let mut bids = BTreeMap::new();
    let mut auctions = BTreeMap::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = repeated_oracle(&bids, &auctions, multiway)?;
        for change in &changes.updates {
            let state = if change.tuple.table == "auction" { &mut auctions } else { &mut bids };
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = repeated_oracle(&bids, &auctions, multiway)?;
        let expected = super::bag_delta(&before, &after)?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, expected);
        query.commit(work)?;
        if index == 1 {
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        }
    }
    Ok(())
}
fn repeated_oracle(
    bids: &BTreeMap<Row, i64>,
    auctions: &BTreeMap<Row, i64>,
    multiway: bool,
) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (a, wa) in bids {
        for (b, wb) in bids {
            if *wa == 0 || *wb == 0 || a["auction"].is_none() || a["auction"] != b["auction"] {
                continue;
            }
            let output: Row =
                [("0:id".into(), a["id"].clone()), ("1:id".into(), b["id"].clone())].into();
            if !multiway {
                updates.push(((Vec::new(), output), wa * wb));
                continue;
            }
            for (c, wc) in auctions {
                if *wc == 0 || c["id"] != b["auction"] {
                    continue;
                }
                let mut output = output.clone();
                output.insert("2:category".into(), c["category"].clone());
                updates.push(((Vec::new(), output), wa * wb * wc));
            }
        }
    }
    Batch::from_updates(updates)
}

#[test]
fn join_tree_aliases_keys_and_unsupported_shapes_bind_identity() -> Result<()> {
    let native = contract();
    let sql = "SELECT a.category AS c,b.price AS p FROM source.auction a LEFT JOIN source.bid b ON a.id=b.auction AND a.id=b.id";
    let first = compile(sql, &native)?;
    let alias = sql
        .replace("a.", "\"X\".")
        .replace("auction a ", "auction AS \"X\" ")
        .replace("\"X\".id=b.auction AND \"X\".id=b.id", "b.id=\"X\".id AND b.auction=\"X\".id");
    assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&compile(&alias, &native)?)?);
    assert_ne!(
        serde_json::to_vec(&first)?,
        serde_json::to_vec(&compile(&sql.replace(" AND a.id=b.id", ""), &native)?)?
    );
    for bad in [
        sql.replace("a.category AS c", "id AS c"),
        sql.replace("LEFT JOIN", "RIGHT JOIN"),
        sql.replace("LEFT JOIN", "FULL JOIN"),
        sql.replace("a.id=b.auction AND a.id=b.id", "a.id=b.auction OR a.id=b.id"),
        sql.replace("a.id=b.auction AND a.id=b.id", "a.id=a.id"),
        sql.replace("bid b ", "bid a "),
        "SELECT l.id FROM source.bid l LEFT JOIN source.bid r USING(id)".into(),
    ] {
        assert!(compile(&bad, &native).is_err(), "accepted {bad}");
    }
    let scoped = "SELECT l.auction,l.n,r.category FROM (SELECT auction,COUNT(*) AS n FROM source.bid GROUP BY auction) l LEFT JOIN source.auction r ON l.auction=r.id";
    assert!(compile(scoped, &native)?.relational().is_some());
    assert!(compile("SELECT l.id,r.n FROM source.auction l LEFT JOIN (SELECT auction,COUNT(*) AS n FROM source.bid GROUP BY auction) r ON l.id=r.auction", &native)?.relational().is_some());
    assert!(compile("SELECT l.id,r.id FROM source.auction l LEFT JOIN (SELECT id,abs(price) AS value FROM source.bid) r ON l.id=r.id",&native).is_err());
    Ok(())
}
