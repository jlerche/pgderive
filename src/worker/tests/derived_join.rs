//! Derived membership joins require an explicit scoped graph/runtime bridge.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{Cell, compile},
    source::Relation,
    transaction::Row,
    worker::program::relational,
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;

fn native() -> crate::source::Contract {
    let mut native = contract();
    let template = native.relations[1].columns[2].clone();
    for (position, name, oid) in [(4, "seller", 23), (5, "date_time", 1184)] {
        let mut column = template.clone();
        column.position = position;
        column.name = name.into();
        column.oid = oid;
        native.relations[0].columns.push(column);
    }
    let mut person =
        Relation { oid: 3, schema: "source".into(), table: "person".into(), columns: Vec::new() };
    for (position, name, oid, primary) in
        [(1, "id", 23, true), (2, "name", 25, false), (3, "date_time", 1184, false)]
    {
        let mut column = template.clone();
        column.position = position;
        column.name = name.into();
        column.oid = oid;
        column.primary = primary;
        column.nullable = !primary;
        column.collation = if oid == 25 { 100 } else { 0 };
        person.columns.push(column);
    }
    native.relations.push(person);
    native
}

#[test]
fn named_q8_joins_independent_grouped_scopes() -> Result<()> {
    let sql = include_str!("../../../queries/nexmark/q8.sql")
        .replace("nexmark.", "source.")
        .trim_end()
        .trim_end_matches(';')
        .to_owned();
    let compiled = compile(&sql, &native())?;
    assert!(compiled.relational().is_some());
    Ok(())
}

type State = BTreeMap<(String, Row), i64>;
type Output = BTreeMap<Vec<Option<Cell>>, i64>;
fn oracle(state: &State) -> Result<Output> {
    let mut left = BTreeMap::new();
    let mut right = BTreeMap::new();
    for ((table, row), weight) in state {
        if *weight == 0
            || table == "bid" && row["price"].as_ref().is_some_and(|price| price.starts_with('-'))
            || table == "auction" && row["category"].is_some()
        {
            continue;
        }
        let (groups, field) =
            if table == "bid" { (&mut left, "auction") } else { (&mut right, "other") };
        let key = row[field].as_ref().map(|value| value.parse::<i64>()).transpose()?;
        *groups.entry(key).or_insert(0) += weight;
    }
    let mut output = Output::new();
    for (key, count) in left {
        if let Some(key) = key
            && count > 1
            && right.get(&Some(key)) == Some(&count)
        {
            output.insert(
                vec![
                    Some(Cell::Integer(key)),
                    Some(Cell::Integer(count)),
                    Some(Cell::Integer(count)),
                ],
                1,
            );
        }
    }
    Ok(output)
}
#[tokio::test]
async fn scoped_aggregate_keys_preserve_simultaneous_changes_and_restart() -> Result<()> {
    for nested in 0..3 {
        history(nested).await?;
    }
    Ok(())
}
fn query_sql(nested: u8) -> String {
    let sql = "SELECT l.auction,l.n AS left_count,r.n AS right_count FROM(SELECT b.auction,COUNT(*) AS n FROM source.bid b WHERE b.price>=0 OR b.price IS NULL GROUP BY b.auction) l JOIN(SELECT a.other,COUNT(*) AS n FROM source.auction a WHERE a.category IS NULL GROUP BY a.other) r ON l.auction=r.other AND l.n=r.n WHERE l.n>1";
    if nested == 1 {
        format!(
            "SELECT x.auction,x.left_count,y.n AS right_count FROM({sql}) x JOIN(SELECT b.auction,COUNT(*) AS n FROM source.bid b WHERE b.price>=0 OR b.price IS NULL GROUP BY b.auction) y ON x.auction=y.auction AND x.left_count=y.n"
        )
    } else if nested == 2 {
        format!(
            "SELECT x.auction,x.left_count,y.n AS right_count FROM(SELECT b.auction,COUNT(*) AS n FROM source.bid b WHERE b.price>=0 OR b.price IS NULL GROUP BY b.auction) y JOIN({sql}) x ON x.auction=y.auction AND x.left_count=y.n"
        )
    } else {
        sql.into()
    }
}
async fn history(nested: u8) -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let sql = query_sql(nested);
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational().context("missing scoped join IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let bid = row(&[("id", Some("1")), ("auction", Some("7")), ("price", None)]);
    let second = row(&[("id", Some("2")), ("auction", Some("7")), ("price", Some("7"))]);
    let moved = row(&[("id", Some("2")), ("auction", Some("8")), ("price", Some("7"))]);
    let auction = row(&[("id", Some("1")), ("category", None), ("other", Some("7"))]);
    let other = row(&[("id", Some("2")), ("category", None), ("other", Some("8"))]);
    let null_bid = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let null_auction = row(&[("id", Some("3")), ("category", None), ("other", None)]);
    let excluded_bid = row(&[("id", Some("10")), ("auction", Some("7")), ("price", Some("-9"))]);
    let excluded_auction =
        row(&[("id", Some("10")), ("category", Some("excluded")), ("other", Some("7"))]);
    let mut state = State::new();
    let mut output = Output::new();
    for (index, changes) in [
        batch(vec![
            ("bid", bid.clone(), 2),
            ("auction", auction.clone(), 2),
            ("bid", null_bid.clone(), 2),
            ("auction", null_auction.clone(), 2),
            ("bid", excluded_bid.clone(), 2),
            ("auction", excluded_auction.clone(), 2),
        ]),
        batch(vec![("bid", second.clone(), 1), ("auction", auction.clone(), 1)]),
        batch(vec![("bid", bid.clone(), -1), ("auction", auction.clone(), -1)]),
        batch(vec![
            ("bid", second, -1),
            ("bid", moved.clone(), 1),
            ("auction", auction, -2),
            ("auction", other.clone(), 1),
        ]),
        batch(vec![
            ("bid", bid, -1),
            ("bid", moved, -1),
            ("auction", other, -1),
            ("bid", null_bid, -2),
            ("auction", null_auction, -2),
            ("bid", excluded_bid, -2),
            ("auction", excluded_auction, -2),
        ]),
    ]
    .into_iter()
    .enumerate()
    {
        for change in &changes.updates {
            *state.entry((change.tuple.table.clone(), change.tuple.row.clone())).or_default() +=
                change.weight;
        }
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        for ((_, row), weight) in prepared.output().batch.iter() {
            *output.entry(ir.output.row(row)?).or_default() += weight;
        }
        output.retain(|_, weight| *weight != 0);
        assert_eq!(output, oracle(&state)?);
        query.commit(prepared)?;
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    Ok(())
}

#[test]
fn scoped_join_normalizes_equalities_and_rejects_unsupported_shapes() -> Result<()> {
    let native = native();
    let sql = include_str!("../../../queries/nexmark/q8.sql")
        .replace("nexmark.", "source.")
        .trim_end()
        .trim_end_matches(';')
        .to_owned();
    let original = compile(&sql, &native)?;
    let equivalent = compile(
        &sql.replace(
            "p.id=a.seller AND p.starttime=a.starttime",
            "a.starttime=p.starttime AND a.seller=p.id AND p.id=a.seller",
        ),
        &native,
    )?;
    assert_eq!(serde_json::to_vec(&original)?, serde_json::to_vec(&equivalent)?);
    for invalid in [
        sql.replace("p.id,p.name,p.starttime", "p.id,p.name,starttime"),
        sql.replace("AND p.starttime=a.starttime", "OR p.starttime=a.starttime"),
        sql.replace("p.starttime=a.starttime", "p.starttime>=a.starttime"),
        sql.replace("p.id=a.seller", "p.id=p.id"),
        sql.replace("p.id=a.seller", "p.id=a.starttime"),
        sql.replace(") a ON", ") p ON"),
        sql.replace("JOIN (", "LEFT JOIN ("),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    let grouped = format!("SELECT q.id,COUNT(*) FROM({sql}) q GROUP BY q.id");
    assert!(compile(&grouped, &native)?.relational().is_some());
    let changed = compile(&sql.replace("10 seconds", "11 seconds"), &native)?;
    assert_ne!(serde_json::to_vec(&original)?, serde_json::to_vec(&changed)?);
    Ok(())
}
