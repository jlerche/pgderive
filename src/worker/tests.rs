use super::{program, spec::Query};
use crate::{
    engine::{Batch, plan::query::Settings},
    source::{Column, Contract, Identity, Relation},
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::sync::Arc;

pub(super) fn contract() -> Contract {
    let column = |position, name: &str, oid, primary: bool| Column {
        position,
        name: name.into(),
        oid,
        modifier: -1,
        nullable: !primary,
        primary,
        collation: if oid == 25 { 100 } else { 0 },
    };
    Contract {
        identity: Identity { system: "1".into(), timeline: 1, database: "db".into() },
        publication: "publication".into(),
        slot: "slot".into(),
        relations: vec![
            Relation {
                oid: 1,
                schema: "source".into(),
                table: "auction".into(),
                columns: vec![
                    column(1, "id", 23, true),
                    column(2, "category", 25, false),
                    column(3, "other", 25, false),
                ],
            },
            Relation {
                oid: 2,
                schema: "source".into(),
                table: "bid".into(),
                columns: vec![
                    column(1, "id", 23, true),
                    column(2, "auction", 23, false),
                    column(3, "price", 20, false),
                ],
            },
        ],
    }
}
fn spec() -> Query {
    Query {
        left_schema: "source".into(),
        left_table: "auction".into(),
        left_key: "id".into(),
        group: "category".into(),
        right_schema: "source".into(),
        right_table: "bid".into(),
        right_key: "auction".into(),
        sum: "price".into(),
    }
}
pub(super) fn settings() -> Settings {
    Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
    }
}
pub(super) fn row(values: &[(&str, Option<&str>)]) -> Row {
    values.iter().map(|(key, value)| ((*key).into(), value.map(Into::into))).collect()
}
pub(super) fn batch(rows: Vec<(&str, Row, i64)>) -> weighted::Batch {
    weighted::Batch {
        updates: rows
            .into_iter()
            .map(|(table, row, weight)| weighted::Update {
                tuple: weighted::Tuple { schema: "source".into(), table: table.into(), row },
                weight,
            })
            .collect(),
    }
}
#[test]
fn registration_binds_native_types_and_query_selectors() -> Result<()> {
    let native = contract();
    let original =
        program::build(&native, &crate::compiler::Compiled::legacy(spec(), &native)?, settings())?;
    let mut changed = spec();
    changed.group = "other".into();
    assert_ne!(
        original.plan().identity(),
        program::build(
            &native,
            &crate::compiler::Compiled::legacy(changed.clone(), &native)?,
            settings()
        )?
        .plan()
        .identity()
    );
    let mut changed_native = native.clone();
    changed_native.relations[1].columns[2].oid = 23;
    assert_ne!(
        original.plan().identity(),
        program::build(
            &changed_native,
            &crate::compiler::Compiled::legacy(spec(), &changed_native)?,
            settings()
        )?
        .plan()
        .identity()
    );
    changed.sum = "missing".into();
    assert!(crate::compiler::Compiled::legacy(changed, &native).is_err());
    changed_native.relations[1].columns[1].oid = 20;
    assert!(crate::compiler::Compiled::legacy(spec(), &changed_native).is_err());
    Ok(())
}
#[tokio::test]
async fn full_row_join_nulls_and_simultaneous_retractions_match_sql_semantics() -> Result<()> {
    let selectors = spec();
    let mut query = program::build(
        &contract(),
        &crate::compiler::Compiled::legacy(selectors.clone(), &contract())?,
        settings(),
    )?;
    let left = row(&[("id", Some("1")), ("category", None), ("other", Some("a"))]);
    let nil = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let bid = row(&[("id", Some("4")), ("auction", Some("1")), ("price", Some("7"))]);
    let absent = row(&[("id", Some("5")), ("auction", None), ("price", Some("100"))]);
    let source = batch(vec![
        ("auction", left.clone(), 1),
        ("bid", nil, 1),
        ("bid", bid.clone(), 1),
        ("bid", absent, 1),
    ]);
    let prepared = query.prepare(program::inputs(&source, &selectors, 1)?).await?;
    query.commit(prepared)?;
    assert_eq!(
        query.snapshot().output.materialize().await?,
        Batch::from_updates([((None, (2, Some(7))), 1)])?
    );
    let mut next_left = left.clone();
    next_left.insert("category".into(), Some("x".into()));
    let mut next_bid = bid.clone();
    next_bid.insert("price".into(), Some("9".into()));
    let changes = batch(vec![
        ("auction", left, -1),
        ("auction", next_left, 1),
        ("bid", bid, -1),
        ("bid", next_bid, 1),
    ]);
    let prepared = query.prepare(program::inputs(&changes, &selectors, 2)?).await?;
    query.commit(prepared)?;
    assert_eq!(
        query.snapshot().output.materialize().await?,
        Batch::from_updates([((Some("x".into()), (2, Some(9))), 1)])?
    );
    query.checkpoint()?.arrangements.first().context("missing worker membership")?;
    Ok(())
}

#[path = "tests/sql.rs"]
mod sql;

#[path = "tests/projection.rs"]
mod projection;

mod relational;
