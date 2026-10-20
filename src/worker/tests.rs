use super::{program, spec::Query};
use crate::{
    engine::{Batch, plan::Settings},
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
        environment: crate::source::Environment::fixture(),
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
        encoded_cache: None,
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
        build(&native, &crate::compiler::Compiled::legacy(spec(), &native)?, &settings())?;
    let mut changed = spec();
    changed.group = "other".into();
    assert_ne!(
        original.plan().identity(),
        build(&native, &crate::compiler::Compiled::legacy(changed.clone(), &native)?, &settings())?
            .plan()
            .identity()
    );
    let mut changed_native = native.clone();
    changed_native.relations[1].columns[2].oid = 23;
    assert_ne!(
        original.plan().identity(),
        build(
            &changed_native,
            &crate::compiler::Compiled::legacy(spec(), &changed_native)?,
            &settings()
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
    let compiled = crate::compiler::Compiled::legacy(selectors, &contract())?;
    let mut query = build(&contract(), &compiled, &settings())?;
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
    let prepared =
        query.prepare(program::relational::inputs(&source, compiled.relational(), 1)?).await?;
    query.commit(prepared)?;
    assert_eq!(
        grouped_output(&query, &compiled).await?,
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
    let prepared =
        query.prepare(program::relational::inputs(&changes, compiled.relational(), 2)?).await?;
    query.commit(prepared)?;
    assert_eq!(
        grouped_output(&query, &compiled).await?,
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

mod expansion;
mod partition;
mod statistics;

mod ranking;

mod peer_frames;

mod case;
mod composition;
mod decimal;
mod derived;
mod distinct;
mod join_composition;
mod session;

mod navigation;

mod lookup;

mod group_membership;

mod derived_join;
mod grouped_occurrence;
mod native_cross;

mod lookup_remainder;

mod engine_expressions;

mod incremental_distinct;

mod ordered;

mod native_join;

mod numeric;
#[path = "tests/topk.rs"]
mod topk;

mod membership;
mod membership_counts;

/// Apply full-row signed updates to the independent source bag used by an oracle.
fn apply_rows(state: &mut std::collections::BTreeMap<Row, i64>, changes: &weighted::Batch) {
    for change in &changes.updates {
        *state.entry(change.tuple.row.clone()).or_default() += change.weight;
    }
}

/// Compute the signed difference of independently evaluated full-bag snapshots.
fn bag_delta<K: Ord + Clone, V: Ord + Clone>(
    before: &Batch<K, V>,
    after: &Batch<K, V>,
) -> Result<Batch<K, V>> {
    Batch::from_updates(
        before
            .iter()
            .map(|(tuple, weight)| (tuple.clone(), -*weight))
            .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
    )
}

/// Qualify raw sufficient-statistic kernels independently of SQL AVG rendering.
fn raw_statistics_compiled(mut compiled: crate::compiler::Compiled) -> crate::compiler::Compiled {
    use crate::compiler::relational::Node;
    let ir = &mut compiled.relational;
    if let Some(position) = ir
        .nodes
        .iter()
        .position(|node| matches!(node, Node::Map { id, .. } if id.starts_with("boundary_")))
    {
        let Node::Map { id, input, computed } = ir.nodes.remove(position) else {
            return compiled;
        };
        for output in &mut ir.output.columns {
            if let Some(column) = computed
                .iter()
                .find(|value| value.column == output.column)
                .and_then(|value| value.expression.as_average())
            {
                output.column = column.clone();
            }
        }
        if let Some(Node::Output { input: output, .. }) = ir.nodes.last_mut() {
            assert_eq!(*output, id);
            *output = input;
        }
    }
    compiled
}

async fn restart_relational(
    query: &mut program::relational::Query,
    native: &Contract,
    compiled: &crate::compiler::Compiled,
    options: &Settings,
) -> Result<()> {
    let checkpoint = query.checkpoint()?;
    *query = program::relational::build(native, compiled, options)?;
    query.restore_checkpoint(checkpoint).await
}

fn build(
    native: &Contract,
    compiled: &crate::compiler::Compiled,
    options: &Settings,
) -> Result<program::relational::Query> {
    program::relational::build(native, compiled, options)
}
type ProjectedBag = Batch<(), Vec<Option<crate::compiler::Cell>>>;
fn projected_batch(
    batch: &Batch<program::relational::Key, Row>,
    compiled: &crate::compiler::Compiled,
) -> Result<ProjectedBag> {
    Batch::from_updates(
        batch
            .iter()
            .map(|((_, row), weight)| Ok((((), compiled.output().row(row)?), *weight)))
            .collect::<Result<Vec<_>>>()?,
    )
}
async fn projected_output(
    query: &program::relational::Query,
    compiled: &crate::compiler::Compiled,
) -> Result<ProjectedBag> {
    projected_batch(&query.snapshot().trace("output")?.materialize().await?, compiled)
}
type GroupedBag = Batch<Option<String>, (i64, Option<i64>)>;
async fn grouped_output(
    query: &program::relational::Query,
    compiled: &crate::compiler::Compiled,
) -> Result<GroupedBag> {
    let projected = projected_output(query, compiled).await?;
    let mut values = Vec::new();
    for (((), row), weight) in projected.iter() {
        let text = |index: usize| -> Result<Option<String>> {
            row.get(index)
                .context("missing grouped cell")?
                .as_ref()
                .map(|cell| {
                    let json = serde_json::to_value(cell)?;
                    Ok(json.as_str().map_or_else(|| json.to_string(), str::to_owned))
                })
                .transpose()
        };
        let group = text(0)?;
        let count = text(1)?.context("NULL grouped count")?.parse()?;
        let sum = text(2)?.map(|value| value.parse()).transpose()?;
        values.push(((group, (count, sum)), *weight));
    }
    Batch::from_updates(values)
}

mod computed_aggregates;

mod computed_join;

mod expression_composition;

mod sliding_windows;
