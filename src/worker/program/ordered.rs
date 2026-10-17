//! `PostgreSQL` integral/timestamp extrema through ordered full-tuple membership.
use super::relational::Key;
use crate::{
    compiler::partition::{Function, Mode, Partition as Spec},
    engine::{
        Batch,
        dataflow::{Expand, OrderedSelection, Partition, Project},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
const ORDER: &str = "\0ordered";
const VALUE: &str = "\0value";
fn width(spec: &Spec) -> Result<usize> {
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("ordered extrema require grouped mode");
    };
    Ok(keys.len())
}
fn token(value: i64, descending: bool) -> String {
    format!("0{:016x}", super::order::biased(value, descending))
}
fn measure(
    aggregate: &crate::compiler::partition::Aggregate,
    row: &Row,
) -> Result<(String, Option<String>)> {
    let argument = aggregate.argument.as_ref().context("missing ordered argument")?;
    let value = row.get(&argument.name).context("missing ordered value")?;
    let qualifies =
        aggregate.filter.as_ref().map(|filter| filter.qualifies(row)).transpose()?.unwrap_or(true);
    if !qualifies || value.is_none() {
        return Ok(("1".into(), None));
    }
    let text = value.as_deref().context("missing nonNULL ordered value")?;
    let temporal = matches!(argument.oid, 1114 | 1184);
    let value = if temporal {
        crate::temporal::Timestamp::parse(text, argument.oid)?.sort_value()
    } else {
        text.parse::<i64>()?
    };
    let text = if temporal {
        crate::temporal::Timestamp::parse(text, argument.oid)?.text(argument.oid)?
    } else {
        value.to_string()
    };
    Ok((token(value, matches!(aggregate.function, Function::Max)), Some(text)))
}
pub(super) fn input(spec: &Spec) -> Expand<Key, Row, Key, Row> {
    let spec = spec.clone();
    Expand::new(move |key: &Key, row: &Row, emit| {
        ensure!(key.len() == width(&spec)?, "ordered key width mismatch");
        ensure!(
            row.keys().all(|field| field.as_str() > ORDER && !field.starts_with('\0')),
            "ordered internal field collision"
        );
        for (index, aggregate) in spec.aggregates.iter().enumerate() {
            let (order, value) = measure(aggregate, row)?;
            let mut indexed = row.clone();
            indexed.insert(ORDER.into(), Some(order));
            indexed.insert(VALUE.into(), value);
            let mut key = key.clone();
            key.push(index.to_string());
            emit((key, indexed))?;
        }
        Ok(())
    })
}
pub(super) fn first(spec: &Spec) -> OrderedSelection<Key, Row> {
    let spec = spec.clone();
    OrderedSelection::first(Project::new(move |key: &Key, row: &Row| {
        let group_width = width(&spec)?;
        ensure!(key.len() == group_width + 1, "ordered selection key width mismatch");
        let index = key.last().context("missing ordered aggregate index")?.parse::<usize>()?;
        let aggregate = spec.aggregates.get(index).context("invalid ordered aggregate index")?;
        let value = row.get(VALUE).context("missing ordered selected value")?.clone();
        let (expected, expected_value) = measure(aggregate, row)?;
        ensure!(
            row.get(ORDER) == Some(&Some(expected)) && value == expected_value,
            "invalid ordered identity codec"
        );
        let Mode::Grouped { keys } = &spec.mode else {
            anyhow::bail!("ordered selection requires grouping");
        };
        let mut output = keys
            .iter()
            .map(|column| {
                Ok((
                    column.name.clone(),
                    row.get(&column.name).context("missing ordered group value")?.clone(),
                ))
            })
            .collect::<Result<Row>>()?;
        output.insert(aggregate.field.clone(), value);
        Ok(Some((key.clone(), output)))
    }))
}
pub(super) fn group() -> Project<Key, Row, Key, Row> {
    Project::new(|key: &Key, row: &Row| {
        let mut key = key.clone();
        key.pop().context("missing ordered aggregate index")?;
        Ok(Some((key, row.clone())))
    })
}
pub(super) fn finalize(spec: &Spec) -> Partition<Key, Row> {
    let spec = spec.clone();
    Partition::new_zero_preserving(move |_: &Key, rows: &Batch<(), Row>, work| {
        let mut output = Row::new();
        for (((), row), weight) in rows.iter() {
            work.charge(1)?;
            ensure!(*weight == 1, "ordered aggregate membership multiplicity");
            for (field, value) in row {
                if let Some(prior) = output.insert(field.clone(), value.clone()) {
                    ensure!(prior == *value, "conflicting ordered group fields");
                }
            }
        }
        ensure!(
            spec.aggregates.iter().all(|aggregate| output.contains_key(&aggregate.field)),
            "missing ordered aggregate output"
        );
        Batch::from_updates([(((), output), 1)])
    })
}

#[cfg(test)]
#[path = "ordered_tests.rs"]
mod tests;
