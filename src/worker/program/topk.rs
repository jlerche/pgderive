//! Ordered occurrence top-k for deterministic `PostgreSQL` `ROW_NUMBER` cutoffs.
use super::relational::Key;
use crate::{
    compiler::topk::Spec,
    engine::dataflow::{OrderedSelection, Project},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use std::fmt::Write;
const ORDER: &str = "\0ordered";
fn encoding(spec: &Spec, row: &Row) -> Result<String> {
    let mut token = String::new();
    for order in &spec.order {
        let value = row.get(&order.column.name).context("missing top-k ordering field")?;
        let Some(value) = value else {
            token.push(if order.nulls_first { '0' } else { '2' });
            continue;
        };
        token.push('1');
        ensure!(
            matches!(order.column.oid, 20 | 21 | 23 | 1114 | 1184),
            "unsupported top-k ordering type"
        );
        let value = super::order::value(value, order.column.oid)?;
        let biased = super::order::biased(value, order.descending);
        write!(token, "{biased:016x}")?;
    }
    Ok(token)
}
pub(super) fn input(spec: &Spec) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        ensure!(row.keys().all(|field| !field.starts_with('\0')), "top-k internal field collision");
        let mut indexed = row.clone();
        indexed.insert(ORDER.into(), Some(encoding(&spec, row)?));
        Ok(Some((key.clone(), indexed)))
    })
}
pub(super) fn operator(spec: &Spec) -> OrderedSelection<Key, Row> {
    let spec = spec.clone();
    OrderedSelection::new(spec.limit, move |key: &Key, row: &Row, position| {
        ensure!(
            row.get(ORDER) == Some(&Some(encoding(&spec, row)?)),
            "invalid top-k identity codec"
        );
        let mut output = row.clone();
        output.remove(ORDER);
        ensure!(
            output.insert(spec.field.clone(), Some(i64::try_from(position)?.to_string())).is_none(),
            "top-k result field collision"
        );
        Ok((key.clone(), output))
    })
}

#[cfg(test)]
#[path = "topk_codec_tests.rs"]
mod tests;
