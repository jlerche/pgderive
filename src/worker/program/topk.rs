//! Ordered occurrence top-k for deterministic `PostgreSQL` `ROW_NUMBER` cutoffs.
use super::relational::Key;
use crate::{
    compiler::topk::Spec,
    engine::dataflow::{OrderedTopK, Project},
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
        let value = match order.column.oid {
            1114 | 1184 => crate::temporal::Timestamp::parse(value, order.column.oid)?.sort_value(),
            20 | 21 | 23 => value.parse::<i64>()?,
            _ => anyhow::bail!("unsupported top-k ordering type"),
        };
        let biased = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
        let biased = if order.descending { !biased } else { biased };
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
pub(super) fn operator(spec: &Spec) -> OrderedTopK<Key, Row> {
    let spec = spec.clone();
    OrderedTopK::new(spec.limit, move |key: &Key, row: &Row, position| {
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
