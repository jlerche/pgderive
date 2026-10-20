//! Durable integral prefix RANGE windows, with ordered data and additive trie nodes.
mod codec;
mod evaluate;
mod rows;
use super::relational::Key;
use crate::{
    compiler::partition::{Mode, Partition},
    engine::dataflow::{Binary, Expand, IndexedStatistics, Project, Statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use codec::{decode, encode, measure, node, path, width};
fn order(spec: &Partition, row: &Row) -> Result<u64> {
    let Mode::Peers { order, .. } = &spec.mode else {
        anyhow::bail!("prefix window requires peer mode")
    };
    let [order] = order.as_slice() else {
        anyhow::bail!("prefix window requires one order column")
    };
    let value = row
        .get(&order.column.name)
        .and_then(Option::as_deref)
        .context("NULL prefix window ordering")?
        .parse::<i64>()?;
    Ok(super::order::biased(value, false))
}
fn split(key: &Key) -> Result<(Key, u64)> {
    let mut base = key.clone();
    let token = base.pop().context("missing prefix ordering token")?;
    let value = u64::from_str_radix(&token, 16)?;
    ensure!(token == format!("{value:016x}"), "noncanonical prefix ordering token");
    Ok((base, value))
}
pub(super) fn input(spec: &Partition) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        let mut key = key.clone();
        key.push(format!("{:016x}", order(&spec, row)?));
        Ok(Some((key, row.clone())))
    })
}
pub(super) fn statistics(spec: &Partition) -> IndexedStatistics<Key, Row> {
    let spec = spec.clone();
    let size = width(&spec);
    let expand = Expand::new(move |key: &Key, row: &Row, emit| {
        let (base, value) = split(key)?;
        ensure!(value == order(&spec, row)?, "prefix ordering disagrees with retained row");
        let payload = encode(&measure(&spec, row)?);
        for prefix in path(value) {
            emit((node(&base, prefix), payload.clone()))?;
        }
        Ok(())
    });
    let stats = Statistics::new(
        size,
        move |row| decode(row, size),
        move |row| decode(row, size),
        move |_, values| {
            codec::validate(values)?;
            Ok((values[0] != 0.into()).then(|| encode(values)))
        },
    );
    IndexedStatistics::new(expand, stats)
}
pub(super) fn window(spec: &Partition) -> Binary<Key, Row> {
    let spec = spec.clone();
    Binary::new(move |input, work| {
        let spec = spec.clone();
        Box::pin(async move { evaluate::evaluate(&spec, input, work).await })
    })
}

#[cfg(test)]
mod codec_tests;
