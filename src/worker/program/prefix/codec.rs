use super::Key;
use crate::{
    compiler::partition::{Partition, statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
pub(super) const fn width(spec: &Partition) -> usize {
    statistics::width(spec.aggregates.len())
}
pub(super) use super::super::statistics::measure;
pub(super) fn encode(values: &[BigInt]) -> Row {
    super::super::statistics::encode_fields(values).collect()
}
pub(super) fn decode(row: &Row, size: usize) -> Result<Vec<BigInt>> {
    ensure!(row.len() == size, "prefix statistic width mismatch");
    super::super::statistics::decode_fields(row, size, "missing prefix statistic")
}
pub(super) fn validate(values: &[BigInt]) -> Result<()> {
    let rows = values.first().context("missing prefix rows")?;
    ensure!(rows >= &0.into(), "negative prefix rows");
    for pair in values[1..].chunks_exact(2) {
        ensure!(pair[0] >= 0.into() && &pair[0] <= rows, "invalid prefix non-NULL count");
        ensure!(pair[0] != 0.into() || pair[1] == 0.into(), "prefix sum without contributions");
    }
    Ok(())
}
pub(super) fn path(value: u64) -> Vec<(u32, u64)> {
    (0..=64).map(|depth| (depth, mask(value, depth))).collect()
}
const fn mask(value: u64, depth: u32) -> u64 {
    if depth == 0 { 0 } else { value & (u64::MAX << (64 - depth)) }
}
pub(super) fn cover(value: u64) -> Vec<(u32, u64)> {
    let mut nodes = Vec::new();
    for depth in 1..=64 {
        let bit = 1_u64 << (64 - depth);
        if value & bit != 0 {
            nodes.push((depth, mask(value, depth) ^ bit));
        }
    }
    nodes.push((64, value));
    nodes
}
pub(super) fn node(base: &Key, (depth, value): (u32, u64)) -> Key {
    let mut key = base.clone();
    key.push(format!("{depth:02x}:{value:016x}"));
    key
}
