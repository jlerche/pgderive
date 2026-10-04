use super::{BatchData, Entry};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Block<T> {
    pub(super) first: T,
    pub(super) last: T,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) rows: usize,
    pub(super) hash: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Index<K, V> {
    pub(super) version: u32,
    pub(super) schema: String,
    pub(super) blocks: Vec<Block<(K, V)>>,
}

pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn validate<K: BatchData, V: BatchData>(rows: &[Entry<K, V>]) -> Result<()> {
    ensure!(rows.iter().all(|(_, weight)| *weight != 0), "zero weight in immutable batch");
    ensure!(
        rows.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "noncanonical immutable batch order"
    );
    Ok(())
}
