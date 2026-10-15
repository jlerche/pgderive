use super::{BatchData, Entry};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Block<T> {
    pub(super) path: String,
    pub(super) first: T,
    pub(super) last: T,
    pub(super) length: u64,
    pub(super) rows: usize,
    pub(super) hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Index<K, V> {
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

impl<T> Block<T> {
    pub(super) fn validate_address(&self) -> Result<()> {
        ensure!(
            self.rows > 0
                && self.rows <= 65_536
                && self.length > 0
                && self.length <= u64::try_from(super::compression::STORED_LIMIT)?,
            "invalid immutable block size"
        );
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && self.path == format!("pgderive/block/{}", self.hash),
            "invalid immutable block address"
        );
        Ok(())
    }
}
