use super::{BatchData, Entry};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;

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

pub(super) fn row_count<T>(blocks: &[Block<T>]) -> Result<usize> {
    blocks.iter().try_fold(0_usize, |total, block| {
        total.checked_add(block.rows).context("object row count overflow")
    })
}

type EncodedBlock<T> = (Block<T>, Vec<u8>);

pub(super) fn encode_block<T: Clone, W>(
    rows: &[(impl Borrow<T>, W)],
    decoded: &[u8],
) -> Result<EncodedBlock<T>> {
    let encoded = super::compression::compress(decoded)?;
    let digest = hash(&encoded);
    let block = Block {
        path: format!("pgderive/block/{digest}"),
        first: rows.first().context("empty immutable block")?.0.borrow().clone(),
        last: rows.last().context("empty immutable block")?.0.borrow().clone(),
        length: u64::try_from(encoded.len())?,
        rows: rows.len(),
        hash: digest,
    };
    Ok((block, encoded))
}
