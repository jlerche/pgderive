use super::{BatchData, Entry};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;

pub(super) const TARGET_OBJECT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pack {
    // Required field rename makes the global data-location codec a hard cutover.
    #[serde(rename = "file")]
    pub(super) path: String,
    pub(super) bytes: u64,
    pub(super) hash: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Block<T> {
    pub(super) pack: usize,
    pub(super) offset: u64,
    pub(super) first: T,
    pub(super) last: T,
    pub(super) length: u64,
    pub(super) rows: usize,
    pub(super) max_weight: u64,
    pub(super) hash: String,
}
pub(super) fn header_bytes(schema: &str) -> Result<usize> {
    // Covers fixed root/address framing; tuple fences are charged separately.
    super::bounded::encode(&super::index::Root::<()> {
        schema: schema.into(),
        pack: None,
        node: None,
        inline: None,
    })
    .map(|bytes| bytes.len() + 512)
}
pub(super) fn pack_bytes() -> Result<usize> {
    // Reserve the largest descriptor's decimal byte count before its digest is known.
    let digest = "0".repeat(64);
    super::bounded::encode(&Pack {
        path: format!("pgderive/pack/{digest}"),
        bytes: u64::try_from(super::compression::STORED_LIMIT)?,
        hash: digest,
    })
    .map(|bytes| bytes.len())
}
pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
            self.max_weight > 0 && self.max_weight <= i64::MIN.unsigned_abs(),
            "invalid immutable weight bound"
        );
        ensure!(valid_hash(&self.hash), "invalid immutable block checksum");
        Ok(())
    }
}
pub(super) fn row_count<T>(blocks: &[Block<T>]) -> Result<usize> {
    blocks.iter().try_fold(0_usize, |total, block| {
        total.checked_add(block.rows).context("object row count overflow")
    })
}
type EncodedBlock<T> = (Block<T>, Vec<u8>);
pub(super) fn encode_block<T: Clone, W: Borrow<i64>>(
    rows: &[(impl Borrow<T>, W)],
    decoded: &[u8],
) -> Result<EncodedBlock<T>> {
    let encoded = super::compression::compress(decoded)?;
    let block = Block {
        pack: 0,
        offset: 0,
        first: rows.first().context("empty immutable block")?.0.borrow().clone(),
        last: rows.last().context("empty immutable block")?.0.borrow().clone(),
        length: u64::try_from(encoded.len())?,
        rows: rows.len(),
        max_weight: rows
            .iter()
            .map(|(_, weight)| weight.borrow().unsigned_abs())
            .max()
            .unwrap_or(0),
        hash: hash(&encoded),
    };
    Ok((block, encoded))
}

impl Pack {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.bytes > 0
                && self.bytes <= u64::try_from(super::compression::STORED_LIMIT)?
                && valid_hash(&self.hash)
                && valid_pack_path(&self.path, &self.hash),
            "invalid immutable pack address/size"
        );
        Ok(())
    }
}

fn valid_pack_path(path: &str, hash: &str) -> bool {
    let canonical = format!("pgderive/pack/{hash}");
    if path == canonical {
        return true;
    }
    path.strip_suffix(&format!("/{canonical}")).is_some_and(super::object::valid_namespace)
}

#[cfg(test)]
mod location_tests {
    use super::*;

    #[test]
    fn old_relative_path_descriptor_is_rejected_instead_of_reinterpreted() -> Result<()> {
        let digest = "a".repeat(64);
        let legacy = serde_json::json!({
            "path": format!("pgderive/pack/{digest}"), "bytes": 1, "hash": digest,
        });
        assert!(serde_json::from_value::<Pack>(legacy).is_err());
        let current = Pack { path: format!("pgderive/pack/{digest}"), bytes: 1, hash: digest };
        let encoded = serde_json::to_value(&current)?;
        assert!(encoded.get("file").is_some());
        assert!(encoded.get("path").is_none());
        Ok(())
    }

    #[test]
    fn pack_locations_require_canonical_hashes_and_valid_upload_namespaces() -> Result<()> {
        let digest = "a".repeat(64);
        let relative = format!("pgderive/pack/{digest}");
        for path in [&relative, &format!("pgderive/upload-v1/good-1/{relative}")] {
            Pack { path: path.clone(), bytes: 1, hash: digest.clone() }.validate()?;
        }
        for path in [
            format!("other/{relative}"),
            format!("pgderive/upload-v1//{relative}"),
            format!("pgderive/upload-v1/../{relative}"),
            format!("pgderive/upload-v1/bad/nested/{relative}"),
            format!("pgderive/upload-v1/good-1/{relative}/extra"),
            format!("pgderive/upload-v1/good-1/pgderive/pack/{}", "b".repeat(64)),
        ] {
            assert!(Pack { path, bytes: 1, hash: digest.clone() }.validate().is_err());
        }
        Ok(())
    }
}
