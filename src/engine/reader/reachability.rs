use super::{ObjectRef, bounded, format::Index};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

impl ObjectRef {
    /// Validate a trusted root and discover its direct immutable storage dependencies.
    /// Fine-grained block addresses remain inside object-local indexes. GC must retain
    /// the root itself as well as every returned dependency; a failed read forbids sweep.
    ///
    /// # Errors
    /// Rejects missing/corrupt roots, incompatible indexes or invalid child addresses.
    pub async fn dependencies(&self, store: &Arc<dyn ObjectStore>) -> Result<Vec<String>> {
        let bytes = self.index_bytes(store).await?;
        let index: Index<serde_json::Value, serde_json::Value> = serde_json::from_slice(&bytes)?;
        ensure!(
            matches!(index.version, 1 | 2) && index.schema == self.schema(),
            "invalid reachability root schema/version"
        );
        ensure!(index.version != 2 || self.index_offset() == 0, "invalid immutable root range");
        let mut paths = Vec::new();
        let mut offset = 0_u64;
        for block in index.blocks {
            ensure!(
                block.rows > 0
                    && block.length > 0
                    && block.length <= u64::try_from(bounded::MAX_BYTES)?,
                "invalid reachability block size"
            );
            if index.version == 1 {
                ensure!(
                    block.path.is_none() && block.offset == offset,
                    "invalid legacy block address"
                );
                offset = offset.checked_add(block.length).context("legacy block range overflow")?;
            } else {
                ensure!(
                    block.offset == 0
                        && block.rows <= 65_536
                        && block.hash.len() == 64
                        && block.hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "invalid immutable block metadata"
                );
                let expected = format!("pgderive/block-v2/{}", block.hash);
                ensure!(
                    block.path.as_deref() == Some(expected.as_str()),
                    "invalid reachability child path"
                );
                paths.push(self.child_path(&expected));
            }
        }
        ensure!(
            index.version != 1 || offset == self.index_offset(),
            "invalid legacy index boundary"
        );
        Ok(paths)
    }
}
