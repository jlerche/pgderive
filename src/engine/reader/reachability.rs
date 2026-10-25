use super::{ObjectRef, format::Index};
use anyhow::{Result, ensure};
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
        let index: Index<serde::de::IgnoredAny, serde::de::IgnoredAny> =
            serde_json::from_slice(&bytes)?;
        ensure!(index.schema == self.schema(), "invalid reachability root schema");
        index.validate_storage()?;
        Ok(index.packs.iter().map(|data| self.child_path(&data.path)).collect())
    }
}
