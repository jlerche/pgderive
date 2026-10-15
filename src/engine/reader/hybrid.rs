use super::format::hash;
use anyhow::{Context, Result, ensure};
use foyer::{
    BlockEngineConfig, DeviceBuilder as _, FsDeviceBuilder, HybridCache, PsyncIoEngineConfig,
};
use serde::Deserialize;
use std::{fs::File, path::PathBuf, sync::Arc};

/// Optional disposable encoded-block cache. Use a dedicated directory per worker.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Dedicated local directory; never put authoritative data here.
    pub directory: PathBuf,
    /// Memory capacity in encoded bytes, separate from the decoded-block budget.
    pub memory_bytes: usize,
    /// Local disk capacity in bytes (at least 64 MiB, a multiple of 16 MiB).
    pub disk_bytes: usize,
}
/// Foyer memory/disk cache of immutable encoded blocks, never authoritative state.
#[derive(Debug)]
pub struct EncodedCache {
    cache: HybridCache<String, Vec<u8>>,
    // Retain ownership until the hybrid cache has been dropped/closed.
    ownership: Arc<File>,
}
impl CacheConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.directory.as_os_str().is_empty()
                && self.memory_bytes > 0
                && self.disk_bytes >= 64 * 1024 * 1024
                && self.disk_bytes.is_multiple_of(16 * 1024 * 1024),
            "invalid Foyer cache directory or capacity"
        );
        Ok(())
    }
    /// Open an exclusively owned cache directory. Existing cache contents are disposable.
    ///
    /// # Errors
    /// Returns invalid configuration, directory ownership or Foyer initialization errors.
    pub async fn open(&self) -> Result<EncodedCache> {
        self.validate()?;
        std::fs::create_dir_all(&self.directory)?;
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join("pgderive.lock"))?;
        lock.try_lock().context("Foyer directory already owned by another worker")?;
        let lock = Arc::new(lock);
        let ownership = lock.clone();
        let device = FsDeviceBuilder::new(self.directory.join("blocks"))
            .with_capacity(self.disk_bytes)
            .build()?;
        let cache = HybridCache::<String, Vec<u8>>::builder()
            .with_name("pgderive-encoded-v1")
            .memory(self.memory_bytes)
            .with_weighter(move |key: &String, value: &Vec<u8>| charge(&ownership, key, value))
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(16 * 1024 * 1024))
            .build()
            .await?;
        Ok(EncodedCache { cache, ownership: lock })
    }
}
impl EncodedCache {
    pub(super) async fn get(&self, key: &str, digest: &str) -> Option<Vec<u8>> {
        let entry = self.cache.get(key).await.ok().flatten()?;
        if hash(entry.value()) != digest {
            self.cache.remove(key);
            return None;
        }
        Some(entry.value().clone())
    }
    pub(super) fn insert(&self, key: String, value: Vec<u8>) {
        self.cache.insert(key, value);
    }
    /// Flush pending cache writes and stop disk workers before directory reuse.
    ///
    /// # Errors
    /// Returns local cache I/O failures; authoritative object data is unaffected.
    pub async fn close(&self) -> Result<()> {
        let cache = self.cache.clone();
        let ownership = self.ownership.clone();
        // A canceled caller must not cancel Foyer after it marks itself closed.
        tokio::spawn(async move {
            let result = cache.close().await;
            drop(ownership);
            result
        })
        .await
        .context("Foyer shutdown task failed")??;
        Ok(())
    }
}

// Foyer clones its memory cache into asynchronous shutdown. The weighter owns
// this lease too, so dropping our wrapper cannot unlock before shutdown finishes.
const fn charge(_ownership: &File, key: &str, value: &[u8]) -> usize {
    key.len().saturating_add(value.len())
}
#[cfg(test)]
#[path = "tests/hybrid.rs"]
mod tests;
