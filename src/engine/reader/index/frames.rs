//! Exact authenticated compressed nodes; namespace-independent content identity.
use super::super::format::hash;
use anyhow::{Result, ensure};
use std::sync::Arc;

type Identity = (String, u64);
#[derive(Debug)]
pub(super) struct Frames {
    cache: foyer::Cache<Identity, Arc<Vec<u8>>>,
    bytes: usize,
    minimum: usize,
}
impl Frames {
    pub(super) fn new(bytes: usize, entries: usize) -> Self {
        let minimum = bytes.div_ceil(entries.max(1));
        let cache = foyer::Cache::builder(bytes.max(1))
            .with_shards(1)
            .with_eviction_config(foyer::LruConfig::default())
            .with_weighter(move |identity: &Identity, bytes: &Arc<Vec<u8>>| {
                Self::charge(identity, bytes.len()).max(minimum)
            })
            .build();
        Self { cache, bytes: if entries == 0 { 0 } else { bytes }, minimum }
    }
    const fn charge(identity: &Identity, length: usize) -> usize {
        length.saturating_add(identity.0.len()).saturating_add(256)
    }
    pub(super) fn stats(&self) -> (usize, usize) {
        (self.cache.usage(), self.cache.entries())
    }
    pub(super) fn get(&self, digest: &str, length: u64) -> Option<Arc<Vec<u8>>> {
        self.cache.get(&(digest.into(), length)).map(|entry| entry.value().clone())
    }
    pub(super) fn insert(&self, digest: &str, length: u64, bytes: Arc<Vec<u8>>) -> Result<()> {
        ensure!(u64::try_from(bytes.len())? == length, "index frame length mismatch");
        ensure!(hash(&bytes) == digest, "index frame checksum mismatch");
        let identity = (digest.into(), length);
        if Self::charge(&identity, bytes.len()).max(self.minimum) <= self.bytes {
            drop(self.cache.insert(identity, bytes));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn clear(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_authenticated_frames_are_retained_under_weight_and_entry_caps() -> Result<()> {
        let frames = Frames::new(4096, 2);
        for key in 0..64_u8 {
            let bytes = vec![key; 64];
            let digest = hash(&bytes);
            frames.insert(&digest, 64, Arc::new(bytes.clone()))?;
            assert_eq!(frames.get(&digest, 64), Some(Arc::new(bytes)));
            assert_eq!(frames.get(&digest, 63), None);
            let (bytes, entries) = frames.stats();
            assert!(bytes <= 4096);
            assert!(entries <= 2);
        }
        let before = frames.stats();
        assert!(frames.insert(&hash(&[1, 2]), 1, Arc::new(vec![1, 2])).is_err());
        assert!(frames.insert(&hash(&[1, 2]), 2, Arc::new(vec![2, 1])).is_err());
        assert_eq!(frames.stats(), before);
        let disabled = Frames::new(0, 0);
        disabled.insert(&hash(&[1]), 1, Arc::new(vec![1]))?;
        assert_eq!(disabled.stats(), (0, 0));
        Ok(())
    }
}
