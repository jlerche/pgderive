//! Retain locally constructed, validated immutable metadata after successful PUTs.
use super::{BatchData, BatchReader as _, BlockCache, ObjectBatch, WriteLimits};
use crate::engine::Batch;
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;

impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    pub(crate) async fn write_resident(
        store: Arc<dyn ObjectStore>,
        batch: &Batch<K, V>,
        schema: &str,
        target: (Option<&str>, usize),
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        // Both upload paths use the same byte/row boundaries and integrity checks.
        // This API returns one root; reject overflow rather than discard membership.
        let limits = WriteLimits {
            block_rows: target.1.min(65_536),
            output_roots: 1,
            ..WriteLimits::default()
        };
        let mut objects = Self::write_stream_resident(
            store,
            &mut batch.cursor().await?,
            schema,
            (target.0, limits),
            cache,
        )
        .await?;
        objects.pop().context("single-root upload returned no object")
    }
}

#[cfg(test)]
#[path = "tests/block_target.rs"]
mod block_target;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{reader::WriteLimits, tests::store::Store};
    use object_store::{ObjectStoreExt as _, path::Path};
    use std::sync::atomic::Ordering;
    #[tokio::test]
    async fn fresh_and_compacted_objects_need_no_read_back_but_cold_open_validates() -> Result<()> {
        let store = Arc::new(Store::default());
        let batch = Batch::from_updates([((1_i64, 2_i64), 3), ((4, 5), -1)])?;
        let cache = Arc::new(BlockCache::new(1_000_000, 100));
        let fresh = ObjectBatch::write_resident(
            store.clone(),
            &batch,
            "probe",
            (Some("pgderive/upload-v1/fresh"), 1),
            cache.clone(),
        )
        .await?;
        assert_eq!(store.puts.load(Ordering::SeqCst), 3);
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        assert_eq!(fresh.cursor().await?.current().map(|(_, _, n)| n), Some(3));
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let compacted = ObjectBatch::write_stream_resident(
            store.clone(),
            &mut fresh.cursor().await?,
            "probe",
            (Some("pgderive/upload-v1/compact"), WriteLimits::default()),
            cache,
        )
        .await?;
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let root = compacted.first().context("missing compacted object")?.reference().clone();
        assert_eq!(
            ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "probe")
                .await?
                .cursor()
                .await?
                .current()
                .map(|(_, _, n)| n),
            Some(3)
        );
        assert!(store.reads.load(Ordering::SeqCst) >= 3);
        store.delete(&Path::from(root.path())).await?;
        assert!(ObjectBatch::<i64, i64>::open(store, root, "probe").await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn compressed_payloads_pay_uncompressed_cache_charge_on_both_upload_paths() -> Result<()>
    {
        let store = Arc::new(Store::default());
        let batch = Batch::from_updates([((1_i64, "x".repeat(100_000)), 1)])?;
        let cache = Arc::new(BlockCache::new(1000, 100));
        let fresh = ObjectBatch::write_resident(
            store.clone(),
            &batch,
            "payload",
            (None, 16),
            cache.clone(),
        )
        .await?;
        assert_eq!(cache.stats()?.entries, 0);
        let compacted = ObjectBatch::write_stream_resident(
            store.clone(),
            &mut fresh.cursor().await?,
            "payload",
            (None, WriteLimits::default()),
            cache.clone(),
        )
        .await?;
        assert_eq!(cache.stats()?.entries, 0);
        let root = compacted.first().context("missing compacted object")?.reference().clone();
        let cold =
            ObjectBatch::<i64, String>::open_with_cache(store, root, "payload", cache.clone())
                .await?;
        let _cursor = cold.cursor().await?;
        assert_eq!(cache.stats()?.entries, 0);
        Ok(())
    }
}
