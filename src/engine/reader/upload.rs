//! Retain locally constructed, validated immutable metadata after successful PUTs.
use super::{
    BatchData, BlockCache, ObjectBatch, bounded, compression,
    format::{Block, hash},
};
use crate::engine::Batch;
use anyhow::{Context, Result, ensure};
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
        let (namespace, block_rows) = target;
        ensure!(!schema.is_empty() && block_rows > 0, "invalid batch schema or block size");
        if let Some(namespace) = namespace {
            ensure!(super::object::valid_namespace(namespace), "invalid batch upload namespace");
        }
        let scoped: Arc<dyn ObjectStore> = namespace.map_or_else(
            || store.clone(),
            |namespace| {
                let scoped: Arc<dyn ObjectStore> =
                    Arc::new(object_store::prefix::PrefixStore::new(store.clone(), namespace));
                scoped
            },
        );
        let mut blocks = Vec::new();
        let mut fence_bytes = 0_usize;
        let mut rows = batch.iter();
        loop {
            let chunk = rows.by_ref().take(block_rows.min(65_536)).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            let decoded_bytes = bounded::encode(&chunk)?;
            let encoded_bytes = decoded_bytes.len();
            let encoded = compression::compress(&decoded_bytes)?;
            let digest = hash(&encoded);
            let path = format!("pgderive/block/{digest}");
            bounded::put(&scoped, &path, encoded.clone()).await?;
            let block = Block {
                path,
                first: chunk.first().context("empty chunk")?.0.clone(),
                last: chunk.last().context("empty chunk")?.0.clone(),
                length: u64::try_from(encoded.len())?,
                rows: chunk.len(),
                hash: digest.clone(),
            };
            fence_bytes = fence_bytes
                .checked_add(bounded::encode(&block)?.len())
                .context("manifest size overflow")?;
            ensure!(fence_bytes <= bounded::MAX_BYTES, "manifest exceeds byte limit");
            let decoded =
                chunk.into_iter().map(|(tuple, weight)| (tuple.clone(), *weight)).collect();
            cache.insert(schema, digest, Arc::new(decoded), encoded_bytes)?;
            blocks.push(block);
        }
        // A successful index PUT remains mandatory. Only the redundant cold
        // reopen is avoided; arbitrary recovered references still use open().
        let mut reference = Self::upload_index(&scoped, schema, blocks.clone()).await?;
        if let Some(namespace) = namespace {
            reference = reference.qualify(namespace)?;
        }
        for block in &mut blocks {
            block.path = reference.child_path(&block.path);
        }
        Ok(Self { store, reference, blocks: Arc::new(blocks), cache })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        reader::{BatchReader as _, WriteLimits},
        tests::store::Store,
    };
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
