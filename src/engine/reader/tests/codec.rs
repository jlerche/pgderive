use super::{ObjectBatch, ObjectRef, hash};
use crate::engine::reader::BatchReader as _;
use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn separate_v2_json_blocks_remain_readable() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = serde_json::to_vec(&vec![((1_i64, 2_i64), -3_i64)])?;
    let digest = hash(&data);
    let child = format!("pgderive/block-v2/{digest}");
    let bytes = serde_json::to_vec(&serde_json::json!({
        "version": 2, "schema": "legacy", "blocks": [{
            "path": child, "first": [1, 2], "last": [1, 2], "offset": 0,
            "length": data.len(), "rows": 1, "hash": digest
        }]
    }))?;
    for namespace in [None, Some("pgderive/upload-v1/legacy")] {
        let mut reference = ObjectRef {
            path: format!("pgderive/batch-v2/{}", hash(&bytes)),
            bytes: u64::try_from(bytes.len())?,
            index_offset: 0,
            index_length: u64::try_from(bytes.len())?,
            index_hash: hash(&bytes),
            schema: "legacy".into(),
            namespace: None,
        };
        if let Some(namespace) = namespace {
            reference = reference.qualify(namespace)?;
        }
        let path = reference.child_path(&child);
        store.put(&Path::from(path.clone()), data.clone().into()).await?;
        store.put(&Path::from(reference.path()), bytes.clone().into()).await?;
        assert_eq!(reference.dependencies(&store).await?, vec![path]);
        let object = ObjectBatch::<i64, i64>::open(store.clone(), reference, "legacy").await?;
        let mut cursor = object.cursor().await?;
        assert_eq!(cursor.current(), Some((&1, &2, -3)));
        cursor.advance().await?;
        assert!(cursor.current().is_none());
    }
    Ok(())
}
