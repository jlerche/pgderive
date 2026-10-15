use super::{ObjectBatch, ObjectRef, hash};
use crate::engine::reader::BatchReader as _;
use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
use std::sync::Arc;

async fn root(store: &Arc<dyn ObjectStore>, bytes: Vec<u8>) -> Result<ObjectRef> {
    let reference = ObjectRef {
        path: format!("pgderive/batch/{}", hash(&bytes)),
        bytes: u64::try_from(bytes.len())?,
        hash: hash(&bytes),
        schema: "tuple".into(),
        namespace: None,
    };
    store.put(&Path::from(reference.path()), bytes.into()).await?;
    Ok(reference)
}

#[tokio::test]
async fn sole_format_rejects_old_layouts_and_plain_json() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = serde_json::to_vec(&vec![((1_i64, 2_i64), -3_i64)])?;
    let digest = hash(&data);
    let child = format!("pgderive/block/{digest}");
    let index = serde_json::json!({"schema": "tuple", "blocks": [{
        "path": child, "first": [1, 2], "last": [1, 2],
        "length": data.len(), "rows": 1, "hash": digest
    }]});
    store.put(&Path::from(child), data.into()).await?;
    let reference = root(&store, serde_json::to_vec(&index)?).await?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), reference.clone(), "tuple").await?;
    assert!(object.cursor().await.is_err());
    for version in [1, 2, 3] {
        let mut old = index.clone();
        old["version"] = version.into();
        let invalid = root(&store, serde_json::to_vec(&old)?).await?;
        assert!(
            ObjectBatch::<i64, i64>::open(store.clone(), invalid.clone(), "tuple").await.is_err()
        );
        assert!(invalid.dependencies(&store).await.is_err());
        let mut old_path = reference.clone();
        old_path.path = format!("pgderive/batch-v{version}/{}", old_path.hash);
        assert!(
            ObjectBatch::<i64, i64>::open(store.clone(), old_path.clone(), "tuple").await.is_err()
        );
        assert!(old_path.dependencies(&store).await.is_err());
    }
    let mut obsolete = index.clone();
    obsolete["blocks"][0]["offset"] = 0.into();
    let invalid = root(&store, serde_json::to_vec(&obsolete)?).await?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), invalid, "tuple").await.is_err());
    let mut obsolete = serde_json::to_value(reference)?;
    obsolete["index_offset"] = 0.into();
    assert!(serde_json::from_value::<ObjectRef>(obsolete).is_err());
    Ok(())
}
