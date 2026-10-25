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
    let child = format!("pgderive/pack/{digest}");
    let index = serde_json::json!({"schema": "tuple", "packs":[{"path":child,"bytes":data.len(),"hash":digest}], "blocks": [{
        "pack":0, "offset": 0, "first": [1, 2], "last": [1, 2],
        "length": data.len(), "rows": 1, "max_weight": 3, "hash": digest
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
    let mut missing = index.clone();
    missing["blocks"][0]
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("invalid block fixture"))?
        .remove("max_weight");
    let invalid = root(&store, serde_json::to_vec(&missing)?).await?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), invalid.clone(), "tuple").await.is_err());
    assert!(invalid.dependencies(&store).await.is_err());
    let mut obsolete = index.clone();
    obsolete.as_object_mut().ok_or_else(|| anyhow::anyhow!("invalid fixture"))?.remove("packs");
    obsolete["blocks"][0]["path"] = "pgderive/block/old".into();
    let invalid = root(&store, serde_json::to_vec(&obsolete)?).await?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), invalid, "tuple").await.is_err());
    let empty_old = root(&store, br#"{"schema":"tuple","blocks":[]}"#.to_vec()).await?;
    assert!(
        ObjectBatch::<i64, i64>::open(store.clone(), empty_old.clone(), "tuple").await.is_err()
    );
    assert!(empty_old.dependencies(&store).await.is_err());
    let mut obsolete = serde_json::to_value(reference)?;
    obsolete["index_offset"] = 0.into();
    assert!(serde_json::from_value::<ObjectRef>(obsolete).is_err());
    Ok(())
}

#[tokio::test]
async fn malformed_packed_ranges_fail_open_and_gc_dependency_discovery() -> Result<()> {
    use crate::engine::Batch;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates((0..4).map(|n| ((n, n), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "tuple", 1).await?;
    let bytes = store.get(&Path::from(reference.path())).await?.bytes().await?;
    let index: serde_json::Value = serde_json::from_slice(&bytes)?;
    let mut invalid = Vec::new();
    for (field, value) in [
        ("offset", 1_u64),
        ("length", 0),
        ("rows", 65_537),
        ("max_weight", 0),
        ("max_weight", u64::MAX),
    ] {
        let mut bad = index.clone();
        bad["blocks"][0][field] = value.into();
        invalid.push(bad);
    }
    let mut bad = index.clone();
    bad["blocks"][1]["offset"] = 0.into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["blocks"][1]["offset"] = u64::MAX.into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["blocks"][0]["hash"] = "invalid".into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["packs"][0]["bytes"] = (16 * 1024 * 1024).into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["packs"][0]["path"] = "../unmanaged".into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["packs"][0]["hash"] = "invalid".into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["packs"] = serde_json::Value::Null;
    invalid.push(bad);
    let mut bad = index.clone();
    bad["blocks"] = serde_json::json!([]);
    invalid.push(bad);
    let mut bad = index.clone();
    bad["blocks"][0]["pack"] = 1.into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["blocks"][1]["pack"] = u64::MAX.into();
    invalid.push(bad);
    let mut bad = index.clone();
    bad["packs"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("missing packs"))?
        .push(index["packs"][0].clone());
    invalid.push(bad);
    let mut bad = index;
    bad["packs"][0]["bytes"] =
        (bad["packs"][0]["bytes"].as_u64().ok_or_else(|| anyhow::anyhow!("missing size"))? + 1)
            .into();
    invalid.push(bad);
    for index in invalid {
        let reference = root(&store, serde_json::to_vec(&index)?).await?;
        assert!(
            ObjectBatch::<i32, i32>::open(store.clone(), reference.clone(), "tuple").await.is_err()
        );
        assert!(reference.dependencies(&store).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn declared_weight_bound_is_rechecked_for_cold_and_cached_rows() -> Result<()> {
    use crate::engine::{Batch, reader::BlockCache};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reference = ObjectBatch::write(
        store.clone(),
        &Batch::from_updates([((1_i64, 2_i64), -7), ((3, 4), 1)])?,
        "tuple",
        2,
    )
    .await?;
    let mut object = ObjectBatch::<i64, i64>::open_with_cache(
        store,
        reference,
        "tuple",
        Arc::new(BlockCache::new(4096, 4)),
    )
    .await?;
    Arc::make_mut(&mut object.blocks)[0].max_weight = 1;
    assert!(object.read(0).await.is_err());
    assert_eq!(object.cache.stats()?.entries, 0);
    assert_eq!(object.cache.stats()?.filter_entries, 0);
    Arc::make_mut(&mut object.blocks)[0].max_weight = 7;
    object.read(0).await?;
    assert_eq!(object.tuple_weight(&(2, 0)).await?, 0);
    Arc::make_mut(&mut object.blocks)[0].max_weight = 8;
    assert!(object.tuple_weight(&(2, 0)).await.is_err());
    assert!(object.read(0).await.is_err());
    Ok(())
}
