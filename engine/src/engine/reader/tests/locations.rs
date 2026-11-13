use super::*;
use crate::engine::Batch;
use object_store::memory::InMemory;

#[tokio::test]
async fn replacement_root_authenticates_data_in_an_older_upload_namespace() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates((0..8_i64).map(|key| ((key, key + 1), 1)))?;
    let schema = "qualified-data-location";
    let source = ObjectBatch::write_namespaced(
        store.clone(),
        &batch,
        schema,
        ("pgderive/upload-v1/source-location", 2),
    )
    .await?;
    let original = ObjectBatch::<i64, i64>::open(store.clone(), source.clone(), schema).await?;
    let namespace = "pgderive/upload-v1/replacement-location";
    let (replacement, _, _) = ObjectBatch::<i64, i64>::upload_index(
        &scoped_store(&store, Some(namespace)),
        schema,
        (original.test_packs().await?, original.test_blocks().await?),
        super::super::bounded::MAX_BYTES,
        None,
    )
    .await?;
    let replacement = replacement.qualify(namespace)?;
    let original_dependencies = source.dependencies(&store).await?;
    let replacement_dependencies = replacement.dependencies(&store).await?;
    assert_eq!(original_dependencies, replacement_dependencies);
    assert!(
        replacement_dependencies
            .iter()
            .all(|path| path.starts_with("pgderive/upload-v1/source-location/"))
    );
    store.delete(&Path::from(source.path())).await?;
    let reopened =
        ObjectBatch::<i64, i64>::open(store.clone(), replacement.clone(), schema).await?;
    let mut cursor = reopened.cursor().await?;
    for key in 0..8_i64 {
        assert_eq!(cursor.current(), Some((&key, &(key + 1), 1)));
        cursor.advance().await?;
    }
    assert!(cursor.current().is_none());
    let (path, _) = reopened.block_range(0).await?.context("missing shared block")?;
    store.put(&Path::from(path), vec![0_u8; 10].into()).await?;
    let cold = ObjectBatch::<i64, i64>::open(store, replacement, schema).await?;
    assert!(cold.cursor().await.is_err());
    Ok(())
}
