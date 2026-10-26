use crate::engine::{
    Batch,
    reader::{BatchReader as _, BlockCache, ObjectBatch},
    tests::store::Store,
};
use anyhow::{Context as _, Result};
use object_store::{ObjectStoreExt as _, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn growing_indexes_keep_only_bounded_metadata_and_preserve_navigation() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let remote: Arc<dyn object_store::ObjectStore> = store.clone();
    let cache = Arc::new(BlockCache::new(256 * 1024, 2));
    let mut root_charges = Vec::new();
    for rows in [128_i64, 2048] {
        let batch = Batch::from_updates(
            (0..rows).map(|key| ((key / 3, key), if key % 2 == 0 { 2 } else { -1 })),
        )?;
        let reference = ObjectBatch::write(store.clone(), &batch, "index-growth", 1).await?;
        let object = ObjectBatch::<i64, i64>::open_with_cache(
            store.clone(),
            reference,
            "index-growth",
            cache.clone(),
        )
        .await?;
        assert!(object.index.pack.is_some());
        assert!(object.head.is_none());
        let copy = object.clone();
        assert!(Arc::ptr_eq(&copy.index, &object.index));
        root_charges.push(object.resident_index_bytes()?);
        for key in (0..rows).step_by(31) {
            assert_eq!(
                object.tuple_weight(&(key / 3, key)).await?,
                if key % 2 == 0 { 2 } else { -1 }
            );
            assert_eq!(object.tuple_weight(&(key / 3, rows + key)).await?, 0);
            let stats = cache.stats()?;
            assert!(stats.index_bytes <= 64 * 1024 && stats.index_entries <= 2);
        }
        let mut cursor = object.cursor().await?;
        cursor.seek_key(&(rows / 6)).await?;
        assert_eq!(cursor.current().map(|(key, _, _)| *key), Some(rows / 6));
        let dependencies = object.reference().dependencies(&remote).await?;
        assert_eq!(dependencies.len(), 2);
    }
    assert!(root_charges.iter().all(|bytes| *bytes < 4096));
    Ok(())
}

#[tokio::test]
async fn authenticated_index_frames_reject_corruption_and_retry_safely() -> Result<()> {
    let mut backend = Store::unpaced();
    backend.read_delay = std::time::Duration::from_millis(5);
    let store = Arc::new(backend);
    let remote: Arc<dyn object_store::ObjectStore> = store.clone();
    let batch = Batch::from_updates((0..256_i64).map(|key| ((key, key), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "index-corrupt", 1).await?;
    let object =
        ObjectBatch::<i64, i64>::open(store.clone(), reference.clone(), "index-corrupt").await?;
    let pack = object.index.pack.as_ref().context("missing index pack")?;
    let path = Path::from(pack.path.clone());
    let original = store.get(&path).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    *corrupt.first_mut().context("empty index pack")? ^= 1;
    store.put(&path, corrupt.into()).await?;
    // Opening verifies only the authenticated root node, not unrelated leaf pages.
    let cold =
        ObjectBatch::<i64, i64>::open(store.clone(), reference.clone(), "index-corrupt").await?;
    assert!(cold.tuple_weight(&(0, 0)).await.is_err());
    assert!(reference.dependencies(&remote).await.is_err());
    store.put(&path, original.into()).await?;
    assert_eq!(cold.tuple_weight(&(0, 0)).await?, 1);
    cold.cache.index.clear_encoded();
    let reads = cold.cache.stats()?.index_reads;
    let clone = cold.clone();
    let (left, right) = tokio::join!(cold.tuple_weight(&(70, 70)), clone.tuple_weight(&(71, 71)));
    assert_eq!((left?, right?), (1, 1));
    assert_eq!(cold.cache.stats()?.index_reads - reads, 1);
    Ok(())
}

#[tokio::test]
async fn failed_index_page_load_preserves_cursor_position() -> Result<()> {
    use crate::engine::reader::index::Node;
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..256_i64).map(|key| ((key, key), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "index-cursor", 1).await?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), reference, "index-cursor").await?;
    let address = object.index.node.as_ref().context("missing index root")?;
    let node = object
        .cache
        .index
        .get::<(i64, i64)>(&address.hash, address.length)?
        .context("missing cached root")?;
    let Node::Branch(children) = node.node() else {
        anyhow::bail!("expected index branch");
    };
    let offset = usize::try_from(children.get(1).context("missing next index leaf")?.offset)?;
    let path = Path::from(object.index.pack.as_ref().context("missing index pack")?.path.clone());
    let original = store.get(&path).await?.bytes().await?;
    let mut cursor = object.cursor().await?;
    for _ in 0..31 {
        cursor.advance().await?;
    }
    assert_eq!(cursor.current(), Some((&31, &31, 1)));
    let mut corrupt = original.to_vec();
    *corrupt.get_mut(offset).context("missing next leaf byte")? ^= 1;
    store.put(&path, corrupt.into()).await?;
    object.cache.index.clear_encoded();
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&31, &31, 1)));
    store.put(&path, original.into()).await?;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&32, &32, 1)));
    Ok(())
}

#[tokio::test]
async fn wide_fences_remain_supported_without_resident_full_index() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..32_i64).map(|key| ((key, "x".repeat(10 * 1024)), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "wide-index", 1).await?;
    let object = ObjectBatch::<i64, String>::open(store, reference, "wide-index").await?;
    assert!(object.head.is_none());
    assert_eq!(object.tuple_weight(&(17, "x".repeat(10 * 1024))).await?, 1);
    Ok(())
}

#[tokio::test]
async fn foreign_selected_metadata_cannot_bypass_validation_through_head_reuse() -> Result<()> {
    use crate::engine::reader::Cursor;
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates([((1_i64, 2_i64), 3)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "selected-owner", 1).await?;
    let first =
        ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "selected-owner").await?;
    let page = first.key_page(&1).await?.context("missing selected page")?;
    let head = Cursor::object_key_page(
        first,
        &crate::engine::reader::KeyIdentity::new(&1)?,
        page.clone(),
        None,
    )
    .await?
    .context("missing reusable head")?;
    let other = ObjectBatch::<i64, i64>::open(store, root, "selected-owner").await?;
    assert!(
        Cursor::object_key_page(
            other,
            &crate::engine::reader::KeyIdentity::new(&1)?,
            page,
            Some(&head)
        )
        .await
        .is_err()
    );
    assert_eq!(head.current(), Some((&1, &2, 3)));
    Ok(())
}

#[tokio::test]
async fn opaque_gc_and_typed_readers_agree_on_inline_struct_field_order() -> Result<()> {
    #[derive(
        Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
    )]
    struct Record {
        z: i64,
        a: String,
    }
    let store = Arc::new(Store::unpaced());
    let remote: Arc<dyn object_store::ObjectStore> = store.clone();
    let row = Record { z: 7, a: "field order".into() };
    let batch = Batch::from_updates([((1_i64, row.clone()), 2)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "inline-record", 1).await?;
    let object =
        ObjectBatch::<i64, Record>::open(store, reference.clone(), "inline-record").await?;
    assert!(object.index.pack.is_none());
    assert_eq!(reference.dependencies(&remote).await?.len(), 1);
    assert_eq!(object.tuple_weight(&(1, row)).await?, 2);
    Ok(())
}

#[tokio::test]
async fn coarse_fences_exclude_unrelated_runs_without_index_io() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((100..356_i64).map(|key| ((key / 2, key), 1)))?;
    let root = ObjectBatch::write(store.clone(), &batch, "coarse-index", 1).await?;
    let mut cache = BlockCache::new(0, 0);
    cache.disable_index_retention();
    let cache = Arc::new(cache);
    let object =
        ObjectBatch::<i64, i64>::open_with_cache(store, root, "coarse-index", cache.clone())
            .await?;
    let before = cache.stats()?.index_reads;
    for key in [0, 49, 178, 1000] {
        assert!(object.key_page(&key).await?.is_none());
        assert_eq!(object.tuple_weight(&(key, -1)).await?, 0);
    }
    let tuples = [(0, 0), (49, 99), (178, 356), (1000, 2000)];
    let tuples = tuples.iter().collect::<Vec<_>>();
    let mut position = 0;
    assert!(object.next_weight_page(&tuples, &mut position).await?.is_none());
    assert_eq!(cache.stats()?.index_reads, before);
    // Inclusive boundaries retain every value of the same navigation key.
    assert!(object.key_page(&50).await?.is_some());
    assert_eq!(object.tuple_weight(&(50, 100)).await?, 1);
    assert_eq!(object.tuple_weight(&(50, 101)).await?, 1);
    assert!(object.key_page(&177).await?.is_some());
    assert_eq!(object.tuple_weight(&(177, 354)).await?, 1);
    assert_eq!(object.tuple_weight(&(177, 355)).await?, 1);
    Ok(())
}

#[tokio::test]
async fn generated_index_frames_are_reused_across_namespaces_and_cold_reopen_is_independent()
-> Result<()> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(4 * 1024 * 1024, 128));
    let input = Batch::from_updates((0..256_i64).map(|key| ((key, key * 2), 1)))?;
    let object = ObjectBatch::write_resident(
        store.clone(),
        &input,
        "qualified-index-warm",
        (Some("pgderive/upload-v1/index-warm"), 1),
        cache.clone(),
    )
    .await?;
    assert!(object.index.pack.is_some());
    assert!(cache.stats()?.index_encoded_bytes > 0);
    let mut pack = object.index.pack.clone().context("missing generated index pack")?;
    pack.path = object.reference().child_path(&pack.path);
    let other = ObjectBatch::write_resident(
        store.clone(),
        &input,
        "qualified-index-warm",
        (Some("pgderive/upload-v1/index-other"), 1),
        cache.clone(),
    )
    .await?;
    let before = cache.stats()?.index_reads;
    let root = object.index.node.as_ref().context("missing index address")?;
    let left = cache
        .index
        .read(&object.store, &pack, &root.hash, root.offset..root.offset + root.length)
        .await?;
    pack.path = other
        .reference()
        .child_path(&other.index.pack.as_ref().context("missing other pack")?.path);
    let right = cache
        .index
        .read(&other.store, &pack, &root.hash, root.offset..root.offset + root.length)
        .await?;
    assert!(Arc::ptr_eq(&left, &right));
    for key in [0_i64, 31, 128, 255] {
        assert_eq!(object.tuple_weight(&(key, key * 2)).await?, 1);
    }
    assert_eq!(cache.stats()?.index_reads, before);
    let cold_cache = Arc::new(BlockCache::new(4 * 1024 * 1024, 128));
    let cold = ObjectBatch::<i64, i64>::open_with_cache(
        store,
        object.reference().clone(),
        "qualified-index-warm",
        cold_cache.clone(),
    )
    .await?;
    assert!(cold_cache.stats()?.index_reads > 0);
    let mut cursor = cold.cursor().await?;
    for key in 0..256_i64 {
        assert_eq!(cursor.current(), Some((&key, &(key * 2), 1)));
        cursor.advance().await?;
    }
    assert!(cursor.current().is_none());
    Ok(())
}

#[tokio::test]
async fn full_scans_leave_foreground_index_admission_and_geometry_unchanged() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let input = Batch::from_updates((0..8192_i64).map(|key| ((key, key * 2), 1)))?;
    let mut roots = ObjectBatch::write_stream(
        store.clone(),
        &mut input.cursor().await?,
        "scan-index",
        crate::engine::reader::WriteLimits {
            block_rows: 1,
            index_blocks: 8192,
            output_roots: 1,
            ..crate::engine::reader::WriteLimits::default()
        },
    )
    .await?;
    let root = roots.pop().context("missing scan root")?;
    let cache = Arc::new(BlockCache::new(256 * 1024, 2));
    let mut object =
        ObjectBatch::<i64, i64>::open_with_cache(store, root, "scan-index", cache.clone()).await?;
    let before = cache.stats()?;
    object.cache = BlockCache::scan(cache.clone());
    let mut cursor = object.cursor().await?;
    for key in 0..8192_i64 {
        assert_eq!(cursor.current(), Some((&key, &(key * 2), 1)));
        cursor.advance().await?;
    }
    assert!(cursor.current().is_none());
    let after = cache.stats()?;
    assert_eq!(after.index_bytes, before.index_bytes);
    assert_eq!(after.index_entries, before.index_entries);
    assert!(after.index_reads > before.index_reads);
    assert!(
        after.index_reads - before.index_reads < 512,
        "{} metadata reads",
        after.index_reads - before.index_reads
    );
    let active = cursor.object_cache().context("missing active cursor cache")?;
    assert!(!Arc::ptr_eq(active, &object.cache));
    let scan = active.stats()?;
    assert!(scan.index_decoded_bytes > 0);
    assert!(scan.index_encoded_bytes > 0);
    assert!(scan.index_decoded_bytes <= 256 * 1024);
    assert!(scan.index_entries <= 17);
    assert!(scan.index_encoded_bytes <= 260 * 1024);
    assert!(scan.index_encoded_entries <= 1);
    Ok(())
}

#[tokio::test]
async fn warmed_typed_nodes_do_not_accept_a_wrong_authenticated_frame_length() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let input = Batch::from_updates((0..128_i64).map(|key| ((key, key), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &input, "typed-length", 1).await?;
    let object = ObjectBatch::<i64, i64>::open(store, reference, "typed-length").await?;
    assert_eq!(object.tuple_weight(&(0, 0)).await?, 1);
    let mut malformed = object.clone();
    let mut root = object.index.as_ref().clone();
    let address = root.node.as_mut().context("missing typed root")?;
    address.length = address.length.checked_sub(1).context("empty root frame")?;
    malformed.index = Arc::new(root);
    assert!(malformed.tuple_weight(&(0, 0)).await.is_err());
    assert_eq!(object.tuple_weight(&(0, 0)).await?, 1);
    Ok(())
}

#[tokio::test]
async fn prepared_pages_recheck_synopses_learned_after_metadata_selection() -> Result<()> {
    use crate::engine::reader::Cursor;
    let store = Arc::new(Store::unpaced());
    let input = Batch::from_updates([((2_i64, 0_i64), 1), ((4, 0), 1)])?;
    let root = ObjectBatch::write(store.clone(), &input, "late-synopsis", 4096).await?;
    let cache = Arc::new(BlockCache::new(64 * 1024, 1));
    let object =
        ObjectBatch::<i64, i64>::open_with_cache(store, root, "late-synopsis", cache.clone())
            .await?;
    let selected = object.key_page(&3).await?.context("missing prepared page")?;
    assert!(object.may_contain_page(&selected, &3)?);
    object.read(0).await?;
    cache.insert("other", "evict".into(), Arc::new(vec![((10_i64, 0_i64), 1)]), 16)?;
    let before = cache.stats()?;
    assert!(
        Cursor::object_key_page(
            object.clone(),
            &crate::engine::reader::KeyIdentity::new(&3)?,
            selected,
            None
        )
        .await?
        .is_none()
    );
    let after = cache.stats()?;
    assert_eq!(after.decoded_bytes, before.decoded_bytes);
    assert_eq!(after.foreground_io.gets, before.foreground_io.gets);
    assert_eq!(after.filter_rejections, before.filter_rejections + 1);
    let present = object.key_page(&4).await?.context("missing present page")?;
    assert!(
        Cursor::object_key_page(
            object,
            &crate::engine::reader::KeyIdentity::new(&4)?,
            present,
            None
        )
        .await?
        .is_some()
    );
    Ok(())
}
