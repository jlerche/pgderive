use super::{Run, Trace};
use crate::engine::{
    Batch,
    reader::{BatchReader, ObjectBatch},
};
use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

fn memory(rows: impl IntoIterator<Item = ((i64, i64), i64)>) -> Result<Run<i64, i64>> {
    Ok(Run::Memory(Arc::new(Batch::from_updates(rows)?)))
}

#[tokio::test]
async fn snapshots_layouts_seeks_and_compaction_are_equivalent() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates((0..30).map(|n| ((n / 6, n), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "i64-pair-v1", 3).await?;
    let object = ObjectBatch::open(store.clone(), reference.clone(), "i64-pair-v1").await?;
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(object.clone()), 1).await?)?;
    let old = trace.snapshot();
    let change = memory([((1, 6), -1), ((1, 8), 3)])?;
    trace.commit(trace.prepare(change, 2).await?)?;
    let current = trace.snapshot().materialize().await?;
    assert_eq!(old.materialize().await?, batch);
    let mut cursor = trace.snapshot().cursor().await?;
    cursor.seek_key(&1).await?;
    assert_eq!(cursor.current(), Some((&1, &7, 1)));
    cursor.seek_key(&99).await?;
    assert!(cursor.current().is_none());
    cursor.seek_key(&-1).await?;
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    let replacement = ObjectBatch::write(store.clone(), &current, "i64-pair-v1", 7).await?;
    trace.commit(
        trace
            .prepare_compaction(vec![Run::Object(
                ObjectBatch::open(store.clone(), replacement, "i64-pair-v1").await?,
            )])
            .await?,
    )?;
    assert_eq!(trace.snapshot().time(), 2);
    assert_eq!(trace.snapshot().generation(), 3);
    assert_eq!(trace.snapshot().materialize().await?, current);
    assert_eq!(old.materialize().await?, batch);
    // Repeated PUTs are idempotent and verify existing immutable bytes.
    assert_eq!(
        ObjectBatch::write(store.clone(), &batch, "i64-pair-v1", 3).await?.path(),
        reference.path()
    );
    assert!(ObjectBatch::<i64, i64>::open(store, reference, "wrong-schema").await.is_err());
    Ok(())
}

#[tokio::test]
async fn stale_foreign_invalid_and_overflow_prepares_leave_state_unchanged() -> Result<()> {
    let mut trace = Trace::default();
    let first = trace.prepare(memory([((0, 0), i64::MAX)])?, 1).await?;
    let stale = trace.prepare(memory([((0, 1), 1)])?, 1).await?;
    trace.commit(first)?;
    assert!(trace.commit(stale).is_err());
    assert!(trace.prepare(memory([((0, 0), 1)])?, 2).await.is_err());
    assert!(trace.prepare(memory([])?, 9).await.is_err());
    assert!(trace.prepare_compaction(vec![memory([])?]).await.is_err());
    let before = trace.snapshot().materialize().await?;
    let foreign = Trace::default().prepare(memory([])?, 1).await?;
    assert!(trace.commit(foreign).is_err());
    assert_eq!(trace.snapshot().materialize().await?, before);
    trace.commit(trace.prepare(memory([((0, 0), -i64::MAX)])?, 2).await?)?;
    assert!(trace.snapshot().materialize().await?.iter().next().is_none());
    Ok(())
}

#[tokio::test]
async fn missing_and_corrupt_objects_fail_closed_without_cursor_movement() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1, 1), 1), ((2, 2), 1)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "test", 1).await?;
    let object = ObjectBatch::open(store.clone(), reference.clone(), "test").await?;
    let mut cursor = object.cursor().await?;
    let path = Path::from(object.block_path(1).ok_or_else(|| anyhow::anyhow!("missing block"))?);
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    assert!(cursor.seek_key(&2).await.is_err());
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    store.put(&path, bytes.clone().into()).await?;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&2, &2, 1)));
    let mut corrupted = bytes.to_vec();
    corrupted[0] ^= 1;
    store.put(&path, corrupted.into()).await?;
    assert!(cursor.seek_key(&2).await.is_err());
    let root = Path::from(reference.path());
    let index = store.get(&root).await?.bytes().await?;
    store.delete(&root).await?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), reference.clone(), "test").await.is_err());
    let mut bad_index = index.to_vec();
    let last = bad_index.last_mut().ok_or_else(|| anyhow::anyhow!("empty fixture"))?;
    *last ^= 1;
    store.put(&root, bad_index.into()).await?;
    assert!(ObjectBatch::<i64, i64>::open(store, reference, "test").await.is_err());
    Ok(())
}

#[tokio::test]
async fn memory_cursor_and_empty_object_have_same_contract() -> Result<()> {
    let batch = Batch::from_updates([((1, 1), 1), ((1, 2), -1), ((3, 3), 2)])?;
    let mut cursor = batch.cursor().await?;
    cursor.seek_key(&2).await?;
    assert_eq!(cursor.current(), Some((&3, &3, 2)));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    cursor.seek_key(&1).await?;
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    let store = Arc::new(InMemory::new());
    let empty = Batch::<i64, i64>::from_updates([])?;
    assert!(ObjectBatch::write(store.clone(), &empty, "", 1).await.is_err());
    let reference = ObjectBatch::write(store.clone(), &empty, "empty", 1).await?;
    let object = ObjectBatch::<i64, i64>::open(store, reference, "empty").await?;
    let mut cursor = object.cursor().await?;
    cursor.advance().await?;
    cursor.seek_key(&0).await?;
    assert!(cursor.current().is_none());
    Ok(())
}

#[tokio::test]
async fn filesystem_reopen_preserves_full_weighted_state() -> Result<()> {
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!("pgderive-trace-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&root)?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root)?);
    let batch = Batch::from_updates([((1, 2), 3), ((1, 3), -2), ((-1, 1), i64::MIN)])?;
    let reference = ObjectBatch::write(store, &batch, "disk-v1", 1).await?;
    let catalog = serde_json::to_vec(&reference)?;
    let fresh: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root)?);
    let reopened =
        ObjectBatch::<i64, i64>::open(fresh, serde_json::from_slice(&catalog)?, "disk-v1").await?;
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(reopened), 1).await?)?;
    assert_eq!(trace.snapshot().materialize().await?, batch);
    // Retain filesystem evidence automatically if a preceding check fails.
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn split_runs_and_order_consolidate_only_at_logical_boundary() -> Result<()> {
    for seed in 1..=16_i64 {
        let store = Arc::new(InMemory::new());
        let mut raw = vec![((0, 0), i64::MAX), ((0, 0), 1), ((0, 0), -1)];
        raw.extend((0..48).map(|n| (((n % 7) + 1, n % 3), (seed + n) % 5 - 2)));
        let mut oracle = std::collections::BTreeMap::<(i64, i64), num_bigint::BigInt>::new();
        for (tuple, weight) in &raw {
            *oracle.entry(*tuple).or_default() += *weight;
        }
        oracle.retain(|_, weight| *weight != num_bigint::BigInt::default());
        let expected = oracle
            .into_iter()
            .map(|(tuple, weight)| Ok((tuple, i64::try_from(weight)?)))
            .collect::<Result<Vec<_>>>()?;
        let mut runs = Vec::new();
        // Individual boundary coefficients are separate runs; no premature merge.
        for (ordinal, entry) in raw.into_iter().enumerate() {
            let batch = Batch::from_updates([entry])?;
            if ordinal % 3 == 0 {
                let reference = ObjectBatch::write(store.clone(), &batch, "split", 1).await?;
                runs.push(Run::Object(ObjectBatch::open(store.clone(), reference, "split").await?));
            } else {
                runs.push(Run::Memory(Arc::new(batch)));
            }
        }
        runs.rotate_left(usize::try_from(seed)?);
        if seed % 2 == 0 {
            runs.reverse();
        }
        let mut trace = Trace::default();
        trace.commit(trace.prepare_runs(runs, 1).await?)?;
        let actual = trace.snapshot().materialize().await?;
        assert_eq!(
            actual.iter().map(|(tuple, weight)| (*tuple, *weight)).collect::<Vec<_>>(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn bounded_objects_round_trip_large_batches_and_reject_oversized_rows() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates((0..100_000).map(|n| ((n, format!("value-{n}")), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "large-v2", 512).await?;
    let object = ObjectBatch::<i32, String>::open(store.clone(), reference, "large-v2").await?;
    let mut cursor = object.cursor().await?;
    let mut count = 0;
    while let Some((key, value, weight)) = cursor.current() {
        assert_eq!(*key, count);
        assert_eq!(value, &format!("value-{count}"));
        assert_eq!(weight, 1);
        count += 1;
        cursor.advance().await?;
    }
    assert_eq!(count, 100_000);
    let oversized = Batch::from_updates([((0, "x".repeat(8 * 1024 * 1024)), 1)])?;
    assert!(ObjectBatch::write(store, &oversized, "large-v2", 1).await.is_err());
    Ok(())
}

#[tokio::test]
async fn legacy_packed_object_remains_readable() -> Result<()> {
    use sha2::{Digest, Sha256};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = serde_json::to_vec(&vec![((1_i64, 2_i64), 3_i64)])?;
    let digest = format!("{:x}", Sha256::digest(&data));
    let index = serde_json::to_vec(&serde_json::json!({
        "version": 1, "schema": "legacy", "blocks": [{
            "first": [1, 2], "last": [1, 2], "offset": 0,
            "length": data.len(), "rows": 1, "hash": digest
        }]
    }))?;
    let mut bytes = data.clone();
    bytes.extend(&index);
    let path = format!("pgderive/batch-v1/{:x}", Sha256::digest(&bytes));
    store.put(&Path::from(path.clone()), bytes.clone().into()).await?;
    let reference = serde_json::from_value(serde_json::json!({
        "path": path, "bytes": bytes.len(), "index_offset": data.len(),
        "index_length": index.len(), "index_hash": format!("{:x}", Sha256::digest(&index)),
        "schema": "legacy"
    }))?;
    let object = ObjectBatch::<i64, i64>::open(store, reference, "legacy").await?;
    let mut cursor = object.cursor().await?;
    assert_eq!(cursor.current(), Some((&1, &2, 3)));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    Ok(())
}

#[tokio::test]
async fn malformed_manifest_limits_fail_before_reading_rows() -> Result<()> {
    use sha2::{Digest, Sha256};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1_i64, 1_i64), 1)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "limits", 1).await?;
    let path = Path::from(reference.path());
    let bytes = store.get(&path).await?.bytes().await?;
    let mut index: serde_json::Value = serde_json::from_slice(&bytes)?;
    index["blocks"][0]["rows"] = serde_json::json!(65_537);
    let malformed = serde_json::to_vec(&index)?;
    store.put(&path, malformed.clone().into()).await?;
    let mut descriptor = serde_json::to_value(&reference)?;
    descriptor["bytes"] = serde_json::json!(malformed.len());
    descriptor["index_length"] = serde_json::json!(malformed.len());
    descriptor["index_hash"] = serde_json::json!(format!("{:x}", Sha256::digest(&malformed)));
    let invalid = serde_json::from_value(descriptor.clone())?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), invalid, "limits").await.is_err());
    descriptor["index_length"] = serde_json::json!(8 * 1024 * 1024 + 1);
    let invalid = serde_json::from_value(descriptor)?;
    assert!(ObjectBatch::<i64, i64>::open(store, invalid, "limits").await.is_err());
    Ok(())
}

#[tokio::test]
async fn full_tuple_probes_match_merged_bags_and_reject_invalid_retractions() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates((0..64).map(|value| ((1, value * 2), 3)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "point-i64-v1", 4).await?;
    let object = ObjectBatch::open(store.clone(), reference.clone(), "point-i64-v1").await?;
    let mut trace = Trace::default();
    trace.commit(trace.prepare(Run::Object(object), 1).await?)?;
    trace.commit(trace.prepare(memory([((1, 64), -3), ((1, 66), 4), ((2, 0), -1)])?, 2).await?)?;
    let snapshot = trace.snapshot();
    for key in 0..4 {
        for value in -1..130 {
            let expected = if key == 1 && value % 2 == 0 && (0..128).contains(&value) {
                match value {
                    64 => 0,
                    66 => 7,
                    _ => 3,
                }
            } else if key == 2 && value == 0 {
                -1
            } else {
                0
            };
            assert_eq!(snapshot.tuple_weight(&(key, value)).await?, expected);
        }
    }
    assert!(
        snapshot
            .validate_bag_delta(&Batch::from_updates([((1, 65), -1), ((1, 67), 1)])?)
            .await
            .is_err()
    );
    assert!(snapshot.validate_bag_delta(&Batch::from_updates([((1, 66), -7)])?).await.is_ok());
    let unbounded = super::TraceSnapshot {
        runs: vec![
            memory([((1, 1), i64::MAX)])?,
            memory([((1, 1), i64::MAX)])?,
            memory([((1, 1), -i64::MAX)])?,
        ],
        generation: 0,
        time: 0,
    };
    assert_eq!(unbounded.tuple_weight(&(1, 1)).await?, i64::MAX);
    store.put(&Path::from(reference.path()), b"corrupt".to_vec().into()).await?;
    // Reopened index corruption is rejected before any point query is exposed.
    assert!(ObjectBatch::<i64, i64>::open(store, reference, "point-i64-v1").await.is_err());
    Ok(())
}
