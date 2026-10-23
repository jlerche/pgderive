use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    reader::{BatchReader, ObjectBatch},
};
use anyhow::{Context, Result};
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
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(object.clone()), 1)?;
    let old = trace.clone();
    let change = memory([((1, 6), -1), ((1, 8), 3)])?;
    trace = trace.append_validated(change, 2)?;
    let current = trace.materialize().await?;
    assert_eq!(old.materialize().await?, batch);
    let mut cursor = trace.cursor().await?;
    cursor.seek_key(&1).await?;
    assert_eq!(cursor.current(), Some((&1, &7, 1)));
    cursor.seek_key(&99).await?;
    assert!(cursor.current().is_none());
    cursor.seek_key(&-1).await?;
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    let replacement = ObjectBatch::write(store.clone(), &current, "i64-pair-v1", 7).await?;
    trace = trace
        .replace_runs(vec![Run::Object(
            ObjectBatch::open(store.clone(), replacement, "i64-pair-v1").await?,
        )])
        .await?;
    assert_eq!(trace.time(), 2);
    assert_eq!(trace.generation(), 3);
    assert_eq!(trace.materialize().await?, current);
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
async fn invalid_and_overflow_staging_leave_snapshot_unchanged() -> Result<()> {
    use crate::engine::dataflow::{Arrangement, TimedBatch};
    let writer = Arrangement::new(Arc::new(InMemory::new()), "overflow".into(), 1)?;
    let tick = |time, weight| -> Result<_> {
        Ok(TimedBatch { time, batch: Batch::from_updates([((0, 0), weight)])? })
    };
    let prior = writer.stage(&writer.empty(), &tick(1, i64::MAX)?).await?;
    let before = prior.materialize().await?;
    assert!(writer.stage(&prior, &tick(2, 1)?).await.is_err());
    assert!(writer.stage(&prior, &tick(9, 0)?).await.is_err());
    assert!(prior.replace_runs(vec![memory([])?]).await.is_err());
    assert_eq!(prior.materialize().await?, before);
    let next = writer.stage(&prior, &tick(2, -i64::MAX)?).await?;
    assert!(next.materialize().await?.iter().next().is_none());
    assert_eq!(prior.materialize().await?, before);
    Ok(())
}

#[tokio::test]
async fn missing_and_corrupt_objects_fail_closed_without_cursor_movement() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1, 1), 1), ((2, 2), 1)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "test", 1).await?;
    let object = ObjectBatch::open(store.clone(), reference.clone(), "test").await?;
    let mut cursor = object.cursor().await?;
    let path = Path::from(
        object
            .block_range(1)
            .map(|(path, _)| path)
            .ok_or_else(|| anyhow::anyhow!("missing block"))?,
    );
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    assert!(cursor.seek_key(&2).await.is_err());
    assert_eq!(cursor.current(), Some((&1, &1, 1)));
    store.put(&path, bytes.clone().into()).await?;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&2, &2, 1)));
    crate::engine::tests::store::corrupt_block(store.as_ref(), &object, 1).await?;
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
    let mut omitted = serde_json::to_value(&reference)?;
    assert!(omitted.get("namespace").is_some());
    omitted.as_object_mut().context("reference object required")?.remove("namespace");
    assert!(serde_json::from_value::<crate::engine::reader::ObjectRef>(omitted).is_err());
    let fresh: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root)?);
    let reopened =
        ObjectBatch::<i64, i64>::open(fresh, serde_json::from_slice(&catalog)?, "disk-v1").await?;
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(reopened), 1)?;
    assert_eq!(trace.materialize().await?, batch);
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
        let trace = TraceSnapshot::from_runs(runs, 1, 1)?;
        let actual = trace.materialize().await?;
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
    descriptor["hash"] = serde_json::json!(format!("{:x}", Sha256::digest(&malformed)));
    let malformed_path = format!("pgderive/batch/{:x}", Sha256::digest(&malformed));
    store.put(&Path::from(malformed_path.clone()), malformed.into()).await?;
    descriptor["path"] = serde_json::json!(malformed_path);
    let invalid = serde_json::from_value(descriptor.clone())?;
    assert!(ObjectBatch::<i64, i64>::open(store.clone(), invalid, "limits").await.is_err());
    descriptor["bytes"] = serde_json::json!(8 * 1024 * 1024 + 1);
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
    let mut trace = TraceSnapshot::empty();
    trace = trace.append_validated(Run::Object(object), 1)?;
    trace = trace.append_validated(memory([((1, 64), -3), ((1, 66), 4), ((2, 0), -1)])?, 2)?;
    let snapshot = trace;
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
    let unbounded = super::TraceSnapshot::from_runs(
        vec![
            memory([((1, 1), i64::MAX)])?,
            memory([((1, 1), i64::MAX)])?,
            memory([((1, 1), -i64::MAX)])?,
        ],
        0,
        0,
    )?;
    assert_eq!(unbounded.tuple_weight(&(1, 1)).await?, i64::MAX);
    store.put(&Path::from(reference.path()), b"corrupt".to_vec().into()).await?;
    // Reopened index corruption is rejected before any point query is exposed.
    assert!(ObjectBatch::<i64, i64>::open(store, reference, "point-i64-v1").await.is_err());
    Ok(())
}
