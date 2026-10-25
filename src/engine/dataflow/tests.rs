use super::TimedBatch;
use crate::engine::{Batch, ZSet};
use anyhow::{Result, bail};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

fn stream(
    time: u64,
    rows: impl IntoIterator<Item = ((i64, i64), i64)>,
) -> Result<TimedBatch<Batch<i64, i64>>> {
    Ok(TimedBatch { time, batch: Batch::from_updates(rows)? })
}
fn group(key: i64, left: i64, right: i64) -> Option<i64> {
    (left + right >= 0).then_some(key % 2)
}

#[tokio::test]
async fn object_graph_matches_memory_and_recomputed_bags() -> Result<()> {
    let mut query = crate::harness::object_oracle::counts(
        Arc::new(InMemory::new()),
        "integers-v1",
        2,
        |k, l, r| Ok(group(*k, *l, *r)),
    )?;
    let mut left_state = ZSet::default();
    let mut right_state = ZSet::default();
    for ordinal in 1_i64..=24 {
        let time = u64::try_from(ordinal)?;
        let left =
            stream(time, [((ordinal % 3, ordinal % 5 - 2), if time % 2 == 0 { -1 } else { 1 })])?;
        let right =
            stream(time, [((ordinal % 3, ordinal % 4 - 1), if time % 4 == 0 { -1 } else { 1 })])?;
        let memory_left =
            ZSet::from_updates(left.batch.iter().map(|(row, weight)| (*row, *weight)))?;
        let memory_right =
            ZSet::from_updates(right.batch.iter().map(|(row, weight)| (*row, *weight)))?;
        let pinned = query.snapshot();
        let prepared = query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?;
        assert_eq!(query.snapshot().left.time(), time - 1);
        query.commit(prepared)?;
        assert_eq!(pinned.left.time(), time - 1);
        left_state.apply(&memory_left)?;
        right_state.apply(&memory_right)?;
        let snapshot = query.snapshot();
        assert_eq!(
            snapshot.left.materialize().await?,
            Batch::from_updates(left_state.iter().map(|(row, w)| (*row, *w)))?
        );
        assert_eq!(
            snapshot.right.materialize().await?,
            Batch::from_updates(right_state.iter().map(|(row, w)| (*row, *w)))?
        );
        let expected = recompute(&left_state, &right_state)?;
        assert_eq!(snapshot.counts.materialize().await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn graph_preparations_are_atomic_stale_foreign_and_retryable() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut query =
        crate::harness::object_oracle::counts(store.clone(), "integers-v1", 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    assert!(
        crate::harness::object_oracle::counts(store.clone(), "", 1, |k, l, r| Ok(group(
            *k, *l, *r
        )))
        .is_err()
    );
    assert!(
        crate::harness::object_oracle::counts(store.clone(), "v1", 0, |k, l, r| Ok(group(
            *k, *l, *r
        )))
        .is_err()
    );
    let left = stream(1, [((1, 1), 1)])?;
    let right = stream(1, [((1, 2), 1)])?;
    assert!(
        query
            .prepare(TimedBatch { time: 0, batch: (stream(0, [])?.batch, right.batch.clone()) })
            .await
            .is_err()
    );
    let failing: crate::harness::object_oracle::Counts<i64, i64, i64, i64> =
        crate::harness::object_oracle::counts(store.clone(), "failing-v1", 1, |_, _, _| {
            bail!("callback failed")
        })?;
    assert!(failing.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await.is_err());
    assert_eq!(failing.snapshot().counts.time(), 0);
    let first = query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?;
    let stale = query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?;
    let mut foreign =
        crate::harness::object_oracle::counts(store.clone(), "integers-v1", 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    assert!(foreign.commit(stale).is_err());
    let stale = query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?;
    let result = query.commit(first)?;
    assert_eq!(result.time, 1);
    assert!(query.commit(stale).is_err());
    let pinned = query.snapshot();
    let reference = crate::engine::reader::ObjectBatch::write(
        store.clone(),
        &left.batch,
        "integers-v1:left",
        1,
    )
    .await?;
    let object = crate::engine::reader::ObjectBatch::<i64, i64>::open(
        store.clone(),
        reference,
        "integers-v1:left",
    )
    .await?;
    let path = Path::from(
        object
            .block_range(0)
            .await?
            .map(|(path, _)| path)
            .ok_or_else(|| anyhow::anyhow!("missing block"))?,
    );
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    let empty = stream(2, [])?;
    // The shared runtime validates accessed state instead of scanning unchanged
    // arrangements on every empty tick. Touch the missing source identity.
    let accessed = stream(2, [((1, 1), 1)])?;
    assert!(
        query.prepare(crate::harness::object_oracle::inputs(&accessed, &empty)?).await.is_err()
    );
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    store.put(&path, bytes.into()).await?;
    query
        .commit(query.prepare(crate::harness::object_oracle::inputs(&accessed, &empty)?).await?)?;
    assert_eq!(query.snapshot().counts.time(), 2);
    assert_eq!(pinned.left.time(), 1);
    let empty = stream(3, [])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&empty, &empty)?).await?)?;
    assert_eq!(query.snapshot().counts.time(), 3);
    Ok(())
}

#[tokio::test]
async fn join_cross_terms_and_count_prior_state_cancel_exactly() -> Result<()> {
    let mut query = crate::harness::object_oracle::counts(
        Arc::new(InMemory::new()),
        "integer-boundaries-v1",
        1,
        |k, l, r| Ok(group(*k, *l, *r)),
    )?;
    let left = stream(1, [((0, 0), i64::MAX)])?;
    let right = stream(1, [])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    let left = stream(2, [((0, 0), 1 - i64::MAX)])?;
    let right = stream(2, [((0, 0), 2)])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([((0, 2), 1)])?);
    let pinned = query.snapshot();
    let left = stream(3, [((0, 0), i64::MAX)])?;
    let right = stream(3, [])?;
    assert!(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    let left = stream(3, [((0, 0), -1)])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([])?);
    Ok(())
}

fn recompute(left: &ZSet<(i64, i64)>, right: &ZSet<(i64, i64)>) -> Result<Batch<i64, i64>> {
    let mut totals = std::collections::BTreeMap::new();
    for ((key, l), lw) in left.iter() {
        for ((rk, r), rw) in right.iter() {
            if key == rk
                && let Some(g) = group(*key, *l, *r)
            {
                *totals.entry(g).or_insert(0_i64) += lw * rw;
            }
        }
    }
    Batch::from_updates(totals.into_iter().filter(|(_, count)| *count != 0).map(|row| (row, 1)))
}

#[tokio::test]
async fn failed_upload_after_staging_left_does_not_publish_any_edge() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut query =
        crate::harness::object_oracle::counts(store.clone(), "upload-failure-v1", 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let left = stream(1, [((1, 1), 1)])?;
    let right = stream(1, [((1, 2), 1)])?;
    let reference = crate::engine::reader::ObjectBatch::write(
        store.clone(),
        &right.batch,
        "upload-failure-v1:right",
        1,
    )
    .await?;
    let path = Path::from(reference.path());
    let bytes = store.get(&path).await?.bytes().await?;
    store.put(&path, Vec::from("corrupt immutable object").into()).await?;
    let pinned = query.snapshot();
    assert!(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    assert_eq!(pinned.right.time(), 0);
    assert_eq!(pinned.counts.time(), 0);
    store.put(&path, bytes.into()).await?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    assert_eq!(query.snapshot().left.time(), 1);
    assert_eq!(pinned.left.materialize().await?, Batch::from_updates([])?);
    Ok(())
}

#[tokio::test]
async fn count_finalization_includes_prior_state_and_input_failure_is_atomic() -> Result<()> {
    let mut query = crate::harness::object_oracle::counts(
        Arc::new(InMemory::new()),
        "count-boundary-v1",
        1,
        |k, l, r| Ok(group(*k, *l, *r)),
    )?;
    let left = stream(1, [((0, 0), i64::MIN)])?;
    let right = stream(1, [((0, 0), 1)])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    let left = stream(2, [((0, 0), i64::MAX), ((0, 1), 1)])?;
    let right = stream(2, [])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([])?);
    let mut query = crate::harness::object_oracle::counts(
        Arc::new(InMemory::new()),
        "input-overflow-v1",
        1,
        |k, l, r| Ok(group(*k, *l, *r)),
    )?;
    let left = stream(1, [((0, 0), i64::MAX)])?;
    let right = stream(1, [])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    let pinned = query.snapshot();
    let left = stream(2, [((0, 0), 1)])?;
    let right = stream(2, [])?;
    assert!(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    let left = stream(2, [((0, 0), -1)])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    Ok(())
}

#[tokio::test]
async fn physical_replacement_retains_old_readers_and_invalidates_staged_tick() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut query =
        crate::harness::object_oracle::counts(store.clone(), "compaction-v1", 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let left = stream(1, [((0, 1), 1), ((0, 2), 1)])?;
    let right = stream(1, [((0, 3), 1)])?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    let pinned = query.snapshot();
    let empty = stream(2, [])?;
    let staged = query.prepare(crate::harness::object_oracle::inputs(&empty, &empty)?).await?;
    let compacted = query.prepare_maintenance(|state| async move { state.compact().await }).await?;
    let stale = query.prepare_maintenance(|state| async move { state.compact().await }).await?;
    let foreign = query.prepare_maintenance(|state| async move { state.compact().await }).await?;
    let mut other = crate::harness::object_oracle::counts(store, "compaction-v1", 1, |k, l, r| {
        Ok(group(*k, *l, *r))
    })?;
    assert!(other.commit_maintenance(foreign).is_err());
    query.commit_maintenance(compacted)?;
    assert!(query.commit(staged).is_err());
    assert!(query.commit_maintenance(stale).is_err());
    assert_eq!(query.snapshot().counts.time(), 1);
    assert_eq!(query.snapshot().counts.generation(), 2);
    assert_eq!(query.snapshot().counts.materialize().await?, pinned.counts.materialize().await?);
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&empty, &empty)?).await?)?;
    assert_eq!(query.snapshot().counts.time(), 2);
    assert_eq!(pinned.counts.time(), 1);
    Ok(())
}

#[tokio::test]
async fn filesystem_put_failure_retains_root_and_can_be_retried() -> Result<()> {
    use object_store::local::LocalFileSystem;
    use std::time::{SystemTime, UNIX_EPOCH};
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let fixture = std::env::temp_dir().join(format!("pgderive-put-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&fixture)?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(&fixture)?);
    // A regular file blocks creation of the object namespace. This causes the
    // actual filesystem PUT to fail, independent of reader/codec validation.
    std::fs::write(fixture.join("pgderive"), b"blocked namespace")?;
    let mut query =
        crate::harness::object_oracle::counts(store, "filesystem-failure-v1", 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let pinned = query.snapshot();
    let left = stream(1, [((0, 0), 1)])?;
    let right = stream(1, [((0, 1), 1)])?;
    let failure = query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await;
    assert!(failure.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    std::fs::remove_file(fixture.join("pgderive"))?;
    query.commit(query.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([((0, 1), 1)])?);
    // Remove only our own successful fixture; failed tests retain filesystem evidence.
    std::fs::remove_dir_all(fixture)?;
    Ok(())
}
