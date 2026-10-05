use super::{TimedBatch, TraceQuery};
use crate::engine::{Batch, GroupedCount, IncrementalJoin, ZSet};
use anyhow::{Result, bail};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

type Query = TraceQuery<i64, i64, i64, i64>;
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
    let mut query = Query::new(Arc::new(InMemory::new()), "integers-v1".into(), 2, |k, l, r| {
        Ok(group(*k, *l, *r))
    })?;
    let mut join = IncrementalJoin::default();
    let mut count = GroupedCount::default();
    let mut output = ZSet::default();
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
        let step = join.step(&memory_left, &memory_right)?;
        let selected = step
            .delta
            .try_filter(|(key, l, r)| Ok(group(*key, *l, *r).is_some()))?
            .try_map(|(key, l, r)| Ok((key % 2, (*l, *r))))?;
        output.apply(&count.step(&selected)?)?;
        let pinned = query.snapshot();
        let prepared = query.prepare(&left, &right).await?;
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
        assert_eq!(expected, Batch::from_updates(output.iter().map(|(row, w)| (*row, *w)))?);
    }
    Ok(())
}

#[tokio::test]
async fn graph_preparations_are_atomic_stale_foreign_and_retryable() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut query =
        Query::new(store.clone(), "integers-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
    assert!(Query::new(store.clone(), String::new(), 1, |k, l, r| Ok(group(*k, *l, *r))).is_err());
    assert!(Query::new(store.clone(), "v1".into(), 0, |k, l, r| Ok(group(*k, *l, *r))).is_err());
    let left = stream(1, [((1, 1), 1)])?;
    let right = stream(1, [((1, 2), 1)])?;
    assert!(query.prepare(&stream(0, [])?, &right).await.is_err());
    let failing =
        Query::new(store.clone(), "failing-v1".into(), 1, |_, _, _| bail!("callback failed"))?;
    assert!(failing.prepare(&left, &right).await.is_err());
    assert_eq!(failing.snapshot().counts.time(), 0);
    let first = query.prepare(&left, &right).await?;
    let stale = query.prepare(&left, &right).await?;
    let mut foreign =
        Query::new(store.clone(), "integers-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
    assert!(foreign.commit(stale).is_err());
    let stale = query.prepare(&left, &right).await?;
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
    let path = Path::from(object.block_path(0).ok_or_else(|| anyhow::anyhow!("missing block"))?);
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    let empty = stream(2, [])?;
    assert!(query.prepare(&empty, &empty).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    store.put(&path, bytes.into()).await?;
    query.commit(query.prepare(&empty, &empty).await?)?;
    assert_eq!(query.snapshot().counts.time(), 2);
    assert_eq!(pinned.left.time(), 1);
    Ok(())
}

#[tokio::test]
async fn join_cross_terms_and_count_prior_state_cancel_exactly() -> Result<()> {
    let mut query =
        Query::new(Arc::new(InMemory::new()), "integer-boundaries-v1".into(), 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let left = stream(1, [((0, 0), i64::MAX)])?;
    let right = stream(1, [])?;
    query.commit(query.prepare(&left, &right).await?)?;
    let left = stream(2, [((0, 0), 1 - i64::MAX)])?;
    let right = stream(2, [((0, 0), 2)])?;
    query.commit(query.prepare(&left, &right).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([((0, 2), 1)])?);
    let pinned = query.snapshot();
    let left = stream(3, [((0, 0), i64::MAX)])?;
    let right = stream(3, [])?;
    assert!(query.prepare(&left, &right).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    let left = stream(3, [((0, 0), -1)])?;
    query.commit(query.prepare(&left, &right).await?)?;
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
        Query::new(store.clone(), "upload-failure-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
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
    assert!(query.prepare(&left, &right).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    assert_eq!(pinned.right.time(), 0);
    assert_eq!(pinned.counts.time(), 0);
    store.put(&path, bytes.into()).await?;
    query.commit(query.prepare(&left, &right).await?)?;
    assert_eq!(query.snapshot().left.time(), 1);
    assert_eq!(pinned.left.materialize().await?, Batch::from_updates([])?);
    Ok(())
}

#[tokio::test]
async fn count_finalization_includes_prior_state_and_input_failure_is_atomic() -> Result<()> {
    let mut query =
        Query::new(Arc::new(InMemory::new()), "count-boundary-v1".into(), 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let left = stream(1, [((0, 0), i64::MIN)])?;
    let right = stream(1, [((0, 0), 1)])?;
    query.commit(query.prepare(&left, &right).await?)?;
    let left = stream(2, [((0, 0), i64::MAX), ((0, 1), 1)])?;
    let right = stream(2, [])?;
    query.commit(query.prepare(&left, &right).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([])?);
    let mut query =
        Query::new(Arc::new(InMemory::new()), "input-overflow-v1".into(), 1, |k, l, r| {
            Ok(group(*k, *l, *r))
        })?;
    let left = stream(1, [((0, 0), i64::MAX)])?;
    let right = stream(1, [])?;
    query.commit(query.prepare(&left, &right).await?)?;
    let pinned = query.snapshot();
    let left = stream(2, [((0, 0), 1)])?;
    let right = stream(2, [])?;
    assert!(query.prepare(&left, &right).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    let left = stream(2, [((0, 0), -1)])?;
    query.commit(query.prepare(&left, &right).await?)?;
    Ok(())
}

#[tokio::test]
async fn physical_replacement_retains_old_readers_and_invalidates_staged_tick() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut query =
        Query::new(store.clone(), "compaction-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
    let left = stream(1, [((0, 1), 1), ((0, 2), 1)])?;
    let right = stream(1, [((0, 3), 1)])?;
    query.commit(query.prepare(&left, &right).await?)?;
    let pinned = query.snapshot();
    let empty = stream(2, [])?;
    let staged = query.prepare(&empty, &empty).await?;
    let compacted = query.prepare_compaction().await?;
    let stale = query.prepare_compaction().await?;
    let foreign = query.prepare_compaction().await?;
    let mut other = Query::new(store, "compaction-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
    assert!(other.commit_compaction(foreign).is_err());
    query.commit_compaction(compacted)?;
    assert!(query.commit(staged).is_err());
    assert!(query.commit_compaction(stale).is_err());
    assert_eq!(query.snapshot().counts.time(), 1);
    assert_eq!(query.snapshot().counts.generation(), 2);
    assert_eq!(query.snapshot().counts.materialize().await?, pinned.counts.materialize().await?);
    query.commit(query.prepare(&empty, &empty).await?)?;
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
        Query::new(store, "filesystem-failure-v1".into(), 1, |k, l, r| Ok(group(*k, *l, *r)))?;
    let pinned = query.snapshot();
    let left = stream(1, [((0, 0), 1)])?;
    let right = stream(1, [((0, 1), 1)])?;
    let failure = query.prepare(&left, &right).await;
    assert!(failure.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    std::fs::remove_file(fixture.join("pgderive"))?;
    query.commit(query.prepare(&left, &right).await?)?;
    assert_eq!(query.snapshot().counts.materialize().await?, Batch::from_updates([((0, 1), 1)])?);
    // Remove only our own successful fixture; failed tests retain filesystem evidence.
    std::fs::remove_dir_all(fixture)?;
    Ok(())
}
