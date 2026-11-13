use super::MergedCursor;
use crate::engine::{
    Batch,
    reader::{BatchCursor as _, BatchReader},
};
use anyhow::Result;
use std::{sync::Arc, time::Duration};

fn buffers(cursor: &MergedCursor<i64, i64>) -> [usize; 2] {
    [cursor.position.cursors.as_ptr().addr(), cursor.workspace.cursors.as_ptr().addr()]
}

fn cleared(cursor: &MergedCursor<i64, i64>) {
    assert!(cursor.workspace.cursors.is_empty());
    assert!(cursor.workspace.row.is_none());
}

#[tokio::test]
async fn staging_reuses_both_buffers_and_clones_own_no_workspace() -> Result<()> {
    let batch = Batch::from_updates((0..8).map(|key| ((key, 0_i64), 1)))?;
    let rows = batch.shared_entries();
    let mut cursor = MergedCursor::new(vec![batch.cursor().await?]);
    cursor.find().await?;
    let pins = Arc::strong_count(&rows);
    cursor.advance().await?;
    cleared(&cursor);
    let initial = buffers(&cursor);
    assert_eq!(Arc::strong_count(&rows), pins);
    let mut cloned = cursor.clone();
    assert_eq!(cloned.workspace.cursors.capacity(), 0);
    cloned.advance().await?;
    assert_eq!(cursor.current(), Some((&1, &0, 1)));
    assert_eq!(cloned.current(), Some((&2, &0, 1)));
    assert!(buffers(&cloned).iter().all(|pointer| !initial.contains(pointer)));
    drop(cloned);
    for key in 2..8 {
        cursor.advance().await?;
        cleared(&cursor);
        assert_eq!(cursor.current(), Some((&key, &0, 1)));
        assert!(buffers(&cursor).iter().all(|pointer| initial.contains(pointer)));
        assert_eq!(Arc::strong_count(&rows), pins);
    }
    cursor.seek_key(&0).await?;
    cleared(&cursor);
    assert!(buffers(&cursor).iter().all(|pointer| initial.contains(pointer)));
    cursor.advance_span().await?;
    assert!(cursor.current().is_none());
    cleared(&cursor);
    assert!(buffers(&cursor).iter().all(|pointer| initial.contains(pointer)));
    assert_eq!(Arc::strong_count(&rows), pins);
    Ok(())
}

#[tokio::test]
async fn failed_staging_releases_pins_and_reuses_capacity() -> Result<()> {
    let left = Batch::from_updates([((0_i64, 0_i64), 1), ((1, 0), i64::MAX)])?;
    let right = Batch::from_updates([((1_i64, 0_i64), 1)])?;
    let rows = [left.shared_entries(), right.shared_entries()];
    let mut cursor = MergedCursor::new(vec![left.cursor().await?, right.cursor().await?]);
    cursor.find().await?;
    let pins = rows.each_ref().map(Arc::strong_count);
    assert!(cursor.advance().await.is_err());
    let workspace = cursor.workspace.cursors.as_ptr();
    let capacity = cursor.workspace.cursors.capacity();
    assert!(capacity >= 2);
    cleared(&cursor);
    assert_eq!(rows.each_ref().map(Arc::strong_count), pins);
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    assert!(cursor.seek_key(&1).await.is_err());
    assert!(cursor.advance_span().await.is_err());
    cleared(&cursor);
    assert_eq!(cursor.workspace.cursors.as_ptr(), workspace);
    assert_eq!(cursor.workspace.cursors.capacity(), capacity);
    assert_eq!(rows.each_ref().map(Arc::strong_count), pins);
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_staging_drops_partial_positions_and_keeps_workspace_for_retry() -> Result<()> {
    let left = Batch::from_updates((0..=257_i64).map(|key| ((key, 0_i64), 1)))?;
    let right = Batch::from_updates((1..=256_i64).map(|key| ((key, 0_i64), -1)))?;
    let rows = [left.shared_entries(), right.shared_entries()];
    let mut cursor = MergedCursor::new(vec![left.cursor().await?, right.cursor().await?]);
    cursor.find().await?;
    let pins = rows.each_ref().map(Arc::strong_count);
    assert!(tokio::time::timeout(Duration::ZERO, cursor.advance()).await.is_err());
    let workspace = cursor.workspace.cursors.as_ptr();
    let capacity = cursor.workspace.cursors.capacity();
    assert!(capacity >= 2);
    cleared(&cursor);
    assert_eq!(rows.each_ref().map(Arc::strong_count), pins);
    assert_eq!(cursor.current(), Some((&0, &0, 1)));
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&257, &0, 1)));
    assert_eq!(cursor.position.cursors.as_ptr(), workspace);
    assert_eq!(cursor.position.cursors.capacity(), capacity);
    cleared(&cursor);
    assert_eq!(rows.each_ref().map(Arc::strong_count), pins);
    Ok(())
}
