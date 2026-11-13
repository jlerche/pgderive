use super::MergedCursor;
use crate::engine::{
    Batch,
    reader::{BatchCursor as _, BatchReader, Cursor},
    weights::Accumulator,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

static CLONES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash, Serialize, Deserialize)]
struct Counted(i64);

impl Clone for Counted {
    fn clone(&self) -> Self {
        CLONES.fetch_add(1, Ordering::Relaxed);
        Self(self.0)
    }
}

fn previous_selection_clones(cursors: &[Cursor<i64, Counted>]) -> Result<usize> {
    CLONES.store(0, Ordering::Relaxed);
    let tuple = cursors
        .iter()
        .filter_map(|cursor| cursor.current().map(|(key, val, _)| (*key, val.clone())))
        .min();
    if let Some(tuple) = tuple {
        let mut weights = Accumulator::default();
        for cursor in cursors {
            if let Some((key, val, weight)) = cursor.current()
                && (key, val) == (&tuple.0, &tuple.1)
            {
                weights.add(tuple.clone(), weight);
            }
        }
        assert!(weights.finish()?.iter().any(|(found, weight)| found == &tuple && *weight == 1));
    }
    Ok(CLONES.load(Ordering::Relaxed))
}

#[tokio::test]
async fn minimum_selection_and_cursor_movement_borrow_pinned_identities() -> Result<()> {
    let mut cursors = Vec::new();
    for value in (0..16).rev() {
        let batch = Batch::from_updates([((0_i64, Counted(value)), 1)])?;
        cursors.push(batch.cursor().await?);
    }
    let mut merged = MergedCursor::new(cursors);
    assert_eq!(previous_selection_clones(&merged.position.cursors)?, 17);
    CLONES.store(0, Ordering::Relaxed);
    merged.find().await?;
    assert_eq!(merged.current(), Some((&0, &Counted(0), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);

    // Several matching heads still borrow the selected pinned identity. Exact
    // summation must allow cancellable intermediate overflow.
    merged.position.cursors.clear();
    for weight in [i64::MAX, 1, -i64::MAX] {
        let batch = Batch::from_updates([((0_i64, Counted(3)), weight)])?;
        merged.position.cursors.push(batch.cursor().await?);
    }
    assert_eq!(previous_selection_clones(&merged.position.cursors)?, 6);
    CLONES.store(0, Ordering::Relaxed);
    merged.find().await?;
    assert_eq!(merged.current(), Some((&0, &Counted(3), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);

    // Two canceled identities use their last matching pinned head as the
    // witness. Interleaved nonmatching heads must not advance with that group.
    let updates = [
        vec![((0_i64, Counted(0)), 1), ((1, Counted(1)), i64::MAX), ((3, Counted(3)), 1)],
        vec![((2, Counted(2)), 1)],
        vec![((0, Counted(0)), -1), ((1, Counted(1)), 1)],
        vec![((1, Counted(1)), i64::MIN)],
    ];
    let mut cursors = Vec::new();
    for rows in updates {
        cursors.push(Batch::from_updates(rows)?.cursor().await?);
    }
    let mut cancelled = MergedCursor::new(cursors);
    CLONES.store(0, Ordering::Relaxed);
    cancelled.find().await?;
    assert_eq!(cancelled.current(), Some((&2, &Counted(2), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    cancelled.advance().await?;
    assert_eq!(cancelled.current(), Some((&3, &Counted(3), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);

    let batch = Batch::from_updates((0..3).map(|key| ((key, Counted(key)), 1)))?;
    let mut merged = MergedCursor::new(vec![batch.cursor().await?]);
    merged.find().await?;
    CLONES.store(0, Ordering::Relaxed);
    let pinned = merged.clone();
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    CLONES.store(0, Ordering::Relaxed);
    merged.advance().await?;
    assert_eq!(merged.current(), Some((&1, &Counted(1), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    assert_eq!(pinned.current(), Some((&0, &Counted(0), 1)));
    CLONES.store(0, Ordering::Relaxed);
    merged.seek_key(&0).await?;
    assert_eq!(merged.current(), Some((&0, &Counted(0), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    CLONES.store(0, Ordering::Relaxed);
    merged.advance_span().await?;
    assert!(merged.current().is_none());
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    assert_eq!(pinned.current(), Some((&0, &Counted(0), 1)));
    Ok(())
}
