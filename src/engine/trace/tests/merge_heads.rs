use super::MergedCursor;
use crate::engine::{
    Batch,
    reader::{BatchReader, Cursor},
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
async fn minimum_selection_borrows_heads_and_clones_only_selected_identity() -> Result<()> {
    let mut cursors = Vec::new();
    for value in (0..16).rev() {
        let batch = Batch::from_updates([((0_i64, Counted(value)), 1)])?;
        cursors.push(batch.cursor().await?);
    }
    let mut merged = MergedCursor { cursors, row: None };
    assert_eq!(previous_selection_clones(&merged.cursors)?, 17);
    CLONES.store(0, Ordering::Relaxed);
    merged.find().await?;
    assert_eq!(merged.current(), Some((&0, &Counted(0), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 1);

    // Several matching heads still require just one owned identity. Exact
    // summation must allow cancellable intermediate overflow.
    merged.cursors.clear();
    for weight in [i64::MAX, 1, -i64::MAX] {
        let batch = Batch::from_updates([((0_i64, Counted(3)), weight)])?;
        merged.cursors.push(batch.cursor().await?);
    }
    assert_eq!(previous_selection_clones(&merged.cursors)?, 6);
    CLONES.store(0, Ordering::Relaxed);
    merged.find().await?;
    assert_eq!(merged.current(), Some((&0, &Counted(3), 1)));
    assert_eq!(CLONES.load(Ordering::Relaxed), 1);
    Ok(())
}
