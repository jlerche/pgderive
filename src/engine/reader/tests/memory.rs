use crate::engine::{Batch, reader::BatchReader as _};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
};

static CLONES: AtomicUsize = AtomicUsize::new(0);
static COMPARISONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Counted(i64);
impl Clone for Counted {
    fn clone(&self) -> Self {
        CLONES.fetch_add(1, AtomicOrdering::Relaxed);
        Self(self.0)
    }
}
impl Ord for Counted {
    fn cmp(&self, other: &Self) -> Ordering {
        COMPARISONS.fetch_add(1, AtomicOrdering::Relaxed);
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for Counted {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[tokio::test]
async fn opening_shared_memory_cursors_never_copies_or_scans_the_run() -> Result<()> {
    for count in [16, 1024, 16_384] {
        let batch = Batch::from_updates(
            (0..count).map(|n| ((Counted(n / 4), Counted(n)), if n % 2 == 0 { 2 } else { -1 })),
        )?;
        CLONES.store(0, AtomicOrdering::Relaxed);
        COMPARISONS.store(0, AtomicOrdering::Relaxed);
        let pinned = batch.clone();
        let mut first = batch.cursor().await?;
        let mut second = pinned.cursor().await?;
        assert_eq!(CLONES.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(COMPARISONS.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(first.current_span().as_ptr(), second.current_span().as_ptr());
        drop(batch);
        drop(pinned);
        let key = Counted(count / 8);
        first.seek_key(&key).await?;
        first.restrict_to_upper(&key);
        assert_eq!(first.current_span().len(), 4);
        assert!(COMPARISONS.load(AtomicOrdering::Relaxed) < 100);
        assert_eq!(CLONES.load(AtomicOrdering::Relaxed), 1); // Only the upper bound.
        assert_eq!(
            first.current().map(|(_, value, weight)| (value.0, weight)),
            Some((count / 2, 2))
        );
        assert_eq!(second.current().map(|(_, value, _)| value.0), Some(0));
        let mut saved = first.clone();
        first.advance_span().await?;
        assert!(first.current().is_none());
        assert_eq!(saved.current_span().len(), 4);
        saved.advance().await?;
        assert_eq!(saved.current_span().len(), 3);
        second.seek_key(&Counted(count)).await?;
        assert!(second.current().is_none());
        second.seek_key(&Counted(-1)).await?;
        assert_eq!(second.current().map(|(_, value, _)| value.0), Some(0));
    }
    Ok(())
}

#[tokio::test]
async fn memory_cursors_pin_old_full_tuple_weights_across_success_and_failure() -> Result<()> {
    let mut batch = Batch::from_updates([
        ((1, "a".to_owned()), 2),
        ((1, "b".to_owned()), -3),
        ((3, "c".to_owned()), i64::MAX),
    ])?;
    let snapshot = batch.clone();
    let mut old = batch.cursor().await?;
    batch.apply(&Batch::from_updates([
        ((1, "a".to_owned()), -2),
        ((1, "b".to_owned()), 4),
        ((2, "x".to_owned()), -7),
    ])?)?;
    assert_eq!(batch.weight(&(1, "a".into())), 0);
    assert_eq!(batch.weight(&(1, "b".into())), 1);
    assert_eq!(batch.weight(&(2, "x".into())), -7);
    assert_ne!(batch.cursor().await?.current_span().as_ptr(), old.current_span().as_ptr());
    let before = batch.clone();
    let pinned = batch.cursor().await?;
    assert!(
        batch
            .apply(&Batch::from_updates([((0, "prefix".into()), 1), ((3, "c".into()), 1)])?)
            .is_err()
    );
    assert_eq!(batch, before);
    assert_eq!(batch.cursor().await?.current_span().as_ptr(), pinned.current_span().as_ptr());
    drop(batch);
    assert_eq!(old.current(), Some((&1, &"a".into(), 2)));
    old.advance().await?;
    assert_eq!(old.current(), Some((&1, &"b".into(), -3)));
    assert_eq!(snapshot.weight(&(1, "a".into())), 2);
    let mut empty = Batch::<i64, i64>::default().cursor().await?;
    empty.seek_key(&0).await?;
    empty.advance_span().await?;
    assert!(empty.current_span().is_empty());
    Ok(())
}
