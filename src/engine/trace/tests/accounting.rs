use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    reader::{BlockCache, ObjectBatch},
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize, Serializer, ser::Error as _};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

static SERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash, Deserialize)]
struct Measured(i64);
impl Serialize for Measured {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        SERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
        if self.0 == i64::MIN {
            return Err(S::Error::custom("injected accounting failure"));
        }
        self.0.serialize(serializer)
    }
}
fn expected(batch: &Batch<i64, Measured>) -> Result<usize> {
    batch.iter().try_fold(0_usize, |bytes, ((key, value), weight)| {
        let payload = serde_json::to_vec(&((key, value.0), weight))?.len();
        bytes
            .checked_add(payload + std::mem::size_of::<((i64, Measured), i64)>() + 64)
            .context("fixture accounting overflow")
    })
}
fn memory(batch: Batch<i64, Measured>) -> Run<i64, Measured> {
    Run::Memory(Arc::new(batch))
}

#[tokio::test]
async fn retained_memory_is_charged_once_and_replacements_recompute_exact_totals() -> Result<()> {
    let first = Batch::from_updates((0..100).map(|n| ((n, Measured(n)), 2)))?;
    let initial_charge = expected(&first)?;
    SERIALIZATIONS.store(0, Ordering::Relaxed);
    let one = TraceSnapshot::empty().append_validated(memory(first), 1)?;
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 100);
    for _ in 0..100 {
        assert_eq!(one.clone().memory_charge(), initial_charge);
    }
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 100);
    let mut parts = one
        .runs
        .iter()
        .map(|run| super::ranges::Parts { retained: vec![], merging: Some(run.clone()) })
        .collect::<Vec<_>>();
    super::budget::restrict(
        &one.runs,
        &[one.runs.len()],
        &mut vec![true; one.runs.len()],
        &mut parts,
    )?;
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 100);
    let delta = Batch::from_updates((0..10).map(|n| ((n, Measured(n)), -1)))?;
    let delta_charge = expected(&delta)?;
    let full =
        Batch::from_updates((0..100).map(|n| ((n, Measured(n)), if n < 10 { 1 } else { 2 })))?;
    let replacement_charge = expected(&full)?;
    let two = one.append_validated(memory(delta), 2)?;
    assert_eq!(two.memory_charge(), replacement_charge);
    assert_eq!(one.memory_charge(), initial_charge);
    assert!(two.memory_charge() < initial_charge + delta_charge);
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 210);
    assert_eq!(two.clone().memory_charge(), replacement_charge);
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 210);
    let compacted = two.replace_runs(vec![memory(full.clone())]).await?;
    assert_eq!(compacted.memory_charge(), replacement_charge);
    assert_eq!(two.memory_charge(), replacement_charge);
    assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 310);
    for time in [3, 4] {
        let bad = memory(Batch::from_updates([((999, Measured(i64::MIN)), 1)])?);
        if time == 3 {
            assert!(two.append_validated(bad, time).is_err());
        } else {
            assert!(two.replace_runs(vec![bad]).await.is_err());
        }
        assert_eq!(two.memory_charge(), replacement_charge);
        assert_eq!(two.time(), 2);
    }
    let store = Arc::new(InMemory::new());
    let root = ObjectBatch::write(store.clone(), &full, "accounting-v1", 16).await?;
    let object = ObjectBatch::open(store.clone(), root, "accounting-v1").await?;
    let immutable = two.replace_runs(vec![Run::Object(object)]).await?;
    assert_eq!(immutable.memory_charge(), 0);
    let reopened = TraceSnapshot::reopen(
        store,
        immutable.manifest()?,
        "accounting-v1",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(reopened.memory_charge(), 0);
    let next =
        reopened.append_validated(memory(Batch::from_updates([((0, Measured(0)), -1)])?), 3)?;
    assert!(next.memory_charge() > 0);
    assert_eq!(reopened.memory_charge(), 0);
    Ok(())
}
