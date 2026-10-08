use crate::engine::{
    Batch,
    dataflow::{Join, TimedBatch},
    reader::{BlockCache, ObjectBatch},
    trace::{Run, Trace},
};
use anyhow::Result;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::test]
async fn repeated_delta_keys_read_prior_blocks_once_without_cache() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let prior_rows = Batch::from_updates([((7_i64, 100_i64), 2), ((7, 101), 3), ((7, 102), 4)])?;
    let reference = ObjectBatch::write(store.clone(), &prior_rows, "ints", 1).await?;
    let object = ObjectBatch::open_with_cache(store, reference, "ints", cache.clone()).await?;
    let mut right = Trace::default();
    right.commit(right.prepare(Run::Object(object), 1).await?)?;
    let mut left = Trace::default();
    left.commit(left.prepare(Run::Memory(Arc::new(Batch::from_updates([])?)), 1).await?)?;
    let delta = Batch::from_updates((0..12_i64).map(|value| ((7, value), value % 3 - 1)))?;
    let before = cache.stats()?.misses;
    // Reproduce the former production scan to measure the physical baseline,
    // independently of the optimized join. KeyProbes only retain initial heads.
    let mut probes = right.snapshot().probes();
    for ((key, _), _) in delta.iter() {
        let mut cursor = probes.cursor(key).await?;
        while cursor.current().is_some() {
            cursor.advance().await?;
        }
    }
    let baseline = cache.stats()?.misses - before;
    let before = cache.stats()?.misses;
    let joined = Join
        .evaluate(
            &TimedBatch { time: 2, batch: delta.clone() },
            &TimedBatch { time: 2, batch: Batch::from_updates([])? },
            &left.snapshot(),
            &right.snapshot(),
        )
        .await?;
    let reads = cache.stats()?.misses - before;
    assert_eq!(reads, 3);
    assert_eq!(baseline, 17); // Eight delta identities: 1 head + 8 * 2 other blocks.
    assert_eq!(cache.stats()?.bytes, 0);
    let expected = Batch::from_updates(delta.iter().flat_map(|((key, value), weight)| {
        prior_rows.iter().map(move |((_, other), other_weight)| {
            ((*key, (*value, *other)), weight * other_weight)
        })
    }))?;
    assert_eq!(joined.batch, expected);
    assert_eq!(joined.time, 2);
    Ok(())
}
