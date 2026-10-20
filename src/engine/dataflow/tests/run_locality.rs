//! Measure independent run-count, hot-key and unrelated-key axes with cold blocks.
use crate::engine::{
    Batch,
    dataflow::{Arrangement, Group, Join, TimedBatch},
    execution::Limits,
    reader::BlockCache,
    trace::TraceSnapshot,
};
use anyhow::Result;
use object_store::memory::InMemory;
use std::sync::Arc;
fn first() -> Group<i64, i64> {
    Group::new(|input, work| {
        Box::pin(async move {
            let mut output = Vec::new();
            for (mut cursor, sign) in [(input.before().await?, -1), (input.after().await?, 1)] {
                if let Some((value, _)) = cursor.next(work).await? {
                    output.push((((), value), sign));
                }
            }
            Batch::from_updates(output)
        })
    })
}
#[tokio::test]
async fn cold_reads_grow_with_matching_runs_and_compaction_removes_amplification() -> Result<()> {
    let mut evidence = Vec::new();
    for runs in [1_u64, 4, 16, 64] {
        for hot in [1_i64, 8] {
            for unrelated in [0_i64, 64] {
                evidence.push(measure(runs, hot, unrelated).await?);
            }
        }
    }
    eprintln!(
        "run-locality (runs,hot,unrelated,group-before,group-after,join-before,join-after): {evidence:?}"
    );
    Ok(())
}
async fn measure(
    runs: u64,
    hot: i64,
    unrelated: i64,
) -> Result<(u64, i64, i64, u64, u64, u64, u64)> {
    let cache = Arc::new(BlockCache::new(0, 0));
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "run-locality".into(), 1)?
        .with_cache(cache.clone());
    let (mut prior, total) = populate(&writer, runs, hot, unrelated).await?;
    // Reopen validates the exact same memberships without relying on resident rows.
    prior = writer.reopen(prior.manifest()?).await?;
    let compacted = writer.compact(&prior).await?;
    assert_eq!(prior.materialize().await?, compacted.materialize().await?);
    assert_eq!(compacted.time(), prior.time());
    assert!(compacted.run_count() <= prior.run_count());
    let mut group_reads = Vec::new();
    let mut join_reads = Vec::new();
    for snapshot in [&prior, &compacted] {
        let delta = TimedBatch { time: runs + 1, batch: Batch::from_updates([((7, hot - 1), 1)])? };
        let before = cache.stats()?.misses;
        let result = first()
            .evaluate(&delta, snapshot, Limits { contributions: 4, ..Limits::default() })
            .await?;
        assert_eq!(result.batch, Batch::from_updates([])?);
        group_reads.push(cache.stats()?.misses - before);
        let unchanged = TimedBatch { time: runs + 1, batch: Batch::from_updates([])? };
        let delta = TimedBatch { time: runs + 1, batch: Batch::from_updates([((7, -1), 1)])? };
        let before = cache.stats()?.misses;
        let joined = Join.evaluate(&delta, &unchanged, snapshot, snapshot).await?;
        assert_eq!(
            joined.batch,
            Batch::from_updates((0..hot).map(|value| ((7, (-1, value)), total)))?
        );
        join_reads.push(cache.stats()?.misses - before);
    }
    bounds(runs, hot, &group_reads, &join_reads)?;
    Ok((runs, hot, unrelated, group_reads[0], group_reads[1], join_reads[0], join_reads[1]))
}

async fn populate(
    writer: &Arrangement<i64, i64>,
    runs: u64,
    hot: i64,
    unrelated: i64,
) -> Result<(TraceSnapshot<i64, i64>, i64)> {
    let mut prior = writer.empty();
    let mut total = 0;
    for tick in 1..=runs {
        let weight = if tick % 2 == 1 { 2 } else { -1 };
        total += weight;
        let marker = i64::try_from(tick)?;
        let rows = (0..hot)
            .map(|value| ((7, value), weight))
            .chain((0..unrelated).map(|key| ((100 + key, marker), 1)))
            .chain([((100_000 + i64::try_from(tick)?, 0), 1)]);
        prior = writer
            .stage(&prior, &TimedBatch { time: tick, batch: Batch::from_updates(rows)? })
            .await?;
    }
    Ok((prior, total))
}
fn bounds(runs: u64, hot: i64, group_reads: &[u64], join_reads: &[u64]) -> Result<()> {
    assert!(group_reads[0] <= 6 * runs, "{runs}/{hot}: {group_reads:?}");
    assert!(group_reads[1] <= 6, "{runs}/{hot}: {group_reads:?}");
    assert!(join_reads[0] <= runs * (u64::try_from(hot)? + 1), "{join_reads:?}");
    assert!(join_reads[1] <= u64::try_from(hot)? + 1, "{join_reads:?}");
    if runs > 1 {
        assert!(group_reads[1] < group_reads[0], "{group_reads:?}");
        assert!(join_reads[1] < join_reads[0], "{join_reads:?}");
    }
    Ok(())
}
