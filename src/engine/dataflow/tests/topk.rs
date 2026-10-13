use crate::engine::{
    Batch,
    dataflow::{Arrangement, OrderedTopK, TimedBatch},
    execution::Limits,
};
use anyhow::Result;
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn top_k_counts_occurrences_clips_cutoffs_and_reopens() -> Result<()> {
    for limit in [0, 1, 3, 8] {
        let store = Arc::new(InMemory::new());
        let inputs = Arrangement::<i64, i64>::new(store.clone(), "topk-input-v1".into(), 1)?;
        let outputs = Arrangement::<i64, i64>::new(store, "topk-output-v1".into(), 1)?;
        let operator = OrderedTopK::new(limit, |key: &i64, value: &i64, _| Ok((*key, *value)));
        let mut input = inputs.empty();
        let mut output = outputs.empty();
        let mut memory = BTreeMap::<(i64, i64), i64>::new();
        let history = [
            vec![((0, 1), 2), ((0, 2), 3), ((0, 3), 1), ((1, -1), 4)],
            vec![((0, 0), 1), ((0, 1), -1), ((1, -1), -2)],
            vec![((0, 0), -1), ((0, 1), -1), ((0, 2), -2), ((1, -2), 3)],
            vec![((0, 2), -1), ((0, 3), -1), ((1, -1), -2)],
            vec![],
            vec![((0, -1), 5), ((1, -2), -3)],
        ];
        for (index, updates) in history.into_iter().enumerate() {
            for (tuple, weight) in &updates {
                *memory.entry(*tuple).or_default() += weight;
            }
            let delta = TimedBatch {
                time: u64::try_from(index)? + 1,
                batch: Batch::from_updates(updates)?,
            };
            let change = operator.evaluate(&delta, &input, &output, Limits::default()).await?;
            input = inputs.reopen(inputs.stage(&input, &delta).await?.manifest()?).await?;
            output = outputs.reopen(outputs.stage(&output, &change).await?.manifest()?).await?;
            let mut remaining = BTreeMap::new();
            let mut expected = Vec::new();
            for ((key, value), weight) in &memory {
                let budget = remaining.entry(*key).or_insert(limit);
                let selected = usize::try_from(*weight)?.min(*budget);
                *budget -= selected;
                expected.push(((*key, *value), i64::try_from(selected)?));
            }
            assert_eq!(output.materialize().await?, Batch::from_updates(expected)?);
        }
    }
    Ok(())
}

#[tokio::test]
async fn top_k_validates_complete_delta_even_outside_selected_prefix() -> Result<()> {
    let writer =
        Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "topk-errors-v1".into(), 1)?;
    let empty = writer.empty();
    let first = TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 3), ((0, 99), 2)])? };
    let prior = writer.stage(&empty, &first).await?;
    let retained = writer
        .stage(&empty, &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1)])? })
        .await?;
    for limit in [0, 1] {
        let operator = OrderedTopK::new(limit, |key: &i64, value: &i64, _| Ok((*key, *value)));
        let negative = TimedBatch { time: 2, batch: Batch::from_updates([((0, 99), -3)])? };
        assert!(operator.evaluate(&negative, &prior, &retained, Limits::default()).await.is_err());
    }
    let operator = OrderedTopK::new(1, |key: &i64, value: &i64, _| Ok((*key, *value)));
    let next = TimedBatch { time: 2, batch: Batch::from_updates([((0, 1), -3)])? };
    let limits = Limits { contributions: 1, ..Limits::default() };
    assert!(operator.evaluate(&next, &prior, &retained, limits).await.is_err());
    let wrong = OrderedTopK::new(1, |_: &i64, value: &i64, _| Ok((1, *value)));
    assert!(wrong.evaluate(&first, &empty, &empty, Limits::default()).await.is_err());
    assert!(operator.evaluate(&first, &prior, &retained, Limits::default()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn top_k_reads_selected_prefix_instead_of_full_partition() -> Result<()> {
    use crate::engine::reader::BlockCache;
    let store = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let inputs = Arrangement::<i64, i64>::new(store.clone(), "topk-read-input-v1".into(), 1)?
        .with_cache(cache.clone());
    let outputs = Arrangement::<i64, i64>::new(store, "topk-read-output-v1".into(), 1)?
        .with_cache(cache.clone());
    let prior = inputs
        .stage(
            &inputs.empty(),
            &TimedBatch {
                time: 1,
                batch: Batch::from_updates((1..=128).map(|value| ((0, value), 1)))?,
            },
        )
        .await?;
    let retained = outputs
        .stage(
            &outputs.empty(),
            &TimedBatch {
                time: 1,
                batch: Batch::from_updates((1..=3).map(|value| ((0, value), 1)))?,
            },
        )
        .await?;
    let before = cache.stats()?.misses;
    let materialized = prior.materialize().await?;
    assert_eq!(materialized.iter().count(), 128);
    let baseline = cache.stats()?.misses - before;
    let before = cache.stats()?.misses;
    let operator = OrderedTopK::new(3, |key: &i64, value: &i64, _| Ok((*key, *value)));
    let changed = operator
        .evaluate(
            &TimedBatch { time: 2, batch: Batch::from_updates([((0, 128), -1), ((0, 129), 1)])? },
            &prior,
            &retained,
            Limits::default(),
        )
        .await?;
    let reads = cache.stats()?.misses - before;
    assert!(changed.batch.iter().next().is_none());
    assert_eq!(baseline, 128);
    assert!(reads <= 12, "top-k read {reads} blocks instead of a bounded prefix");
    assert_eq!(cache.stats()?.bytes, 0);
    Ok(())
}
