use super::Statistics;
use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    execution::Limits,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Value = (i64, Option<i64>);
type Aggregate = Statistics<Option<i64>, Value, Vec<i64>>;
fn aggregate() -> Aggregate {
    Statistics::with_measure(
        3,
        |_, row: &Value| {
            Ok(vec![
                BigInt::from(1),
                BigInt::from(i64::from(row.1.is_some())),
                BigInt::from(row.1.unwrap_or_default()),
            ])
        },
        |state: &Vec<i64>| Ok(state.iter().copied().map(BigInt::from).collect()),
        |_, values| {
            ensure!(values[0] >= BigInt::default(), "negative group count");
            let values =
                values.iter().cloned().map(i64::try_from).collect::<Result<Vec<_>, _>>()?;
            Ok((values[0] != 0).then_some(values))
        },
    )
}
#[tokio::test]
async fn ordered_and_spilled_statistics_match_independent_group_state() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "ordered-stats".into(), 4)?;
    let mut state = writer.empty();
    let mut source = BTreeMap::new();
    let operator = aggregate();
    for time in 1_i64..=32 {
        let mut changes = Vec::new();
        let id = time % 11;
        if let Some(old) = source.remove(&id) {
            changes.push((old, -1));
        }
        if time % 5 != 0 {
            let row = (
                (time % 4 != 0).then_some(time % 3),
                (id, (time % 3 != 0).then_some(time % 17 - 8)),
            );
            source.insert(id, row);
            changes.push((row, 1));
        }
        let input = TimedBatch { time: u64::try_from(time)?, batch: Batch::from_updates(changes)? };
        let ordered = operator.evaluate(&input, &state, Limits::default()).await?;
        let spilled = operator
            .evaluate(&input, &state, Limits { resident_entries: 1, ..Limits::default() })
            .await?;
        assert_eq!(ordered.batch, spilled.batch);
        state = writer.stage(&state, &ordered).await?;
        let mut oracle = BTreeMap::<Option<i64>, Vec<i64>>::new();
        for (key, (_, value)) in source.values() {
            let total = oracle.entry(*key).or_insert_with(|| vec![0; 3]);
            total[0] += 1;
            total[1] += i64::from(value.is_some());
            total[2] += value.unwrap_or_default();
        }
        assert_eq!(
            state.materialize().await?,
            Batch::from_updates(oracle.into_iter().map(|row| (row, 1)))?
        );
    }
    Ok(())
}
#[tokio::test]
async fn ordered_statistics_keep_contribution_and_intermediate_output_limits() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "ordered-limits".into(), 4)?;
    let input = TimedBatch { time: 1, batch: Batch::from_updates([((Some(0), (1, Some(2))), 1)])? };
    let operator = aggregate();
    for limits in [
        Limits { contributions: 1, ..Limits::default() },
        Limits { output_entries: 2, ..Limits::default() },
    ] {
        assert!(operator.evaluate(&input, &writer.empty(), limits).await.is_err());
    }
    let output = operator
        .evaluate(&input, &writer.empty(), Limits { scratch_bytes: 1, ..Limits::default() })
        .await?;
    assert_eq!(output.batch, Batch::from_updates([((Some(0), vec![1, 1, 2]), 1)])?);
    Ok(())
}
