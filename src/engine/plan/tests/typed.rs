//! The former typed-adapter history now exercises the production relational executor.
use super::{Bag, recompute};
use crate::engine::{
    Batch,
    dataflow::{Project, Statistics, TimedBatch},
    plan::{
        Arrangement, Definition, Kind, Node, Plan, Settings, Source,
        relational::{Operator, Operators, Relational},
    },
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Rows = Batch<i64, Vec<i64>>;
fn plan() -> Result<Plan> {
    let nodes = [
        ("as", Kind::Source, vec!["a"]),
        ("bs", Kind::Source, vec!["b"]),
        ("cs", Kind::Source, vec!["c"]),
        ("a", Kind::Project, vec!["as"]),
        ("b", Kind::Project, vec!["bs"]),
        ("c", Kind::Project, vec!["cs"]),
        ("ab", Kind::Join, vec!["a", "b"]),
        ("abc", Kind::Join, vec!["ab", "c"]),
        ("sum", Kind::Statistics, vec!["abc"]),
    ]
    .into_iter()
    .map(|(id, kind, inputs)| Node {
        id: id.into(),
        kind,
        inputs: inputs.into_iter().map(Into::into).collect(),
        schema: "vector-v1".into(),
    })
    .collect();
    Plan::new(Definition {
        revision: "chain-production-v1".into(),
        sources: ["a", "b", "c"]
            .into_iter()
            .map(|id| Source { id: id.into(), schema: "vector-v1".into() })
            .collect(),
        nodes,
        arrangements: ["a", "b", "c", "ab", "abc", "sum"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "vector-v1".into() })
            .collect(),
        outputs: vec!["sum".into()],
    })
}
fn operators() -> Operators<i64, Vec<i64>> {
    let mut operators: Operators<_, _> = ["a", "b", "c"]
        .into_iter()
        .map(|id| {
            (
                id.into(),
                Operator::Project(Project::new(|key: &i64, row: &Vec<i64>| {
                    Ok(Some((*key, row.clone())))
                })),
            )
        })
        .collect();
    for id in ["ab", "abc"] {
        operators.insert(
            id.into(),
            Operator::Join(Project::new(|key: &i64, pair: &(Vec<i64>, Vec<i64>)| {
                Ok(Some((*key, pair.0.iter().chain(&pair.1).copied().collect())))
            })),
        );
    }
    operators.insert(
        "sum".into(),
        Operator::Statistics(Statistics::new(
            2,
            |row: &Vec<i64>| {
                Ok(vec![BigInt::from(1), row.iter().map(|value| BigInt::from(*value)).sum()])
            },
            |row: &Vec<i64>| Ok(row.iter().map(|value| BigInt::from(*value)).collect()),
            |_: &i64, values: &[BigInt]| {
                let rows = i64::try_from(values.first().context("missing row count")?)?;
                let sum = i64::try_from(values.get(1).context("missing sum")?)?;
                Ok((rows != 0).then_some(vec![rows, sum]))
            },
        )),
    );
    operators
}
fn expected(bags: &[Bag; 3]) -> Result<Rows> {
    Batch::from_updates(
        recompute(bags)
            .into_iter()
            .filter(|(_, (rows, _))| *rows != 0)
            .map(|(key, (rows, sum))| ((key, vec![rows, sum]), 1)),
    )
}
#[tokio::test]
async fn chained_join_statistics_emit_snapshot_differences_and_restore() -> Result<()> {
    let settings = Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    };
    let mut query = Relational::new(plan()?, operators(), &settings)?;
    let mut bags = [BTreeMap::new(), BTreeMap::new(), BTreeMap::new()];
    for time in 1_i64..=12 {
        let before = expected(&bags)?;
        let (row, weight) = if time <= 6 { (time, 1) } else { (time - 6, -1) };
        let updates = [
            vec![((row % 2, row % 3), weight)],
            vec![((row % 2, row % 4), weight)],
            vec![((row % 2, row % 5), weight)],
        ];
        let mut inputs = Vec::new();
        for (bag, changes) in bags.iter_mut().zip(updates) {
            for (tuple, weight) in &changes {
                *bag.entry(*tuple).or_default() += weight;
            }
            inputs.push(Batch::from_updates(
                changes.into_iter().map(|((key, value), weight)| ((key, vec![value]), weight)),
            )?);
        }
        let after = expected(&bags)?;
        let delta = Batch::from_updates(
            before
                .iter()
                .map(|(row, w)| (row.clone(), -w))
                .chain(after.iter().map(|(row, w)| (row.clone(), *w))),
        )?;
        let prepared =
            query.prepare(TimedBatch { time: u64::try_from(time)?, batch: inputs }).await?;
        ensure!(prepared.output().batch == delta, "wrong relational aggregate delta");
        query.commit(prepared)?;
        ensure!(
            query.snapshot().trace("sum")?.materialize().await? == after,
            "wrong integrated output"
        );
        if time == 6 {
            query = Relational::reopen(plan()?, operators(), settings.clone(), query.checkpoint()?)
                .await?;
        }
    }
    let input = Batch::from_updates([((0, vec![99]), 1), ((0, vec![99]), -1)])?;
    let empty = query.prepare(TimedBatch { time: 13, batch: vec![input; 3] }).await?;
    assert!(empty.output().batch.iter().next().is_none());
    query.commit(empty)?;
    assert_eq!(query.time(), 13);
    assert!(query.snapshot().trace("sum")?.materialize().await?.iter().next().is_none());
    Ok(())
}
