use super::{Operators, Relational};
use crate::engine::{
    Batch,
    dataflow::{Project, TimedBatch},
    plan::{Arrangement, Definition, Kind, Node, Plan, Source, query::Settings},
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Rows = Batch<i64, Vec<i64>>;
type Bag = BTreeMap<(i64, Vec<i64>), i64>;
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
    ]
    .into_iter()
    .map(|(id, kind, inputs)| Node {
        id: id.into(),
        kind,
        inputs: inputs.into_iter().map(Into::into).collect(),
        schema: "i64-vector-v1".into(),
    })
    .collect();
    Plan::new(Definition {
        revision: "three-source-vector-v1".into(),
        sources: ["a", "b", "c"]
            .into_iter()
            .map(|id| Source { id: id.into(), schema: "i64-vector-v1".into() })
            .collect(),
        nodes,
        arrangements: ["a", "b", "c", "ab", "abc"]
            .into_iter()
            .map(|id| Arrangement {
                id: id.into(),
                node: id.into(),
                schema: "i64-vector-v1".into(),
            })
            .collect(),
        outputs: vec!["abc".into()],
    })
}
fn operators() -> Operators<i64, Vec<i64>> {
    let projects = ["a", "b", "c"]
        .into_iter()
        .map(|id| {
            (
                id.into(),
                Arc::new(Project::new(|key: &i64, row: &Vec<i64>| Ok(Some((*key, row.clone()))))),
            )
        })
        .collect();
    let joins = ["ab", "abc"]
        .into_iter()
        .map(|id| {
            (
                id.into(),
                Arc::new(Project::new(|key: &i64, pair: &(Vec<i64>, Vec<i64>)| {
                    let row = pair.0.iter().chain(&pair.1).copied().collect();
                    Ok(Some((*key, row)))
                })),
            )
        })
        .collect();
    Operators {
        projects,
        joins,
        partitions: BTreeMap::new(),
        statistics: BTreeMap::new(),
        expansions: BTreeMap::new(),
    }
}
fn oracle(states: &[Bag; 3]) -> Result<Rows> {
    let mut updates = Vec::new();
    for ((ka, a), wa) in &states[0] {
        for ((kb, b), wb) in &states[1] {
            for ((kc, c), wc) in &states[2] {
                if ka == kb && kb == kc {
                    updates
                        .push(((*ka, a.iter().chain(b).chain(c).copied().collect()), wa * wb * wc));
                }
            }
        }
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn chained_join_complete_deltas_recovery_and_ownership() -> Result<()> {
    let settings = Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
    };
    let mut query = Relational::new(plan()?, operators(), &settings)?;
    let histories = [
        [vec![(1, 2)], vec![(3, 1)], vec![(5, 1)]],
        [vec![(1, -1), (2, 1)], vec![(3, -1), (4, 2)], vec![(5, -1), (6, 1)]],
        [vec![(1, -1)], vec![(4, -1)], vec![(6, -1), (7, 1)]],
        [vec![], vec![], vec![]],
    ];
    let mut states = std::array::from_fn(|_| BTreeMap::new());
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&states)?;
        let mut deltas = Vec::new();
        for (side, updates) in changes.into_iter().enumerate() {
            let delta = Batch::from_updates(
                updates.into_iter().map(|(value, weight)| ((0, vec![value]), weight)),
            )?;
            for (tuple, weight) in delta.iter() {
                *states[side].entry(tuple.clone()).or_default() += weight;
            }
            deltas.push(delta);
        }
        let after = oracle(&states)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let work = query.prepare(TimedBatch { time: query.time() + 1, batch: deltas }).await?;
        assert_eq!(work.output().batch, expected);
        query.commit(work)?;
        let output = query
            .engine
            .snapshot()
            .entries
            .get("abc")
            .context("missing output")?
            .trace
            .materialize()
            .await?;
        assert_eq!(output, after);
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = Relational::new(plan()?, operators(), &settings)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    assert_eq!(query.time(), 4);
    let checkpoint = query.checkpoint()?;
    let work =
        query.prepare(TimedBatch { time: 5, batch: vec![Batch::from_updates([])?; 3] }).await?;
    query.restore_checkpoint(checkpoint).await?;
    assert!(query.commit(work).is_err());
    let mut missing = operators();
    missing.joins.remove("ab");
    assert!(Relational::new(plan()?, missing, &settings).is_err());
    Ok(())
}
