use super::{Operator, Operators, Relational};
use crate::engine::{
    Batch,
    dataflow::{Project, TimedBatch},
    plan::{Arrangement, Definition, Kind, Node, Plan, Settings, Source},
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Rows = Batch<i64, Vec<i64>>;
type Bag = BTreeMap<(i64, Vec<i64>), i64>;
type Projection = Project<i64, Vec<i64>, i64, Vec<i64>>;
type Join = super::JoinProject<i64, Vec<i64>>;
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
            (id.into(), Project::new(|key: &i64, row: &Vec<i64>| Ok(Some((*key, row.clone())))))
        })
        .collect();
    let joins = ["ab", "abc"]
        .into_iter()
        .map(|id| {
            (
                id.into(),
                Project::new(|key: &i64, pair: &(Vec<i64>, Vec<i64>)| {
                    let row = pair.0.iter().chain(&pair.1).copied().collect();
                    Ok(Some((*key, row)))
                }),
            )
        })
        .collect();
    let projects: BTreeMap<_, Projection> = projects;
    let joins: BTreeMap<_, Join> = joins;
    projects
        .into_iter()
        .map(|(id, value)| (id, Operator::Project(value)))
        .chain(joins.into_iter().map(|(id, value)| (id, Operator::Join(value))))
        .collect()
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
        encoded_cache: None,
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
    missing.remove("ab");
    assert!(Relational::new(plan()?, missing, &settings).is_err());
    Ok(())
}

#[tokio::test]
async fn wrong_source_arity_rejects_before_any_staged_objects() -> Result<()> {
    use futures_util::TryStreamExt;
    use object_store::ObjectStore;
    let store = Arc::new(InMemory::new());
    let settings = Settings {
        store: store.clone(),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    };
    let query = Relational::new(plan()?, operators(), &settings)?;
    let checkpoint = query.checkpoint()?;
    for count in [2, 4] {
        let batch = (0..count)
            .map(|_| Batch::from_updates([((0, vec![1]), 1)]))
            .collect::<Result<Vec<_>>>()?;
        assert!(
            query
                .execution
                .evaluate(
                    query.snapshot(),
                    TimedBatch { time: 1, batch },
                    super::circuit::Staging::Objects(None)
                )
                .await
                .is_err()
        );
        assert_eq!(query.checkpoint()?, checkpoint);
        assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn fanout_self_join_keeps_prior_isolation_and_retries_downstream_failure() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let definition = Definition {
        revision: "fanout-v1".into(),
        sources: vec![Source { id: "rows".into(), schema: "vector-v1".into() }],
        nodes: [
            ("source", Kind::Source, vec!["rows"]),
            ("input", Kind::Project, vec!["source"]),
            ("join", Kind::Join, vec!["input", "input"]),
            ("output", Kind::Project, vec!["join"]),
        ]
        .into_iter()
        .map(|(id, kind, inputs)| Node {
            id: id.into(),
            kind,
            inputs: inputs.into_iter().map(Into::into).collect(),
            schema: "vector-v1".into(),
        })
        .collect(),
        arrangements: ["input", "output"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "vector-v1".into() })
            .collect(),
        outputs: vec!["output".into()],
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let operators = BTreeMap::from([
        (
            "input".into(),
            Operator::Project(Project::new(move |key: &i64, row: &Vec<i64>| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(Some((*key, row.clone())))
            })),
        ),
        (
            "join".into(),
            Operator::Join(Project::new(|key: &i64, pair: &(Vec<i64>, Vec<i64>)| {
                Ok(Some((*key, vec![pair.0[0] * pair.1[0]])))
            })),
        ),
        (
            "output".into(),
            Operator::Project(Project::new(|key: &i64, row: &Vec<i64>| {
                anyhow::ensure!(row[0] != 25, "downstream failure");
                Ok(Some((*key, row.clone())))
            })),
        ),
    ]);
    let settings = Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    };
    let mut query = Relational::new(Plan::new(definition)?, operators, &settings)?;
    let tick = |time, value| -> Result<_> {
        Ok(TimedBatch { time, batch: vec![Batch::from_updates([((0, vec![value]), 1)])?] })
    };
    let first = query.prepare(tick(1, 3)?).await?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(first.output().batch, Batch::from_updates([((0, vec![9]), 1)])?);
    assert_eq!(query.time(), 0);
    query.commit(first)?;
    let pinned = query.snapshot();
    let checkpoint = query.checkpoint()?;
    assert!(query.prepare(tick(2, 5)?).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &query.snapshot()));
    assert_eq!(query.checkpoint()?, checkpoint);
    let retry = query.prepare(tick(2, 0)?).await?;
    // Both prior/new cross terms plus the new/new term see the original prior.
    assert_eq!(retry.output().batch, Batch::from_updates([((0, vec![0]), 3)])?);
    query.commit(retry)?;
    assert_eq!(query.time(), 2);
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    assert_eq!(
        pinned.trace("input")?.materialize().await?,
        Batch::from_updates([((0, vec![3]), 1)])?
    );
    Ok(())
}

#[tokio::test]
async fn independent_uploads_overlap_and_failures_drain_without_state_advance() -> Result<()> {
    use crate::engine::tests::store::Store;
    use std::sync::atomic::Ordering;
    for fail_first in [false, true] {
        let store = Arc::new(Store::new(fail_first));
        let settings = Settings {
            store: store.clone(),
            block_rows: 2,
            limits: crate::engine::execution::Limits::default(),
            encoded_cache: None,
        };
        let mut query = Relational::new(plan()?, operators(), &settings)?;
        let prior = serde_json::to_value(query.checkpoint()?)?;
        let inputs =
            (0..3).map(|n| Batch::from_updates([((1, vec![n]), 1)])).collect::<Result<Vec<_>>>()?;
        let result = query.prepare(TimedBatch { time: 1, batch: inputs }).await;
        assert!(store.peak.load(Ordering::SeqCst) > 1);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert_eq!(query.time(), 0);
        assert_eq!(serde_json::to_value(query.checkpoint()?)?, prior);
        if fail_first {
            assert!(result.is_err());
            assert!(store.puts.load(Ordering::SeqCst) > 1);
        } else {
            let prepared = result?;
            assert_eq!(store.reads.load(Ordering::SeqCst), 0);
            query.commit(prepared)?;
            assert_eq!(query.time(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn memtable_preserves_join_cross_terms_duplicates_and_flushes_to_reopenable_objects()
-> Result<()> {
    use crate::engine::tests::store::Store;
    use std::sync::atomic::Ordering;
    let store = Arc::new(Store::default());
    let settings = Settings {
        store: store.clone(),
        block_rows: 16,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    };
    let mut query = Relational::new(plan()?, operators(), &settings)?;
    let input = |time, weight| -> Result<_> {
        Ok(TimedBatch {
            time,
            batch: (1..=3)
                .map(|value| Batch::from_updates([((0, vec![value]), weight)]))
                .collect::<Result<Vec<_>>>()?,
        })
    };
    assert!(
        query
            .prepare_memtable(input(1, 2)?, 1)
            .await
            .err()
            .context("expected memory limit")?
            .is::<super::MemtableFull>()
    );
    assert_eq!(query.time(), 0);
    let work = query.prepare_memtable(input(1, 2)?, 1_000_000).await?;
    assert_eq!(work.output().batch, Batch::from_updates([((0, vec![1, 2, 3]), 8)])?);
    query.commit(work)?;
    let work = query.prepare_memtable(input(2, -1)?, 1_000_000).await?;
    assert_eq!(work.output().batch, Batch::from_updates([((0, vec![1, 2, 3]), -7)])?);
    query.commit(work)?;
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    assert!(query.checkpoint().is_err());
    let compacted = query.prepare_compaction().await?;
    let checkpoint = query.prepared_compaction_checkpoint(&compacted)?;
    query.commit_compaction(compacted)?;
    let restored = Relational::reopen(plan()?, operators(), settings, checkpoint).await?;
    assert_eq!(restored.time(), 2);
    assert_eq!(
        restored.snapshot().trace("abc")?.materialize().await?,
        Batch::from_updates([((0, vec![1, 2, 3]), 1)])?
    );
    assert!(store.puts.load(Ordering::SeqCst) > 0);
    Ok(())
}
