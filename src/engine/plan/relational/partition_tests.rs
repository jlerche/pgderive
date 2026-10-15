use super::{Operators, Relational};
use crate::engine::{
    Batch,
    dataflow::{Partition, Project, TimedBatch},
    plan::{Arrangement, Definition, Kind, Node, Plan, Source, query::Settings},
};
use anyhow::Result;
use object_store::memory::InMemory;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn plan() -> Result<Plan> {
    Plan::new(Definition {
        revision: "retained-partition-test-v1".into(),
        sources: vec![Source { id: "source".into(), schema: "i64-v1".into() }],
        nodes: [
            ("source", Kind::Source, "source"),
            ("input", Kind::Project, "source"),
            ("partition", Kind::Aggregate, "input"),
            ("output", Kind::Project, "partition"),
        ]
        .into_iter()
        .map(|(id, kind, input)| Node {
            id: id.into(),
            kind,
            inputs: vec![input.into()],
            schema: "i64-v1".into(),
        })
        .collect(),
        arrangements: ["input", "partition", "output"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "i64-v1".into() })
            .collect(),
        outputs: vec!["output".into()],
    })
}
fn operators(calls: &Arc<AtomicUsize>) -> Operators<i64, i64> {
    let calls = calls.clone();
    let projects = [
        ("input".into(), Arc::new(Project::new(|key: &i64, value: &i64| Ok(Some((*key, *value)))))),
        ("output".into(), Arc::new(Project::new(|_: &i64, value: &i64| Ok(Some((0, *value)))))),
    ]
    .into_iter()
    .collect();
    let partition = Partition::new_zero_preserving(move |_: &i64, rows: &Batch<(), i64>, work| {
        calls.fetch_add(1, Ordering::Relaxed);
        work.charge(rows.iter().count())?;
        anyhow::ensure!(rows.iter().all(|(((), value), _)| *value != 99), "callback failure");
        let count = rows.iter().map(|(_, weight)| *weight).sum::<i64>();
        Batch::from_updates([(((), count), 1)])
    });
    Operators {
        projects,
        partitions: BTreeMap::from([("partition".into(), Arc::new(partition))]),
        joins: BTreeMap::new(),
        statistics: BTreeMap::new(),
        ordered: BTreeMap::new(),
        expansions: BTreeMap::new(),
    }
}
#[tokio::test]
async fn retained_partition_circuit_restore_collisions_and_atomic_failure() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let settings = Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: crate::engine::execution::Limits::default(),
        encoded_cache: None,
    };
    let mut query = Relational::new(plan()?, operators(&calls), &settings)?;
    let changes = [
        vec![((0, 1), 2), ((1, 1), 2)],
        vec![((0, 1), -1), ((0, 2), 1)],
        vec![((0, 1), -1), ((0, 2), -1)],
        vec![((0, 3), 2)],
    ];
    let expected = [vec![((0, 2), 2)], vec![], vec![((0, 2), -1)], vec![((0, 2), 1)]];
    for (changes, expected) in changes.into_iter().zip(expected) {
        let input =
            TimedBatch { time: query.time() + 1, batch: vec![Batch::from_updates(changes)?] };
        let prepared = query.prepare(input).await?;
        assert_eq!(prepared.output().batch, Batch::from_updates(expected)?);
        query.commit(prepared)?;
        let checkpoint = query.checkpoint()?;
        assert_eq!(checkpoint.arrangements.len(), 3);
        query = Relational::new(plan()?, operators(&calls), &settings)?;
        query.restore_checkpoint(checkpoint).await?;
    }
    assert_eq!(calls.load(Ordering::Relaxed), 4);
    let before = query.checkpoint()?;
    let invalid = TimedBatch { time: 5, batch: vec![Batch::from_updates([((1, 99), 1)])?] };
    assert!(query.prepare(invalid).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    assert_eq!(query.time(), 4);
    let empty = TimedBatch { time: 5, batch: vec![Batch::from_updates([])?] };
    query.commit(query.prepare(empty).await?)?;
    assert_eq!(query.time(), 5);
    let mut missing = plan()?.definition().clone();
    missing.arrangements.retain(|entry| entry.node != "partition");
    assert!(Relational::new(Plan::new(missing)?, operators(&calls), &settings).is_err());
    Ok(())
}
