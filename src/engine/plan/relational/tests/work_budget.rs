use super::super::{Operator, Operators, Relational};
use crate::engine::{
    Batch,
    dataflow::{Group, OrderedSelection, Partition, Project, TimedBatch},
    execution::{CapacityExceeded, Limits},
    plan::{Arrangement, Definition, Kind, Node, Plan, Settings, Source},
};
use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::sync::Arc;

type Input = TimedBatch<Vec<Batch<i64, i64>>>;

fn settings(resident_entries: usize, contributions: usize) -> Settings {
    Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 16,
        limits: Limits { resident_entries, contributions, ..Limits::default() },
        encoded_cache: None,
    }
}
fn unary(operator: Operator<i64, i64>, options: &Settings) -> Result<Relational<i64, i64>> {
    let kind = operator.kind();
    let plan = Plan::new(Definition {
        revision: "work-scope-v1".into(),
        sources: vec![Source { id: "s".into(), schema: "i64".into() }],
        nodes: [("s", Kind::Source, "s"), ("p", Kind::Project, "s"), ("q", kind, "p")]
            .into_iter()
            .map(|(id, kind, input)| Node {
                id: id.into(),
                kind,
                inputs: vec![input.into()],
                schema: "i64".into(),
            })
            .collect(),
        arrangements: ["p", "q"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "i64".into() })
            .collect(),
        outputs: vec!["q".into()],
    })?;
    Relational::new(
        plan,
        Operators::from([
            ("p".into(), Operator::Project(Project::new(|k: &i64, v: &i64| Ok(Some((*k, *v)))))),
            ("q".into(), operator),
        ]),
        options,
    )
}
fn input(time: u64, value: i64) -> Result<Input> {
    Ok(TimedBatch { time, batch: vec![Batch::from_updates([((1, value), 1), ((2, value), 1)])?] })
}
#[tokio::test]
async fn complete_operator_work_is_shared_across_resident_and_spilled_keys() -> Result<()> {
    for resident in [1, 4096] {
        let operators = [
            (
                Operator::Partition(Partition::new_zero_preserving(|_, _, work| {
                    work.charge(2)?;
                    Batch::from_updates([])
                })),
                5,
            ),
            (
                Operator::Group(Group::new(|_, work| {
                    Box::pin(async move {
                        work.charge(2)?;
                        Batch::from_updates([])
                    })
                })),
                5,
            ),
            (Operator::Ordered(OrderedSelection::new(1, |k, v, _| Ok((*k, *v)))), 3),
        ];
        for (operator, limit) in operators {
            let query = unary(operator, &settings(resident, limit))?;
            let before = query.checkpoint()?;
            let error =
                query.prepare(input(1, 0)?).await.err().context("work budget reset per key")?;
            assert!(error.is::<CapacityExceeded>(), "{error:#}");
            assert_eq!(query.checkpoint()?, before);
        }
    }
    Ok(())
}
#[tokio::test]
async fn canceled_partition_outputs_still_charge_complete_epoch_contributions() -> Result<()> {
    for resident in [1, 4096] {
        let callback =
            Partition::new(|_: &i64, _: &Batch<(), i64>, _| Batch::from_updates([(((), 0), 1)]));
        let query = unary(Operator::Partition(callback), &settings(resident, 3))?;
        let error =
            query.prepare(input(1, 0)?).await.err().context("canceled outputs escaped budget")?;
        assert!(error.is::<CapacityExceeded>(), "{error:#}");
        assert_eq!(query.time(), 0);
    }
    Ok(())
}
#[tokio::test]
async fn unchanged_ordered_outputs_still_charge_complete_epoch_contributions() -> Result<()> {
    for resident in [1, 4096] {
        let callback = OrderedSelection::first(Project::new(|k: &i64, v: &i64| Ok(Some((*k, *v)))));
        let mut query = unary(Operator::Ordered(callback), &settings(resident, 3))?;
        query.commit(query.prepare(input(1, 0)?).await?)?;
        let before = query.checkpoint()?;
        let error = query
            .prepare(input(2, 1)?)
            .await
            .err()
            .context("unchanged selections escaped budget")?;
        assert!(error.is::<CapacityExceeded>(), "{error:#}");
        assert_eq!(query.checkpoint()?, before);
    }
    Ok(())
}

fn joined(options: &Settings) -> Result<Relational<i64, i64>> {
    let plan = Plan::new(Definition {
        revision: "join-work-scope-v1".into(),
        sources: ["l", "r"]
            .into_iter()
            .map(|id| Source { id: id.into(), schema: "i64".into() })
            .collect(),
        nodes: [
            ("l", Kind::Source, vec!["l"]),
            ("r", Kind::Source, vec!["r"]),
            ("lp", Kind::Project, vec!["l"]),
            ("rp", Kind::Project, vec!["r"]),
            ("j", Kind::Join, vec!["lp", "rp"]),
        ]
        .into_iter()
        .map(|(id, kind, inputs)| Node {
            id: id.into(),
            kind,
            inputs: inputs.into_iter().map(Into::into).collect(),
            schema: "i64".into(),
        })
        .collect(),
        arrangements: ["lp", "rp", "j"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "i64".into() })
            .collect(),
        outputs: vec!["j".into()],
    })?;
    let mut operators = Operators::new();
    for id in ["lp", "rp"] {
        operators.insert(
            id.into(),
            Operator::Project(Project::new(|k: &i64, v: &i64| Ok(Some((*k, *v))))),
        );
    }
    operators.insert(
        "j".into(),
        Operator::Join(Project::new(|k: &i64, pair: &(i64, i64)| Ok(Some((*k, pair.0 + pair.1))))),
    );
    Relational::new(plan, operators, options)
}
fn pair(time: u64, left: i64, right: i64) -> Result<Input> {
    Ok(TimedBatch {
        time,
        batch: [left, right]
            .into_iter()
            .map(|weight| Batch::from_updates([((1, 0), weight), ((2, 0), weight)]))
            .collect::<Result<Vec<_>>>()?,
    })
}
#[tokio::test]
async fn canceled_join_cross_terms_charge_all_keys_before_consolidation() -> Result<()> {
    for resident in [1, 4096] {
        for budget in [5, 6] {
            let mut query = joined(&settings(resident, budget))?;
            query.commit(query.prepare(pair(1, 2, 2)?).await?)?;
            let before = query.checkpoint()?;
            let prepared = query.prepare(pair(2, 2, -1)?).await;
            if budget == 5 {
                let error = prepared.err().context("pair contributions escaped budget")?;
                assert!(error.is::<CapacityExceeded>(), "{error:#}");
                assert_eq!(query.checkpoint()?, before);
            } else {
                let prepared = prepared?;
                assert!(prepared.output().batch.iter().next().is_none());
                query.commit(prepared)?;
                assert_eq!(query.time(), 2);
            }
        }
    }
    Ok(())
}
