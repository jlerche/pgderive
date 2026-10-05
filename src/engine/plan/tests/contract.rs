use super::{Arrangement, Binding, Definition, Engine, Kind, Node, Plan, Source, State};
use crate::engine::{
    Batch,
    dataflow::{Arrangement as Writer, GroupSum, Join, Project, TimedBatch},
    trace::TraceSnapshot,
};
use anyhow::{Result, ensure};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
fn definition() -> Definition {
    let nodes = vec![
        Node {
            id: "a".into(),
            kind: Kind::Source,
            inputs: vec!["as".into()],
            schema: "i64-v1".into(),
        },
        Node {
            id: "b".into(),
            kind: Kind::Source,
            inputs: vec!["bs".into()],
            schema: "i64-v1".into(),
        },
        Node {
            id: "c".into(),
            kind: Kind::Source,
            inputs: vec!["cs".into()],
            schema: "i64-v1".into(),
        },
        Node {
            id: "ab".into(),
            kind: Kind::Join,
            inputs: vec!["a".into(), "b".into()],
            schema: "pair-v1".into(),
        },
        Node {
            id: "abc".into(),
            kind: Kind::Join,
            inputs: vec!["ab".into(), "c".into()],
            schema: "triple-v1".into(),
        },
        Node {
            id: "project".into(),
            kind: Kind::Project,
            inputs: vec!["abc".into()],
            schema: "i64-v1".into(),
        },
        Node {
            id: "sum".into(),
            kind: Kind::Aggregate,
            inputs: vec!["project".into()],
            schema: "sum-v1".into(),
        },
    ];
    Definition {
        revision: "chain-v1".into(),
        sources: ["as", "bs", "cs"]
            .into_iter()
            .map(|id| Source { id: id.into(), schema: "i64-v1".into() })
            .collect(),
        arrangements: [
            ("a", "a", "i64-v1"),
            ("b", "b", "i64-v1"),
            ("c", "c", "i64-v1"),
            ("ab", "ab", "pair-v1"),
            ("sum", "sum", "sum-v1"),
        ]
        .into_iter()
        .map(|(id, node, schema)| Arrangement {
            id: id.into(),
            node: node.into(),
            schema: schema.into(),
        })
        .collect(),
        nodes,
        outputs: vec!["sum".into()],
    }
}
#[test]
fn registrations_are_checked_and_semantic_changes_change_identity() -> Result<()> {
    let valid = definition();
    let plan = Plan::new(valid.clone())?;
    assert_eq!(plan.identity(), Plan::new(valid.clone())?.identity());
    let mut changed = valid.clone();
    changed.revision = "chain-v2".into();
    assert_ne!(plan.identity(), Plan::new(changed)?.identity());
    let mut invalid = valid.clone();
    invalid.nodes[3].inputs[0] = "abc".into();
    assert!(Plan::new(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.sources[0].schema = "changed".into();
    assert!(Plan::new(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.nodes[3].inputs.pop();
    assert!(Plan::new(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.arrangements[1].id = "a".into();
    assert!(Plan::new(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.outputs = vec!["a".into()];
    assert!(Plan::new(invalid).is_err());
    let mut invalid = valid;
    invalid.outputs.clear();
    assert!(Plan::new(invalid).is_err());
    Ok(())
}
type Rows = Batch<i64, i64>;
type Inputs = (Rows, Rows, Rows);
#[derive(Clone)]
struct Snapshot {
    a: TraceSnapshot<i64, i64>,
    b: TraceSnapshot<i64, i64>,
    c: TraceSnapshot<i64, i64>,
    ab: TraceSnapshot<i64, (i64, i64)>,
    sum: TraceSnapshot<i64, crate::engine::dataflow::SumState>,
}
impl State for Snapshot {
    fn bindings(&self) -> Vec<Binding> {
        [
            ("a", "i64-v1", self.a.time()),
            ("b", "i64-v1", self.b.time()),
            ("c", "i64-v1", self.c.time()),
            ("ab", "pair-v1", self.ab.time()),
            ("sum", "sum-v1", self.sum.time()),
        ]
        .into_iter()
        .map(|(id, schema, time)| Binding { id: id.into(), schema: schema.into(), time })
        .collect()
    }
}
struct Writers {
    a: Writer<i64, i64>,
    b: Writer<i64, i64>,
    c: Writer<i64, i64>,
    ab: Writer<i64, (i64, i64)>,
    sum: Writer<i64, crate::engine::dataflow::SumState>,
}
impl Writers {
    fn new() -> Result<Self> {
        let store = Arc::new(InMemory::new());
        Ok(Self {
            a: Writer::new(store.clone(), "i64-v1".into(), 2)?,
            b: Writer::new(store.clone(), "i64-v1".into(), 2)?,
            c: Writer::new(store.clone(), "i64-v1".into(), 2)?,
            ab: Writer::new(store.clone(), "pair-v1".into(), 2)?,
            sum: Writer::new(store, "sum-v1".into(), 2)?,
        })
    }
    const fn empty(&self) -> Snapshot {
        Snapshot {
            a: self.a.empty(),
            b: self.b.empty(),
            c: self.c.empty(),
            ab: self.ab.empty(),
            sum: self.sum.empty(),
        }
    }
    async fn step(
        &self,
        prior: Arc<Snapshot>,
        input: TimedBatch<Inputs>,
    ) -> Result<(Snapshot, ())> {
        let (a, b, c) = input.batch;
        let time = input.time;
        let a = TimedBatch { time, batch: a };
        let b = TimedBatch { time, batch: b };
        let c = TimedBatch { time, batch: c };
        let ab = Join.evaluate(&a, &b, &prior.a, &prior.b).await?;
        let abc = Join.evaluate(&ab, &c, &prior.ab, &prior.c).await?;
        let projected =
            Project::new(|key: &i64, ((a, b), c): &((i64, i64), i64)| Ok(Some((*key, a + b + c))))
                .evaluate(&abc)?;
        let sums = GroupSum::new(|_: &i64, value: &i64| Ok(Some(*value)))
            .evaluate(&projected, &prior.sum)
            .await?;
        Ok((
            Snapshot {
                a: self.a.stage(&prior.a, &a).await?,
                b: self.b.stage(&prior.b, &b).await?,
                c: self.c.stage(&prior.c, &c).await?,
                ab: self.ab.stage(&prior.ab, &ab).await?,
                sum: self.sum.stage(&prior.sum, &sums.state).await?,
            },
            (),
        ))
    }
}
#[tokio::test]
async fn three_sources_chained_joins_projection_count_sum_match_recomputation() -> Result<()> {
    let writers = Arc::new(Writers::new()?);
    let initial = writers.empty();
    let mut engine = Engine::new(Plan::new(definition())?, initial, move |prior, input| {
        let writers = writers.clone();
        async move { writers.step(prior, input).await }
    })?;
    let mut bags = [BTreeMap::<(i64, i64), i64>::new(), BTreeMap::new(), BTreeMap::new()];
    for time in 1_i64..=12 {
        let updates = [
            vec![((time % 2, time % 3), 1)],
            vec![((time % 2, time % 4), 1)],
            vec![((time % 2, time % 5), 1)],
        ];
        for (bag, rows) in bags.iter_mut().zip(&updates) {
            for (row, weight) in rows {
                *bag.entry(*row).or_default() += weight;
            }
        }
        let inputs = TimedBatch {
            time: u64::try_from(time)?,
            batch: (
                Batch::from_updates(updates[0].clone())?,
                Batch::from_updates(updates[1].clone())?,
                Batch::from_updates(updates[2].clone())?,
            ),
        };
        engine.commit(engine.prepare(inputs).await?)?;
        let expected = recompute(&bags);
        let rows = expected.into_iter().map(|(key, (rows, sum))| {
            ((key, crate::engine::dataflow::SumState { rows, non_null: rows, sum }), 1)
        });
        ensure!(
            engine.snapshot().sum.materialize().await? == Batch::from_updates(rows)?,
            "chained join differs from oracle"
        );
    }
    Ok(())
}

type Bag = BTreeMap<(i64, i64), i64>;
fn recompute(bags: &[Bag; 3]) -> BTreeMap<i64, (i64, i64)> {
    let mut expected = BTreeMap::<i64, (i64, i64)>::new();
    for ((ka, a), wa) in &bags[0] {
        for ((kb, b), wb) in &bags[1] {
            for ((kc, c), wc) in &bags[2] {
                if ka == kb && ka == kc {
                    let total = expected.entry(*ka).or_default();
                    total.0 += wa * wb * wc;
                    total.1 += (a + b + c) * wa * wb * wc;
                }
            }
        }
    }
    expected
}

#[derive(Clone)]
struct Report(Vec<Binding>);
impl State for Report {
    fn bindings(&self) -> Vec<Binding> {
        self.0.clone()
    }
}
#[tokio::test]
async fn incomplete_or_mismatched_node_state_never_publishes() -> Result<()> {
    let plan = Plan::new(definition())?;
    let initial = Report(
        plan.definition()
            .arrangements
            .iter()
            .map(|a| Binding { id: a.id.clone(), schema: a.schema.clone(), time: 0 })
            .collect(),
    );
    let mut engine =
        Engine::new(plan, initial, |prior: Arc<Report>, input: TimedBatch<u8>| async move {
            let mut next = (*prior).clone();
            for binding in &mut next.0 {
                binding.time = input.time;
            }
            match input.batch {
                1 => next.0[0].time = 0,
                2 => next.0[0].schema = "wrong".into(),
                3 => {
                    next.0.pop();
                }
                4 => next.0[0].id = next.0[1].id.clone(),
                _ => {}
            }
            Ok((next, ()))
        })?;
    let pinned = engine.snapshot();
    for failure in 1..=4 {
        assert!(engine.prepare(TimedBatch { time: 1, batch: failure }).await.is_err());
        assert!(Arc::ptr_eq(&pinned, &engine.snapshot()));
    }
    let prepared = engine.prepare(TimedBatch { time: 1, batch: 0 }).await?;
    engine.commit(prepared)?;
    assert_eq!(engine.time(), 1);
    assert_eq!(pinned.0[0].time, 0);
    Ok(())
}

#[path = "typed.rs"]
mod typed;
