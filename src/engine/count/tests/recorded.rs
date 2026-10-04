use crate::engine::{
    Batch, Circuit, GroupedCount, IncrementalJoin, ZSet,
    dataflow::{Arrangement, GroupSum, Join, Project, Stream, SumState, TraceQuery},
    trace::TraceSnapshot,
};
use anyhow::{Result, bail, ensure};
use object_store::memory::InMemory;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

type Task = (i64, i64, String, i64);
type TaskInput = ((i64, Task), i64);
type ProjectInput = ((i64, i64), i64);
type Inputs = (ZSet<(i64, Task)>, ZSet<(i64, i64)>);
type CountRow = ((i64, i64), i64);
type Image = Option<Value>;
type Event = (String, Image, Image);

#[derive(Deserialize)]
struct Record {
    tx: u64,
    rollback: bool,
    events: Vec<Event>,
}

#[derive(Clone, Default)]
struct Graph {
    join: IncrementalJoin<i64, Task, i64>,
    count: GroupedCount<(i64, i64)>,
    output: ZSet<CountRow>,
}

fn evaluate(state: &mut Graph, input: &Inputs) -> Result<()> {
    let joined = state.join.step(&input.0, &input.1)?;
    let selected = joined
        .delta
        .try_filter(|(_, task, _)| Ok(task.2 == "open" && task.3 >= 8))?
        .try_map(|(project, task, organization)| Ok(((*project, *organization), task.clone())))?;
    state.output.apply(&state.count.step(&selected)?)?;
    Ok(())
}

fn oracle(tasks: &BTreeMap<i64, Task>, projects: &BTreeMap<i64, i64>) -> Result<ZSet<CountRow>> {
    let mut counts = BTreeMap::new();
    for task in tasks.values() {
        if task.2 == "open"
            && task.3 >= 8
            && let Some(organization) = projects.get(&task.1)
        {
            *counts.entry((task.1, *organization)).or_insert(0_i64) += 1;
        }
    }
    ZSet::from_updates(counts.into_iter().map(|row| (row, 1)))
}

fn replace<T: Clone + Eq>(
    model: &mut BTreeMap<i64, T>,
    old: Option<T>,
    new: Option<T>,
    key: impl Fn(&T) -> i64,
) -> Result<()> {
    if let Some(old) = old {
        ensure!(model.remove(&key(&old)).as_ref() == Some(&old), "recorded old row mismatch");
    }
    if let Some(new) = new {
        ensure!(model.insert(key(&new), new).is_none(), "recorded duplicate row");
    }
    Ok(())
}

fn replay(
    record: Record,
    tasks: &mut BTreeMap<i64, Task>,
    projects: &mut BTreeMap<i64, i64>,
) -> Result<Inputs> {
    let mut left: Vec<TaskInput> = Vec::new();
    let mut right: Vec<ProjectInput> = Vec::new();
    for (kind, old, new) in record.events {
        match kind.as_str() {
            "task" => {
                let old = old.map(serde_json::from_value::<Task>).transpose()?;
                let new = new.map(serde_json::from_value::<Task>).transpose()?;
                for (image, sign) in [(&old, -1), (&new, 1)] {
                    if let Some(task) = image {
                        left.push(((task.1, task.clone()), sign));
                    }
                }
                replace(tasks, old, new, |task| task.0)?;
            }
            "project" => {
                let old = old.map(serde_json::from_value::<(i64, i64)>).transpose()?;
                let new = new.map(serde_json::from_value::<(i64, i64)>).transpose()?;
                for (image, sign) in [(&old, -1), (&new, 1)] {
                    if let Some((key, organization)) = image {
                        ensure!((1..=32).contains(key), "recorded project outside replay scope");
                        right.push(((*key, *organization), sign));
                    }
                }
                let mut rows = projects.iter().map(|(key, org)| (*key, (*key, *org))).collect();
                replace(&mut rows, old, new, |row| row.0)?;
                *projects = rows.into_values().collect();
            }
            _ => bail!("unknown recorded event"),
        }
    }
    Ok((ZSet::from_updates(left)?, ZSet::from_updates(right)?))
}

#[tokio::test]
async fn accepted_poc_trace_matches_independent_source_map_oracle() -> Result<()> {
    let mut tasks = (1..=10_000)
        .map(|id| {
            let status = if id % 3 == 0 { "closed" } else { "open" };
            (id, (id, id, status.into(), (id - 1) % 10 + 1))
        })
        .collect::<BTreeMap<_, Task>>();
    tasks.extend((20_000..20_512).map(|id| (id, (id, 1, "open".into(), 9))));
    let mut projects = (1..=32).map(|key| (key, key)).collect::<BTreeMap<_, _>>();
    let mut circuit = Circuit::<Graph>::default();
    let counts = TraceQuery::new(
        Arc::new(InMemory::new()),
        "poc-tasks-projects-v1".into(),
        64,
        |project, task: &Task, organization| {
            Ok((task.2 == "open" && task.3 >= 8).then_some((*project, *organization)))
        },
    )?;
    let writer = Arrangement::new(Arc::new(InMemory::new()), "poc-sum-state-v1".into(), 64)?;
    let mut trace = TraceGraph { counts, sums: writer.empty(), writer };
    let initial = (
        ZSet::from_updates(tasks.values().map(|task| ((task.1, task.clone()), 1)))?,
        ZSet::from_updates(projects.iter().map(|(key, org)| ((*key, *org), 1)))?,
    );
    circuit.step(&initial, evaluate)?;
    verify_trace(&mut trace, 1, &initial, &tasks, &projects).await?;
    assert_eq!(circuit.state().output, oracle(&tasks, &projects)?);
    let mut committed = 0;
    let mut rollbacks = 0;
    for (ordinal, line) in
        include_str!("../../../../tests/fixtures/zset-contract-events.jsonl").lines().enumerate()
    {
        let record: Record = serde_json::from_str(line)?;
        assert_eq!(record.tx, u64::try_from(ordinal)?);
        if record.rollback {
            rollbacks += 1;
            continue;
        }
        let input = replay(record, &mut tasks, &mut projects)?;
        let step = circuit.step(&input, evaluate)?;
        committed += 1;
        assert_eq!(step.time, committed + 1);
        verify_trace(&mut trace, step.time, &input, &tasks, &projects).await?;
        assert_eq!(circuit.state().output, oracle(&tasks, &projects)?, "transaction {ordinal}");
    }
    assert_eq!(committed, 111);
    assert_eq!(rollbacks, 9);
    Ok(())
}

type Group = (i64, i64);
struct TraceGraph {
    counts: TraceQuery<i64, Task, i64, Group>,
    sums: TraceSnapshot<Group, SumState>,
    writer: Arrangement<Group, SumState>,
}
async fn verify_trace(
    graph: &mut TraceGraph,
    time: u64,
    input: &Inputs,
    tasks: &BTreeMap<i64, Task>,
    projects: &BTreeMap<i64, i64>,
) -> Result<()> {
    // Accepted fixture's project domain is 1..=32. Scope both old and new task
    // images before forming this arrangement; source-map validation stays complete.
    let left = Stream {
        time,
        batch: Batch::from_updates(
            input
                .0
                .iter()
                .filter(|((project, _), _)| (1..=32).contains(project))
                .map(|(row, w)| (row.clone(), *w)),
        )?,
    };
    let right =
        Stream { time, batch: Batch::from_updates(input.1.iter().map(|(row, w)| (*row, *w)))? };
    let prior = graph.counts.snapshot();
    let joined = Join.evaluate(&left, &right, &prior.left, &prior.right).await?;
    let project = Project::new(|key: &i64, (task, org): &(Task, i64)| {
        Ok((task.2 == "open" && task.3 >= 8).then_some(((*key, *org), task.clone())))
    });
    let selected = project.evaluate(&joined)?;
    let sum = GroupSum::new(|_: &Group, task: &Task| Ok(Some(task.3)));
    let delta = sum.evaluate(&selected, &graph.sums).await?;
    graph.sums = graph.writer.stage(&graph.sums, &delta.state).await?;
    verify_sum(&graph.sums, tasks, projects).await?;
    let prepared = graph.counts.prepare(&left, &right).await?;
    graph.counts.commit(prepared)?;
    let snapshot = graph.counts.snapshot();
    assert_eq!(
        snapshot.counts.materialize().await?,
        Batch::from_updates(oracle(tasks, projects)?.iter().map(|(row, w)| (*row, *w)))?
    );
    assert_eq!(
        snapshot.left.materialize().await?,
        Batch::from_updates(
            tasks
                .values()
                .filter(|task| (1..=32).contains(&task.1))
                .map(|task| ((task.1, task.clone()), 1))
        )?
    );
    assert_eq!(
        snapshot.right.materialize().await?,
        Batch::from_updates(projects.iter().map(|(key, org)| ((*key, *org), 1)))?
    );
    Ok(())
}

async fn verify_sum(
    state: &TraceSnapshot<Group, SumState>,
    tasks: &BTreeMap<i64, Task>,
    projects: &BTreeMap<i64, i64>,
) -> Result<()> {
    let mut expected = BTreeMap::<Group, (i64, i64)>::new();
    for task in tasks.values() {
        if task.2 != "open" || task.3 < 8 {
            continue;
        }
        if let Some(org) = projects.get(&task.1) {
            let accumulated = expected.entry((task.1, *org)).or_default();
            accumulated.0 += 1;
            accumulated.1 += task.3;
        }
    }
    assert_eq!(
        state.materialize().await?,
        Batch::from_updates(
            expected
                .into_iter()
                .map(|(key, (rows, sum))| ((key, SumState { rows, non_null: rows, sum }), 1))
        )?
    );
    Ok(())
}
