use crate::engine::{Circuit, GroupedCount, IncrementalJoin, ZSet};
use anyhow::{Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

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

#[test]
fn accepted_poc_trace_matches_independent_source_map_oracle() -> Result<()> {
    let mut tasks = (1..=10_000)
        .map(|id| {
            let status = if id % 3 == 0 { "closed" } else { "open" };
            (id, (id, id, status.into(), (id - 1) % 10 + 1))
        })
        .collect::<BTreeMap<_, Task>>();
    tasks.extend((20_000..20_512).map(|id| (id, (id, 1, "open".into(), 9))));
    let mut projects = (1..=32).map(|key| (key, key)).collect::<BTreeMap<_, _>>();
    let mut circuit = Circuit::<Graph>::default();
    let initial = (
        ZSet::from_updates(tasks.values().map(|task| ((task.1, task.clone()), 1)))?,
        ZSet::from_updates(projects.iter().map(|(key, org)| ((*key, *org), 1)))?,
    );
    circuit.step(&initial, evaluate)?;
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
        assert_eq!(circuit.state().output, oracle(&tasks, &projects)?, "transaction {ordinal}");
    }
    assert_eq!(committed, 111);
    assert_eq!(rollbacks, 9);
    Ok(())
}
