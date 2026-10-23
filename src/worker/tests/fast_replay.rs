//! Reuse application trace/oracle exports; only the compiled engine is timed.
use super::{build, program, settings};
use crate::{
    source::{Column, Contract, Relation},
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

type Cells = Vec<Option<String>>;
type Bag = BTreeMap<Cells, i64>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Suite {
    version: u32,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    sql: String,
    steps: Vec<Step>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Step {
    updates: Vec<Update>,
    expected: Option<Vec<(Cells, i64)>>,
    measure: bool,
    restart: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    table: String,
    row: BTreeMap<String, Option<String>>,
    weight: i64,
}

fn native() -> Contract {
    let mut native = super::contract();
    native.relations = [
        ("account", vec![("id", 23), ("label", 25), ("cohort", 23)]),
        ("entry", vec![("id", 23), ("account_id", 23), ("amount", 23)]),
    ]
    .into_iter()
    .zip(1..)
    .map(|((table, columns), oid)| Relation {
        oid,
        schema: "source".into(),
        table: table.into(),
        columns: columns
            .into_iter()
            .zip(1..)
            .map(|((name, oid), position)| Column {
                name: name.into(),
                oid,
                position,
                modifier: -1,
                nullable: name != "id",
                primary: name == "id",
                collation: if oid == 25 { 100 } else { 0 },
            })
            .collect(),
    })
    .collect();
    native
}
fn changes(updates: Vec<Update>) -> weighted::Batch {
    weighted::Batch {
        updates: updates
            .into_iter()
            .map(|update| weighted::Update {
                tuple: weighted::Tuple {
                    schema: "source".into(),
                    table: update.table,
                    row: Row::from(update.row),
                },
                weight: update.weight,
            })
            .collect(),
    }
}
fn integrate(bag: &mut Bag, rows: &super::ProjectedBag) -> Result<()> {
    for (((), row), weight) in rows.iter() {
        let cells = row
            .iter()
            .map(|cell| {
                cell.as_ref()
                    .map(|cell| {
                        let json = serde_json::to_value(cell)?;
                        Ok(json.as_str().map_or_else(|| json.to_string(), str::to_owned))
                    })
                    .transpose()
            })
            .collect::<Result<Cells>>()?;
        let count = bag
            .get(&cells)
            .copied()
            .unwrap_or_default()
            .checked_add(*weight)
            .context("replay output weight overflow")?;
        if count == 0 {
            bag.remove(&cells);
        } else {
            bag.insert(cells, count);
        }
    }
    Ok(())
}
async fn run(case: Case) -> Result<serde_json::Value> {
    let native = native();
    let compiled = crate::compiler::compile(&case.sql, &native)?;
    let mut settings = settings();
    settings.block_rows = 4096;
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    settings.store = store.clone();
    let mut query = build(&native, &compiled, &settings)?;
    let mut output = Bag::new();
    let mut elapsed = Vec::new();
    let mut measured_changes = 0;
    let mut cache_misses = 0;
    let mut object_reads = 0;
    let mut object_puts = 0;
    let mut oracle_checks = 0;
    for (index, step) in case.steps.into_iter().enumerate() {
        if step.restart {
            let compacted = query.prepare_compaction().await?;
            query.commit_compaction(compacted)?;
            super::restart_relational(&mut query, &native, &compiled, &settings).await?;
        } else {
            let count = step.updates.len();
            let input = program::relational::inputs(
                &changes(step.updates),
                compiled.relational(),
                query.time() + 1,
            )?;
            let before = query.cache_stats()?;
            let reads = store.reads.load(Ordering::SeqCst);
            let puts = store.puts.load(Ordering::SeqCst);
            let started = Instant::now();
            let work = query
                .prepare_memtable(input, 256 * 1024 * 1024)
                .await
                .with_context(|| format!("replay {} step {index}", case.name))?;
            let delta = query.commit(work)?;
            if step.measure {
                elapsed.push(started.elapsed().as_nanos());
                measured_changes += count;
                cache_misses += query.cache_stats()?.misses - before.misses;
                object_reads += store.reads.load(Ordering::SeqCst) - reads;
                object_puts += store.puts.load(Ordering::SeqCst) - puts;
            }
            integrate(&mut output, &super::projected_batch(&delta.batch, &compiled)?)?;
        }
        if let Some(expected) = step.expected {
            oracle_checks += 1;
            ensure!(
                output == expected.into_iter().collect::<Bag>(),
                "replay {} step {index} differs from independent application oracle",
                case.name
            );
            let mut snapshot = Bag::new();
            integrate(&mut snapshot, &super::projected_output(&query, &compiled).await?)?;
            ensure!(snapshot == output, "replay {} snapshot differs at step {index}", case.name);
        }
    }
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    Ok(serde_json::json!({"case":case.name,"prepare_commit_ns":elapsed,
        "object_puts":store.puts.load(Ordering::SeqCst),"stored_objects":objects.len(),
        "stored_bytes":objects.iter().map(|object| object.size).sum::<u64>(),
        "measured_object_reads":object_reads,"measured_object_puts":object_puts,
        "oracle_checks":oracle_checks,"measured_weighted_updates":measured_changes,"measured_cache_misses":cache_misses,"output_groups":output.len(),
        "memtable_bytes":query.memtable_bytes()?,"logical_time":query.time()}))
}
#[tokio::test]
async fn fast_replay() -> Result<()> {
    let text = if let Some(path) = std::env::var_os("PGDERIVE_FAST_REPLAY") {
        std::fs::read_to_string(path)?
    } else {
        include_str!("../../../tests/fixtures/fast-replay.json").into()
    };
    let suite = parse(&text)?;
    let mut reports = Vec::new();
    for case in suite.cases {
        reports.push(run(case).await?);
    }
    if let Some(path) = std::env::var_os("PGDERIVE_FAST_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&reports)?)?;
    }
    Ok(())
}

fn parse(text: &str) -> Result<Suite> {
    let suite: Suite = serde_json::from_str(text)?;
    ensure!(suite.version == 1 && !suite.cases.is_empty(), "invalid fast replay suite");
    let mut names = BTreeSet::new();
    for case in &suite.cases {
        ensure!(
            !case.name.is_empty() && names.insert(&case.name),
            "duplicate or empty replay name"
        );
        ensure!(
            case.steps.first().is_some_and(|step| step.expected.is_some())
                && case.steps.last().is_some_and(|step| step.expected.is_some()),
            "initial/final oracle missing"
        );
        for step in &case.steps {
            ensure!(
                !step.restart
                    || (step.updates.is_empty() && !step.measure && step.expected.is_some()),
                "invalid restart step"
            );
            if let Some(expected) = &step.expected {
                let rows = expected.iter().map(|(row, _)| row).collect::<BTreeSet<_>>();
                ensure!(
                    rows.len() == expected.len() && expected.iter().all(|(_, weight)| *weight > 0),
                    "invalid expected bag"
                );
            }
        }
    }
    Ok(suite)
}
#[tokio::test]
async fn wrong_oracle_is_rejected() -> Result<()> {
    let mut suite = parse(include_str!("../../../tests/fixtures/fast-replay.json"))?;
    let mut case = suite.cases.remove(0);
    case.steps.last_mut().context("fixture missing steps")?.expected = Some(vec![]);
    let error = run(case).await.err().context("wrong oracle unexpectedly passed")?;
    assert!(error.to_string().contains("independent application oracle"));
    Ok(())
}
#[test]
fn missing_coverage_is_rejected() -> Result<()> {
    assert!(parse(r#"{"version":0,"cases":[]}"#).is_err());
    assert!(
        parse(r#"{"version":1,"cases":[{"name":"empty","sql":"SELECT 1","steps":[]}]}"#).is_err()
    );
    let text = include_str!("../../../tests/fixtures/fast-replay.json");
    let mut value: serde_json::Value = serde_json::from_str(text)?;
    value["cases"][0]["steps"][0]["expected"] = serde_json::Value::Null;
    assert!(parse(&value.to_string()).is_err());
    Ok(())
}
