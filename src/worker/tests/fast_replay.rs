//! Reuse application trace/oracle exports; only the compiled engine is timed.
use super::{program, settings};
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

mod lifecycle;

type Cells = Vec<Option<String>>;
type Bag = BTreeMap<Cells, i64>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Suite {
    version: u32,
    lifecycle: lifecycle::Config,
    limits: crate::engine::execution::Limits,
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
    source_rows: u64,
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
async fn bootstrap(
    query: &program::relational::Query,
    changes: &weighted::Batch,
    ir: &crate::compiler::relational::Relational,
    limits: crate::engine::execution::Limits,
) -> Result<crate::engine::plan::relational::Prepared<program::relational::Key, Row>> {
    let mut workspace = query.bootstrap_workspace(64 * 1024 * 1024)?;
    for updates in changes.updates.chunks(limits.resident_entries.min(4096)) {
        workspace
            .append(program::relational::chunk(&weighted::Batch { updates: updates.to_vec() }, ir)?)
            .await?;
    }
    query.prepare_bootstrap_unprotected(workspace).await
}
fn replay_settings(
    limits: crate::engine::execution::Limits,
) -> Result<crate::engine::plan::Settings> {
    let mut settings = settings();
    settings.block_rows = 4096;
    settings.limits = limits.validate()?;
    Ok(settings)
}
async fn verify_snapshot(
    query: &program::relational::Query,
    compiled: &crate::compiler::Compiled,
    expected: &Bag,
) -> Result<()> {
    let mut snapshot = Bag::new();
    integrate(&mut snapshot, &super::projected_output(query, compiled).await?)?;
    ensure!(&snapshot == expected, "replay snapshot differs from committed output");
    Ok(())
}
async fn restart_committed(
    query: &mut program::relational::Query,
    native: &Contract,
    compiled: &crate::compiler::Compiled,
    settings: &crate::engine::plan::Settings,
) -> Result<()> {
    let replacement = program::relational::build_committed(native, compiled, settings)?;
    super::restart_with(query, replacement).await
}
async fn verify_cold(
    query: &program::relational::Query,
    native: &Contract,
    compiled: &crate::compiler::Compiled,
    settings: &crate::engine::plan::Settings,
    expected: &Bag,
) -> Result<()> {
    let mut cold = program::relational::build_committed(native, compiled, settings)?;
    cold.restore_checkpoint(query.checkpoint()?).await?;
    verify_snapshot(&cold, compiled, expected).await
}
async fn prepare_tick(
    query: &mut program::relational::Query,
    changes: &weighted::Batch,
    ir: &crate::compiler::relational::Relational,
    limits: crate::engine::execution::Limits,
    lifecycle: &mut lifecycle::Lifecycle,
) -> Result<crate::engine::plan::relational::Prepared<program::relational::Key, Row>> {
    if query.time() == 0 {
        bootstrap(query, changes, ir, limits).await
    } else {
        let input = program::relational::inputs(changes, ir, query.time() + 1)?;
        lifecycle.prepare(query, input).await
    }
}
fn check_expected(
    output: &Bag,
    expected: Option<Vec<(Cells, i64)>>,
    case: &str,
    index: usize,
) -> Result<usize> {
    let Some(expected) = expected else { return Ok(0) };
    ensure!(
        output == &expected.into_iter().collect::<Bag>(),
        "replay {case} step {index} differs from independent application oracle"
    );
    Ok(1)
}
fn configure_diagnostics(query: &program::relational::Query) -> Result<()> {
    if std::env::var_os("PGDERIVE_TEST_PAGE_PROBES").is_some() {
        query.test_cache().enable_page_probes();
    }
    if let Ok(width) = std::env::var("PGDERIVE_TEST_RANGE_WIDTH") {
        query.test_cache().fix_range_width(width.parse()?)?;
    }
    Ok(())
}
async fn run(
    case: Case,
    limits: crate::engine::execution::Limits,
    config: lifecycle::Config,
) -> Result<serde_json::Value> {
    let native = native();
    let compiled = crate::compiler::compile(&case.sql, &native)?;
    let mut settings = replay_settings(limits)?;
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    settings.store = store.clone();
    let mut query = program::relational::build_committed(&native, &compiled, &settings)?;
    configure_diagnostics(&query)?;
    let mut output = Bag::new();
    let mut lifecycle = lifecycle::Lifecycle::new(config, store.clone());
    let mut elapsed = Vec::new();
    let mut measured_changes = 0;
    let mut cache_misses = 0;
    let mut decoded_bytes = 0;
    let mut decoded_rows = 0;
    let mut object_reads = 0;
    let mut object_puts = 0;
    let mut oracle_checks = 0;
    let mut bootstrap_metrics = (0, 0);
    for (index, step) in case.steps.into_iter().enumerate() {
        if step.restart {
            lifecycle.report.restarts += 1;
            lifecycle.flush(&mut query).await?;
            restart_committed(&mut query, &native, &compiled, &settings).await?;
            configure_diagnostics(&query)?;
        } else {
            let count = step.updates.len();
            let changes = changes(step.updates);
            let before = query.cache_stats()?;
            let reads = store.reads.load(Ordering::SeqCst);
            let puts = store.puts.load(Ordering::SeqCst);
            let cpu_started = lifecycle::process_cpu_ticks()?;
            let started = Instant::now();
            let work =
                prepare_tick(&mut query, &changes, compiled.relational(), limits, &mut lifecycle)
                    .await?;
            let delta = query.commit(work)?;
            let prepare_ns = started.elapsed().as_nanos();
            if index == 0 {
                bootstrap_metrics = (started.elapsed().as_nanos(), count);
            }
            if step.measure {
                elapsed.push(started.elapsed().as_nanos());
                measured_changes += count;
                let after = query.cache_stats()?;
                cache_misses += after.misses - before.misses;
                decoded_bytes += after.decoded_bytes - before.decoded_bytes;
                decoded_rows += after.decoded_rows - before.decoded_rows;
                object_reads += store.reads.load(Ordering::SeqCst) - reads;
                object_puts += store.puts.load(Ordering::SeqCst) - puts;
            }
            integrate(&mut output, &super::projected_batch(&delta.batch, &compiled)?)?;
            if index > 0 {
                let prior_ms = lifecycle.report.logical_ms;
                lifecycle.committed(&mut query, step.source_rows).await?;
                lifecycle.advance(prior_ms).await;
                lifecycle.observe(&query, prepare_ns, cpu_started)?;
            }
        }
        oracle_checks += check_expected(&output, step.expected, &case.name, index)?;
    }
    lifecycle.flush(&mut query).await?;
    verify_cold(&query, &native, &compiled, &settings, &output).await?;
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    Ok(
        serde_json::json!({"case":case.name,"cold_reopen_equal":true,"lifecycle":lifecycle.report,"bootstrap_ns":bootstrap_metrics.0,"bootstrap_rows":bootstrap_metrics.1,"prepare_commit_ns":elapsed,
        "object_puts":store.puts.load(Ordering::SeqCst),"stored_objects":objects.len(),
        "stored_bytes":objects.iter().map(|object| object.size).sum::<u64>(),
        "measured_decoded_bytes":decoded_bytes,"measured_decoded_rows":decoded_rows,"measured_object_reads":object_reads,"measured_object_puts":object_puts,
        "oracle_checks":oracle_checks,"measured_weighted_updates":measured_changes,"measured_cache_misses":cache_misses,"output_groups":output.len(),
        "memtable_bytes":query.memtable_bytes()?,"logical_time":query.time()}),
    )
}
#[tokio::test(start_paused = true)]
async fn fast_replay() -> Result<()> {
    let text = if let Some(path) = std::env::var_os("PGDERIVE_FAST_REPLAY") {
        std::fs::read_to_string(path)?
    } else {
        include_str!("../../../tests/fixtures/fast-replay.json").into()
    };
    let suite = parse(&text)?;
    let mut reports = Vec::new();
    for case in suite.cases {
        reports.push(run(case, suite.limits, suite.lifecycle).await?);
    }
    if let Some(path) = std::env::var_os("PGDERIVE_FAST_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&reports)?)?;
    }
    Ok(())
}

fn parse(text: &str) -> Result<Suite> {
    let suite: Suite = serde_json::from_str(text)?;
    ensure!(suite.version == 2 && !suite.cases.is_empty(), "invalid fast replay suite");
    suite.lifecycle.validate()?;
    let mut names = BTreeSet::new();
    for case in &suite.cases {
        ensure!(
            !case.name.is_empty() && names.insert(&case.name),
            "duplicate or empty replay name"
        );
        ensure!(
            case.steps.first().is_some_and(|step| step.expected.is_some()
                && !step.restart
                && !step.measure
                && step.updates.iter().all(|update| update.weight == 1))
                && case.steps.last().is_some_and(|step| step.expected.is_some()),
            "initial/final oracle missing"
        );
        for step in &case.steps {
            ensure!(
                !step.restart
                    || (step.updates.is_empty()
                        && step.source_rows == 0
                        && !step.measure
                        && step.expected.is_some()),
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
#[tokio::test(start_paused = true)]
async fn wrong_oracle_is_rejected() -> Result<()> {
    let mut suite = parse(include_str!("../../../tests/fixtures/fast-replay.json"))?;
    let mut case = suite.cases.remove(0);
    case.steps.last_mut().context("fixture missing steps")?.expected = Some(vec![]);
    let error = run(case, suite.limits, suite.lifecycle)
        .await
        .err()
        .context("wrong oracle unexpectedly passed")?;
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

#[tokio::test(start_paused = true)]
async fn upload_rebases_newer_join_transactions_without_compacting_restart() -> Result<()> {
    let suite = parse(include_str!("../../../tests/fixtures/fast-replay.json"))?;
    let case = suite
        .cases
        .into_iter()
        .find(|case| case.name.starts_with("join-"))
        .context("missing join fixture")?;
    let report =
        run(case, suite.limits, lifecycle::Config { checkpoint_interval_ms: 1, ..suite.lifecycle })
            .await?;
    assert!(report["lifecycle"]["rebased_uploads"].as_u64().context("missing rebases")? > 0);
    assert!(report["cold_reopen_equal"].as_bool().context("missing cold check")?);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn capacity_flushes_use_bounded_ingestion_and_preserve_exact_results() -> Result<()> {
    let update = |id: i32, amount: i32, weight| Update {
        table: "entry".into(),
        row: [
            ("id".into(), Some(id.to_string())),
            ("account_id".into(), Some(id.to_string())),
            ("amount".into(), Some(amount.to_string())),
        ]
        .into_iter()
        .collect(),
        weight,
    };
    let expected = |changed: i32| {
        (1..=64)
            .map(|key| {
                (
                    vec![
                        Some(key.to_string()),
                        Some("1".into()),
                        Some(i32::from(key <= changed).to_string()),
                    ],
                    1,
                )
            })
            .collect()
    };
    let mut steps = vec![Step {
        source_rows: 0,
        updates: (1..=64).map(|key| update(key, 0, 1)).collect(),
        expected: Some(expected(0)),
        measure: false,
        restart: false,
    }];
    for key in 1..=32 {
        steps.push(Step {
            source_rows: 1,
            updates: vec![update(key, 0, -1), update(key, 1, 1)],
            expected: (key == 32).then(|| expected(32)),
            measure: true,
            restart: false,
        });
    }
    let suite = parse(include_str!("../../../tests/fixtures/fast-replay.json"))?;
    let report = run(
        Case {
            name: "capacity".into(),
            sql: "SELECT account_id,COUNT(*),SUM(amount) FROM source.entry GROUP BY account_id"
                .into(),
            steps,
        },
        suite.limits,
        lifecycle::Config { memtable_bytes: 4096, ..suite.lifecycle },
    )
    .await?;
    assert!(
        report["lifecycle"]["capacity_flushes"].as_u64().context("missing capacity count")? > 0
    );
    Ok(())
}
