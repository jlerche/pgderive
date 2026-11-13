//! Existing immutable writer, streaming merge, memory oracle and cold restore.
use crate::engine::{
    Batch, BatchBuilder,
    dataflow::Arrangement,
    execution::{Workspaces, service},
    reader::ObjectBatch,
    tests::store::Store,
    trace::{Run, TraceSnapshot},
};
use anyhow::Result;
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
const SCHEMA: &str = "physical-budget-string-v1";
struct Fixture {
    store: Arc<Store>,
    writer: Arrangement<i64, String>,
    trace: TraceSnapshot<i64, String>,
    oracle: Batch<i64, String>,
}
fn value(row: usize, cohort: usize, width: usize) -> Result<String> {
    let mut seed = u64::try_from(row)?
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(u64::try_from(cohort)? + 1);
    let mut result = format!("{row:08}:{cohort:03}:");
    for _ in 0..width {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        result.push(char::from(b'a' + u8::try_from((seed >> 32) % 26)?));
    }
    Ok(result)
}
async fn fixture(roots: usize, rows: usize, width: usize) -> Result<Fixture> {
    let store = Arc::new(Store::unpaced());
    let writer = Arrangement::new(store.clone(), SCHEMA.into(), 4096)?;
    let mut runs = Vec::new();
    let mut oracle = BatchBuilder::default();
    for cohort in 0..roots {
        let batch = Batch::from_updates(
            (0..rows)
                .map(|row| {
                    Ok(((0, value(row, cohort, if cohort < roots / 2 { width } else { 0 })?), 1))
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        oracle.extend(&batch);
        let reference = ObjectBatch::write(store.clone(), &batch, SCHEMA, 4096).await?;
        runs.push(Run::Object(ObjectBatch::open(store.clone(), reference, SCHEMA).await?));
    }
    let mut trace = TraceSnapshot::from_runs(runs, 1, 1)?;
    trace.cohorts = vec![1; roots];
    Ok(Fixture { store, writer, trace, oracle: oracle.finish()? })
}
#[derive(serde::Serialize)]
struct Work {
    elapsed_ns: u128,
    reads: usize,
    read_bytes: u64,
    puts: usize,
    put_bytes: usize,
    runs: usize,
    service: service::Report,
}
fn counters(store: &Store) -> (usize, u64, usize, usize) {
    (
        store.reads.load(Ordering::SeqCst),
        store.range_bytes.load(Ordering::SeqCst),
        store.puts.load(Ordering::SeqCst),
        store.put_bytes.load(Ordering::SeqCst),
    )
}
async fn execute(f: &mut Fixture, full: bool, time: u64) -> Result<Work> {
    let delta = Batch::from_updates([((0, format!("{time:08}:999:hot")), 1)])?;
    f.oracle.apply(&delta)?;
    let pending = f.trace.append_validated(Run::Memory(Arc::new(delta)), time)?;
    let before = counters(&f.store);
    let started = Instant::now();
    let (result, service) = service::scope(Arc::new(Workspaces::new(2)?), async {
        if full { f.writer.compact(&pending).await } else { f.writer.flush(&pending).await }
    })
    .await;
    f.trace = result?;
    let after = counters(&f.store);
    let work = Work {
        elapsed_ns: started.elapsed().as_nanos(),
        reads: after.0 - before.0,
        read_bytes: after.1 - before.1,
        puts: after.2 - before.2,
        put_bytes: after.3 - before.3,
        runs: f.trace.run_count(),
        service,
    };
    if time == 2 || time.is_multiple_of(16) {
        assert_eq!(f.trace.materialize().await?, f.oracle);
    }
    Ok(work)
}
async fn run_case(
    roots: usize,
    rows: usize,
    width: usize,
    full: bool,
    cycles: u64,
) -> Result<serde_json::Value> {
    let mut f = fixture(roots, rows, width).await?;
    let before = f.trace.manifest()?;
    let first = execute(&mut f, full, 2).await?;
    let mut jobs = Vec::new();
    for time in 3..cycles + 3 {
        jobs.push(execute(&mut f, false, time).await?);
        assert!(f.trace.run_count() < 96);
        if time % 8 == 0 {
            f.trace = f.writer.reopen(f.trace.manifest()?).await?;
        }
    }
    let after = f.trace.manifest()?;
    let cold = Arrangement::<i64, String>::new(f.store.clone(), SCHEMA.into(), 4096)?
        .reopen(after.clone())
        .await?;
    assert_eq!(cold.materialize().await?, f.oracle);
    let preserved =
        after.objects.iter().filter(|new| before.objects.iter().any(|old| old == *new)).count();
    Ok(
        json!({"roots":roots,"rows_per_root":rows,"wide_bytes":width,"full_control":full,"first":first,"cycles":jobs,"final_runs":f.trace.run_count(),"retained_original_roots":preserved,"cold_equal":true,"total_puts":first.puts+jobs.iter().map(|job|job.puts).sum::<usize>(),"total_read_bytes":first.read_bytes+jobs.iter().map(|job|job.read_bytes).sum::<u64>()}),
    )
}
#[tokio::test]
async fn bounded_fan_in_survives_repeated_flushes_and_cold_restore() -> Result<()> {
    let candidate = run_case(40, 32, 0, false, 64).await?;
    assert!(candidate["first"]["service"]["selected_runs"].as_u64().is_some_and(|n| n <= 16));
    assert!(candidate["first"]["runs"].as_u64().is_some_and(|n| n > 1));
    assert!(candidate["final_runs"].as_u64().is_some_and(|n| n < 32));
    Ok(())
}
#[tokio::test]
#[ignore = "manual paired physical byte/fan-in selection and complete-cycle I/O report"]
async fn report_physical_selection() -> Result<()> {
    let mut report = Vec::new();
    for (roots, rows, width) in [(40, 256, 0), (24, 8192, 1024)] {
        for full in [true, false] {
            report.push(run_case(roots, rows, width, full, 64).await?);
        }
    }
    std::fs::write(
        std::env::var("PGDERIVE_SELECTION_REPORT")?,
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}

#[tokio::test]
async fn root_pressure_escalation_is_counted_and_reduces_retained_fan_in() -> Result<()> {
    let report = run_case(96, 4, 0, false, 4).await?;
    assert_eq!(report["first"]["service"]["oversized_selections"], 1);
    assert_eq!(report["first"]["service"]["pressure_selections"], 1);
    assert_eq!(report["first"]["service"]["selected_runs"], 97);
    assert_eq!(report["first"]["runs"], 1);
    Ok(())
}
