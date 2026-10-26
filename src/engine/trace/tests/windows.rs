//! Whole-root key windows retain cold membership without changing weighted identity.
use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    dataflow::Arrangement,
    reader::{Entry, ObjectBatch, ObjectRef},
    tests::store::{Store, range_totals},
};
use anyhow::{Context as _, Result};
use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::Ordering},
};

const SCHEMA: &str = "window-i64-v1";

struct Fixture {
    store: Arc<Store>,
    writer: Arrangement<i64, i64>,
    trace: TraceSnapshot<i64, i64>,
    cold: Vec<ObjectRef>,
    cold_paths: BTreeSet<String>,
}

async fn object(store: &Arc<Store>, rows: Vec<Entry<i64, i64>>) -> Result<Run<i64, i64>> {
    let batch = Batch::from_updates(rows)?;
    let root = ObjectBatch::write(store.clone(), &batch, SCHEMA, 32).await?;
    Ok(Run::Object(ObjectBatch::open(store.clone(), root, SCHEMA).await?))
}

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Layout {
    Partitioned,
    Broad,
}

async fn fixture(
    layout: Layout,
    duplicate_cold: bool,
    hot_rows: i64,
    cold_rows: i64,
) -> Result<Fixture> {
    let mut store = Store::unpaced();
    store.record_ranges();
    let store = Arc::new(store);
    let writer = Arrangement::new(store.clone(), SCHEMA.into(), 32)?;
    let mut runs = Vec::new();
    let mut cold = Vec::new();
    let mut cold_paths = BTreeSet::new();
    let mut shared_left: Option<Run<i64, i64>> = None;
    for value in 0..4 {
        if matches!(layout, Layout::Broad) {
            let rows = (-cold_rows..0)
                .map(|key| ((key, value), 1))
                .chain((value * hot_rows..(value + 1) * hot_rows).map(|item| ((0, item), 1)))
                .chain((1..=cold_rows).map(|key| ((key, value), 1)))
                .collect();
            runs.push(object(&store, rows).await?);
            continue;
        }
        let left = if duplicate_cold {
            if let Some(left) = &shared_left {
                left.clone()
            } else {
                let left =
                    object(&store, (-cold_rows..0).map(|key| ((key, value), 1)).collect()).await?;
                shared_left = Some(left.clone());
                left
            }
        } else {
            object(&store, (-cold_rows..0).map(|key| ((key, value), 1)).collect()).await?
        };
        let hot = object(
            &store,
            (value * hot_rows..(value + 1) * hot_rows).map(|item| ((0, item), 1)).collect(),
        )
        .await?;
        let right = object(&store, (1..=cold_rows).map(|key| ((key, value), 1)).collect()).await?;
        for run in [&left, &right] {
            let Run::Object(batch) = run else { anyhow::bail!("object fixture required") };
            cold.push(batch.reference().clone());
            cold_paths.insert(batch.block_range(0).await?.context("cold block required")?.0);
        }
        runs.extend([left, hot, right]);
    }
    let mut trace = TraceSnapshot::from_runs(runs, 1, 1)?;
    trace.cohorts = vec![if matches!(layout, Layout::Broad) { 1 } else { 3 }; 4];
    Ok(Fixture { store, writer, trace, cold, cold_paths })
}

fn delta(time: u64) -> Result<Batch<i64, i64>> {
    Batch::from_updates([((0, i64::try_from(time)?), 1)])
}

#[tokio::test]
async fn window_flush_preserves_cold_roots_and_full_tuple_values_across_restart() -> Result<()> {
    for duplicate_cold in [false, true] {
        let Fixture { store, writer, mut trace, cold, cold_paths } =
            fixture(Layout::Partitioned, duplicate_cold, 1, 100).await?;
        let mut oracle = trace.materialize().await?;
        store.take_ranges()?;
        let first = delta(2)?;
        oracle.apply(&first)?;
        let pending = trace.append_validated(Run::Memory(Arc::new(first)), 2)?;
        let puts = store.puts.load(Ordering::SeqCst);
        trace = writer.flush(&pending).await?;
        let window_puts = store.puts.load(Ordering::SeqCst) - puts;
        let requests = store.take_ranges()?;
        assert!(requests.keys().all(|(path, _, _)| !cold_paths.contains(path)));
        let (window_bytes, _) = range_totals(&requests)?;
        assert_eq!(trace.materialize().await?, oracle);
        let manifest = trace.manifest()?;
        for root in &cold {
            assert_eq!(
                manifest.objects.iter().filter(|candidate| *candidate == root).count(),
                cold.iter().filter(|candidate| *candidate == root).count()
            );
        }
        // The baseline overlap selector chooses all four original cohorts and
        // the hot memory suffix. Full compaction exercises that same production
        // merge/upload kernel, giving comparable immutable request accounting.
        store.take_ranges()?;
        let puts = store.puts.load(Ordering::SeqCst);
        let baseline = writer.compact(&pending).await?;
        let baseline_puts = store.puts.load(Ordering::SeqCst) - puts;
        let baseline_requests = store.take_ranges()?;
        let (baseline_bytes, _) = range_totals(&baseline_requests)?;
        assert_eq!(baseline.materialize().await?, oracle);
        assert!(window_bytes < baseline_bytes / 8);
        assert!(requests.len() < baseline_requests.len());
        assert!(window_puts <= baseline_puts);
        println!(
            "duplicate_cold={duplicate_cold}: window bytes={window_bytes}, ranges={}, PUTs={window_puts}; baseline bytes={baseline_bytes}, ranges={}, PUTs={baseline_puts}",
            requests.len(),
            baseline_requests.len()
        );
        for time in 3..=18 {
            let batch = delta(time)?;
            oracle.apply(&batch)?;
            store.take_ranges()?;
            trace =
                writer.flush(&trace.append_validated(Run::Memory(Arc::new(batch)), time)?).await?;
            let requests = store.take_ranges()?;
            assert!(requests.keys().all(|(path, _, _)| !cold_paths.contains(path)));
            let manifest = trace.manifest()?;
            for root in &cold {
                assert_eq!(
                    manifest.objects.iter().filter(|candidate| *candidate == root).count(),
                    cold.iter().filter(|candidate| *candidate == root).count()
                );
            }
            assert_eq!(trace.materialize().await?, oracle);
            if time % 4 == 0 {
                trace = writer.reopen(manifest.clone()).await?;
                assert_eq!(trace.manifest()?, manifest);
                assert_eq!(trace.materialize().await?, oracle);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn window_subset_overflow_preserves_excluded_cohort_cancellation() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let writer = Arrangement::new(store.clone(), SCHEMA.into(), 32)?;
    let mut runs = Vec::new();
    // This dominant cohort is excluded by overlap balancing. Its three roots
    // cancel three of the four positive hot coefficients in the chosen window.
    for value in 0..3 {
        let rows = (-1000..0).map(|key| ((key, value), 1)).chain([((0, 0), -i64::MAX)]).collect();
        runs.push(object(&store, rows).await?);
    }
    for value in 0..4 {
        runs.push(object(&store, (-100..0).map(|key| ((key, value), 1)).collect()).await?);
        runs.push(object(&store, vec![((0, 0), i64::MAX)]).await?);
        runs.push(object(&store, (1..101).map(|key| ((key, value), 1)).collect()).await?);
    }
    let mut trace = TraceSnapshot::from_runs(runs, 1, 1)?;
    trace.cohorts = vec![3; 5];
    let batch = Batch::from_updates([((0, 1), 1)])?;
    let pending = trace.append_validated(Run::Memory(Arc::new(batch)), 2)?;
    let expected = pending.materialize().await?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(flushed.cohorts.len(), 1);
    assert_eq!(flushed.materialize().await?, expected);
    assert_eq!(flushed.tuple_weight(&(0, 0)).await?, i64::MAX);
    let restored = writer.reopen(flushed.manifest()?).await?;
    assert_eq!(restored.materialize().await?, expected);
    Ok(())
}

#[tokio::test]
async fn dominant_hot_root_survives_tiny_updates_until_peers_are_comparable() -> Result<()> {
    let Fixture { store, writer, trace, cold, cold_paths } =
        fixture(Layout::Partitioned, false, 100, 100).await?;
    let mut oracle = trace.materialize().await?;
    let first = Batch::from_updates([((0, 10_000), 1)])?;
    oracle.apply(&first)?;
    let pending = trace.append_validated(Run::Memory(Arc::new(first)), 2)?;
    store.take_ranges()?;
    let mut trace = writer.flush(&pending).await?;
    let initial = store.take_ranges()?;
    let (initial_bytes, _) = range_totals(&initial)?;
    let hot = trace
        .manifest()?
        .objects
        .into_iter()
        .find(|root| !cold.contains(root))
        .context("merged hot root required")?;
    assert_eq!(trace.materialize().await?, oracle);
    store.take_ranges()?;
    let mut read_bytes = 0_u64;
    let puts = store.puts.load(Ordering::SeqCst);
    for time in 3..=258 {
        let batch = Batch::from_updates([((0, 10_000 + i64::try_from(time)?), 1)])?;
        oracle.apply(&batch)?;
        trace = writer.flush(&trace.append_validated(Run::Memory(Arc::new(batch)), time)?).await?;
        let requests = store.take_ranges()?;
        assert!(requests.keys().all(|(path, _, _)| !cold_paths.contains(path)));
        read_bytes += range_totals(&requests)?.0;
        let manifest = trace.manifest()?;
        assert!(manifest.objects.contains(&hot), "dominant hot root rewritten at {time}");
        for root in &cold {
            assert_eq!(manifest.objects.iter().filter(|candidate| *candidate == root).count(), 1);
        }
        assert_eq!(trace.materialize().await?, oracle);
        if time % 32 == 0 {
            trace = writer.reopen(manifest.clone()).await?;
            assert_eq!(trace.manifest()?, manifest);
            assert_eq!(trace.materialize().await?, oracle);
        }
        store.take_ranges()?;
    }
    let puts = store.puts.load(Ordering::SeqCst) - puts;
    assert!(
        read_bytes < initial_bytes * 32,
        "hot merge read amplification {read_bytes}/{initial_bytes}"
    );
    assert!(puts <= 2 * 256, "tiny checkpoints added redundant PUTs: {puts}");
    println!(
        "dominant-hot: initialreadbytes={initial_bytes}, 256flush readbytes={read_bytes}, PUTs={puts}"
    );
    Ok(())
}

async fn report_case(cold_rows: i64, changes: u64) -> Result<serde_json::Value> {
    let Fixture { store, writer, mut trace, .. } =
        fixture(Layout::Partitioned, false, 64, cold_rows).await?;
    let mut oracle = trace.materialize().await?;
    let reads = store.reads.load(Ordering::SeqCst);
    let read_bytes = store.range_bytes.load(Ordering::SeqCst);
    let puts = store.puts.load(Ordering::SeqCst);
    let put_bytes = store.put_bytes.load(Ordering::SeqCst);
    let mut elapsed = Vec::new();
    let mut max_runs = trace.run_count();
    let mut values: Vec<i64> = (0..256).collect();
    let mut updates = Vec::new();
    for time in 2..changes + 2 {
        let slot = usize::try_from(time % 256)?;
        let replacement = 100_000 + i64::try_from(time)?;
        let change = [((0, values[slot]), -1), ((0, replacement), 1)];
        values[slot] = replacement;
        updates.extend(change);
        let batch = Batch::from_updates(change)?;
        let pending = trace.append_validated(Run::Memory(Arc::new(batch)), time)?;
        let start = std::time::Instant::now();
        trace = writer.flush(&pending).await?;
        elapsed.push(start.elapsed().as_nanos());
        store.take_ranges()?;
        max_runs = max_runs.max(trace.run_count());
    }
    let reads = store.reads.load(Ordering::SeqCst) - reads;
    let read_bytes = store.range_bytes.load(Ordering::SeqCst) - read_bytes;
    let puts = store.puts.load(Ordering::SeqCst) - puts;
    let put_bytes = store.put_bytes.load(Ordering::SeqCst) - put_bytes;
    let merges = writer.cache().stats()?.merges;
    oracle.apply(&Batch::from_updates(updates)?)?;
    assert_eq!(trace.materialize().await?, oracle);
    let manifest = trace.manifest()?;
    let restored = writer.reopen(manifest.clone()).await?;
    assert_eq!(restored.manifest()?, manifest);
    assert_eq!(restored.materialize().await?, oracle);
    Ok(serde_json::json!({
        "cold_rows_per_root":cold_rows,"initial_hot_rows":256,"changes":changes,
        "source_row_changes":changes * 2,"flush_ns":elapsed,
        "reads":reads,"read_bytes":read_bytes,"puts":puts,"put_bytes":put_bytes,
        "max_runs":max_runs,"final_runs":trace.run_count(),"merge_sizes":merges,
        "cold_oracle":true
    }))
}

#[tokio::test]
#[ignore = "manual production-path key-window request and latency benchmark"]
async fn report_window_compaction_costs() -> Result<()> {
    let mut reports = Vec::new();
    for cold_rows in [100, 1000, 10_000] {
        for changes in [256, 512] {
            reports.push(report_case(cold_rows, changes).await?);
        }
    }
    let output = std::env::var("PGDERIVE_WINDOW_REPORT")?;
    std::fs::write(output, serde_json::to_vec_pretty(&reports)?)?;
    Ok(())
}

async fn cold_root_case(layout: Layout, cold_rows: i64) -> Result<serde_json::Value> {
    let Fixture { store, writer, trace, .. } = fixture(layout, false, 64, cold_rows).await?;
    let mut oracle = trace.materialize().await?;
    let before = trace.manifest()?;
    let batch = Batch::from_updates([((0, 0), -1), ((0, 100_000), 1)])?;
    oracle.apply(&batch)?;
    let pending = trace.append_validated(Run::Memory(Arc::new(batch)), 2)?;
    store.take_ranges()?;
    let reads = store.reads.load(Ordering::SeqCst);
    let read_bytes = store.range_bytes.load(Ordering::SeqCst);
    let puts = store.puts.load(Ordering::SeqCst);
    let put_bytes = store.put_bytes.load(Ordering::SeqCst);
    let started = std::time::Instant::now();
    let flushed = writer.flush(&pending).await?;
    let flush_ns = started.elapsed().as_nanos();
    let reads = store.reads.load(Ordering::SeqCst) - reads;
    let read_bytes = store.range_bytes.load(Ordering::SeqCst) - read_bytes;
    let puts = store.puts.load(Ordering::SeqCst) - puts;
    let put_bytes = store.put_bytes.load(Ordering::SeqCst) - put_bytes;
    let after = flushed.manifest()?;
    let selected_rows = trace.runs.iter().try_fold(0_usize, |rows, run| {
        let Run::Object(root) = run else { anyhow::bail!("durable preload required") };
        if after.objects.contains(root.reference()) {
            Ok(rows)
        } else {
            rows.checked_add(root.row_count()).context("selected row count overflow")
        }
    })?;
    assert_eq!(flushed.materialize().await?, oracle);
    let cold_writer = Arrangement::<i64, i64>::new(store.clone(), SCHEMA.into(), 32)?;
    let restored = cold_writer.reopen(after.clone()).await?;
    assert_eq!(restored.manifest()?, after);
    assert_eq!(restored.materialize().await?, oracle);
    let merges = writer.cache().stats()?.merges;
    assert_eq!(merges.iter().sum::<u64>(), 1);
    Ok(serde_json::json!({
        "layout":layout,"cold_rows_per_side_per_cohort":cold_rows,
        "initial_hot_rows":256,"source_row_changes":2,
        "initial_rows":256 + 8 * cold_rows,"selected_durable_rows":selected_rows,
        "selected_cold_rows":selected_rows.saturating_sub(256),
        "merge_sizes":merges,"natural_merges":1,"flush_ns":flush_ns,
        "reads":reads,"read_bytes":read_bytes,"puts":puts,"put_bytes":put_bytes,
        "before_objects":before.objects,"after_objects":after.objects,
        "before_cohorts":before.cohorts,"after_cohorts":after.cohorts,
        "maximum_runs":pending.run_count(),"final_runs":flushed.run_count(),
        "cold_oracle":true,"external_reloads":0,"pending_upload":false,
        "scope":"One naturally selected installed merge per independent preload; not continuous cycles. Preload, oracle and cold reopen excluded from request and latency measurements."
    }))
}

#[tokio::test]
#[ignore = "manual production-path cold-root scaling acceptance report"]
async fn report_cold_root_scaling() -> Result<()> {
    let mut reports = Vec::new();
    for cold_rows in [100, 1000, 10_000] {
        for layout in [Layout::Partitioned, Layout::Broad] {
            reports.push(cold_root_case(layout, cold_rows).await?);
        }
    }
    std::fs::write(
        std::env::var("PGDERIVE_COLD_ROOT_REPORT")?,
        serde_json::to_vec_pretty(&reports)?,
    )?;
    Ok(())
}
