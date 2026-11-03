//! Checkpoint request accounting uses the production relational upload path.
use super::tests::{Query, settings, sibling_operators, sibling_plan, tick};
use super::*;
use crate::engine::{reader::ObjectBatch, tests::store::Store};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};

type Dependencies = BTreeMap<String, Vec<String>>;
async fn dependencies(
    checkpoint: &crate::engine::plan::Checkpoint,
    store: &Arc<dyn object_store::ObjectStore>,
) -> Result<Dependencies> {
    let mut result = BTreeMap::new();
    for member in &checkpoint.arrangements {
        let mut paths = Vec::new();
        for root in &member.trace.objects {
            paths.extend(root.dependencies(store).await?);
        }
        result.insert(member.id.clone(), paths);
    }
    Ok(result)
}

async fn flush_individually(query: &Query, namespace: &str) -> Result<()> {
    for (id, entry) in &query.snapshot().entries {
        query
            .execution
            .writers
            .get(id)
            .context("missing writer")?
            .clone()
            .with_namespace(namespace)
            .flush(&entry.trace)
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn two_query_workers_pack_independent_arrangements_and_recover() -> Result<()> {
    let mut baseline_puts = 0;
    let mut packed_puts = 0;
    let mut compaction_puts = 0;
    for worker in ["totals", "top10"] {
        let store = Arc::new(Store::unpaced());
        let config = crate::engine::plan::Settings { store: store.clone(), ..settings() };
        let mut query = Query::new(sibling_plan()?, sibling_operators()?, &config)?;
        tick(&mut query, 1, 2).await?;
        // Reproduce the old arrangement-local production flush, using precisely
        // the same traces, codec and writers rather than an arithmetic baseline.
        flush_individually(&query, &format!("pgderive/upload-v1/{worker}-baseline")).await?;
        let baseline = store.puts.load(Ordering::SeqCst);
        baseline_puts += baseline;
        let uploaded = query
            .capture_flush(format!("pgderive/upload-v1/{worker}-packed"))
            .await?
            .upload()
            .await?;
        let packed = store.puts.load(Ordering::SeqCst) - baseline;
        packed_puts += packed;
        assert!(packed < baseline, "packing must reduce real PUT attempts: {packed} >= {baseline}");
        let checkpoint = uploaded.checkpoint().clone();
        let object_store: Arc<dyn object_store::ObjectStore> = store.clone();
        let paths = dependencies(&checkpoint, &object_store).await?;
        let mut counts = BTreeMap::new();
        for path in paths.values().flatten() {
            *counts.entry(path).or_insert(0) += 1;
        }
        assert!(counts.values().any(|count| *count > 1));
        check_shared_ranges(&checkpoint, &object_store).await?;
        let mut restored =
            Query::reopen(sibling_plan()?, sibling_operators()?, config, checkpoint).await?;
        for id in paths.keys() {
            assert_eq!(
                query.snapshot().trace(id)?.materialize().await?,
                restored.snapshot().trace(id)?.materialize().await?
            );
        }
        for time in 2..=4 {
            tick(&mut query, time, 1).await?;
            tick(&mut restored, time, 1).await?;
        }
        for id in paths.keys() {
            assert_eq!(
                query.snapshot().trace(id)?.materialize().await?,
                restored.snapshot().trace(id)?.materialize().await?
            );
        }
        let before_compaction = store.puts.load(Ordering::SeqCst);
        let compacted = restored.prepare_compaction().await?;
        compaction_puts += store.puts.load(Ordering::SeqCst) - before_compaction;
        restored.commit_compaction(compacted)?;
        let checkpoint = restored.checkpoint()?;
        let cold = Query::reopen(
            sibling_plan()?,
            sibling_operators()?,
            crate::engine::plan::Settings { store: object_store, ..settings() },
            checkpoint,
        )
        .await?;
        assert_eq!(
            query.snapshot().trace("f")?.materialize().await?,
            cold.snapshot().trace("f")?.materialize().await?
        );
    }
    println!(
        "{}",
        serde_json::json!({"workers":2,"baseline_puts":baseline_puts,"packed_puts":packed_puts,"compaction_puts":compaction_puts})
    );
    assert_eq!(baseline_puts, 32);
    assert!(compaction_puts > 0);
    assert!(packed_puts <= 24, "two workers must remove at least a quarter of PUTs: {packed_puts}");
    Ok(())
}

async fn check_shared_ranges(
    checkpoint: &crate::engine::plan::Checkpoint,
    object_store: &Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    // Every root retains its own typed identity and exact byte range even
    // when the payload object is shared with a different arrangement.
    let mut addresses = BTreeMap::<String, Vec<std::ops::Range<u64>>>::new();
    for member in &checkpoint.arrangements {
        for root in &member.trace.objects {
            let object = ObjectBatch::<i64, Vec<i64>>::open(
                object_store.clone(),
                root.clone(),
                &member.schema,
            )
            .await?;
            let (path, range) = object.block_range(0).await?.context("missing packed block")?;
            addresses.entry(path).or_default().push(range);
        }
    }
    let shared =
        addresses.values().find(|ranges| ranges.len() > 1).context("missing shared ranges")?;
    assert!(shared.iter().any(|range| range.start > 0));
    let mut ordered = shared.clone();
    ordered.sort_by_key(|range| range.start);
    assert!(ordered.windows(2).all(|pair| pair[0].end <= pair[1].start));
    Ok(())
}
