//! Metadata-only reuse preserves immutable cold ranges and exact weighted identity.
use crate::engine::{
    Batch,
    reader::{BatchReader as _, BlockCache, Entry, ObjectBatch, WriteLimits},
    tests::store::{Store, range_totals},
    trace::{Manifest, Run, TraceSnapshot},
};
use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::Ordering},
};

const SCHEMA: &str = "reuse-i64-v1";
fn cache() -> Arc<BlockCache> {
    Arc::new(BlockCache::new(0, 0))
}
fn limits() -> WriteLimits {
    WriteLimits { block_rows: 32, ..WriteLimits::default() }
}
async fn object(
    store: Arc<Store>,
    rows: Vec<Entry<i64, i64>>,
    namespace: &str,
) -> Result<ObjectBatch<i64, i64>> {
    ObjectBatch::write_resident(
        store,
        &Batch::from_updates(rows)?,
        SCHEMA,
        (Some(namespace), 32),
        cache(),
    )
    .await
}
fn snapshot(runs: &[Run<i64, i64>]) -> Result<TraceSnapshot<i64, i64>> {
    let mut trace = TraceSnapshot::empty();
    for (time, run) in runs.iter().enumerate() {
        trace = trace.append_validated(run.clone(), u64::try_from(time)? + 1)?;
    }
    Ok(trace)
}
async fn cold_oracle(
    store: Arc<Store>,
    objects: &[ObjectBatch<i64, i64>],
    expected: &Batch<i64, i64>,
) -> Result<()> {
    let roots = objects.iter().map(|object| object.reference().clone()).collect::<Vec<_>>();
    let trace = TraceSnapshot::reopen(
        store,
        Manifest { version: 2, time: 1, generation: 1, cohorts: vec![roots.len()], objects: roots },
        SCHEMA,
        cache(),
    )
    .await?;
    assert_eq!(&trace.materialize().await?, expected);
    Ok(())
}

#[tokio::test]
async fn broad_root_reuses_cold_addresses_without_extra_puts() -> Result<()> {
    let mut store = Store::unpaced();
    store.record_ranges();
    let store = Arc::new(store);
    let rows = (-4096..4096).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let base = object(store.clone(), rows.clone(), "pgderive/upload-v1/base").await?;
    let update = Batch::from_updates([((0, 0), -1), ((0, 100_000), 1)])?;
    let mut expected = Batch::from_updates(rows)?;
    expected.apply(&update)?;
    let runs = vec![Run::Object(base.clone()), Run::Memory(Arc::new(update))];
    let original = base.test_blocks().await?;
    let mut original_ranges = BTreeSet::new();
    for ordinal in 0..base.block_count() {
        if let Some((path, range)) = base.block_range(ordinal).await? {
            original_ranges.insert((path, range.start, range.end));
        }
    }
    store.take_ranges()?;
    let before = store.puts.load(Ordering::SeqCst);
    let before_bytes = store.put_bytes.load(Ordering::SeqCst);
    let compacted = ObjectBatch::compact_resident(
        store.clone(),
        &runs,
        SCHEMA,
        (Some("pgderive/upload-v1/reused"), limits()),
        cache(),
    )
    .await?;
    let puts = store.puts.load(Ordering::SeqCst) - before;
    let uploaded = store.put_bytes.load(Ordering::SeqCst) - before_bytes;
    let requests = store.take_ranges()?;
    let bytes = range_totals(&requests)?.0;
    let mut reused = 0;
    for output in &compacted {
        for (ordinal, leaf) in output.test_blocks().await?.into_iter().enumerate() {
            if original.iter().any(|input| {
                input.hash == leaf.hash
                    && input.offset == leaf.offset
                    && input.length == leaf.length
            }) {
                let (path, range) = output
                    .block_range(ordinal)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("missing reused range"))?;
                assert!(original_ranges.contains(&(path, range.start, range.end)));
                reused += 1;
            }
        }
    }
    assert!(reused > original.len() / 2, "cold block addresses were rewritten");
    let before = store.puts.load(Ordering::SeqCst);
    let before_bytes = store.put_bytes.load(Ordering::SeqCst);
    let baseline = ObjectBatch::write_stream_resident(
        store.clone(),
        &mut snapshot(&runs)?.cursor().await?,
        SCHEMA,
        (Some("pgderive/upload-v1/rewritten"), limits()),
        cache(),
    )
    .await?;
    let baseline_puts = store.puts.load(Ordering::SeqCst) - before;
    let baseline_uploaded = store.put_bytes.load(Ordering::SeqCst) - before_bytes;
    let baseline_bytes = range_totals(&store.take_ranges()?)?.0;
    assert!(bytes < baseline_bytes / 8, "cold bytes {bytes}/{baseline_bytes}");
    assert!(puts <= baseline_puts, "PUT regression {puts}/{baseline_puts}");
    println!(
        "block-reuse rows=8192 blocks={} reused={reused} read_bytes={bytes}/{baseline_bytes} PUTs={puts}/{baseline_puts} upload_bytes={uploaded}/{baseline_uploaded}",
        original.len()
    );
    cold_oracle(store.clone(), &compacted, &expected).await?;
    cold_oracle(store, &baseline, &expected).await?;
    Ok(())
}

#[tokio::test]
async fn exact_identity_endpoints_duplicates_and_extreme_weights_consolidate() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let a = object(
        store.clone(),
        vec![((1, 0), 1), ((1, 1), i64::MAX), ((1, 2), i64::MIN)],
        "pgderive/upload-v1/a",
    )
    .await?;
    let b =
        object(store.clone(), vec![((1, 1), 1), ((1, 2), 1), ((1, 3), -2)], "pgderive/upload-v1/b")
            .await?;
    let c = object(store.clone(), vec![((1, 1), -i64::MAX), ((1, 3), 1)], "pgderive/upload-v1/c")
        .await?;
    let duplicate =
        object(store.clone(), vec![((2, 0), 3)], "pgderive/upload-v1/duplicate").await?;
    let runs = vec![
        Run::Object(a),
        Run::Object(b),
        Run::Object(c),
        Run::Object(duplicate.clone()),
        Run::Object(duplicate),
    ];
    let expected = Batch::from_updates([
        ((1, 0), 1),
        ((1, 1), 1),
        ((1, 2), i64::MIN + 1),
        ((1, 3), -1),
        ((2, 0), 6),
    ])?;
    let outputs = ObjectBatch::compact_resident(
        store.clone(),
        &runs,
        SCHEMA,
        (Some("pgderive/upload-v1/result"), limits()),
        cache(),
    )
    .await?;
    cold_oracle(store, &outputs, &expected).await?;
    Ok(())
}

#[tokio::test]
async fn cross_namespace_reuse_survives_obsolete_root_and_index_deletion() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let rows = (-512..512).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let base = object(store.clone(), rows.clone(), "pgderive/upload-v1/old").await?;
    let mut expected = Batch::from_updates(rows)?;
    let mut runs = vec![Run::Object(base.clone())];
    let remote: Arc<dyn ObjectStore> = store.clone();
    let old_dependencies = base.reference().dependencies(&remote).await?;
    let mut old_roots = vec![base.reference().clone()];
    for (time, namespace) in [(1, "pgderive/upload-v1/new"), (2, "pgderive/upload-v1/newer")] {
        let delta = Batch::from_updates([((0, 10_000 + time), 1)])?;
        expected.apply(&delta)?;
        runs.push(Run::Memory(Arc::new(delta)));
        let output = ObjectBatch::compact_resident(
            store.clone(),
            &runs,
            SCHEMA,
            (Some(namespace), limits()),
            cache(),
        )
        .await?;
        let mut retained = BTreeSet::new();
        for object in &output {
            retained.extend(object.reference().dependencies(&remote).await?);
        }
        assert!(
            retained.iter().any(|path| path.starts_with("pgderive/upload-v1/old/pgderive/pack/"))
        );
        for root in &old_roots {
            store.delete(&Path::from(root.path())).await?;
        }
        for path in &old_dependencies {
            if !retained.contains(path) {
                store.delete(&Path::from(path.as_str())).await?;
            }
        }
        cold_oracle(store.clone(), &output, &expected).await?;
        old_roots = output.iter().map(|object| object.reference().clone()).collect();
        runs = output.into_iter().map(Run::Object).collect();
    }
    Ok(())
}

#[tokio::test]
async fn failed_compaction_upload_preserves_inputs_and_retry_is_exact() -> Result<()> {
    // One base data PUT and its root precede the compactor's new data/root PUTs.
    for fail_at in [2, 3] {
        let mut store = Store::unpaced();
        store.fail_at = Some(fail_at);
        let store = Arc::new(store);
        let base =
            object(store.clone(), vec![((0, 0), 1), ((1, 1), 1)], "pgderive/upload-v1/source")
                .await?;
        let delta = Batch::from_updates([((0, 0), -1), ((0, 2), 1)])?;
        let runs = vec![Run::Object(base.clone()), Run::Memory(Arc::new(delta))];
        assert!(
            ObjectBatch::compact_resident(
                store.clone(),
                &runs,
                SCHEMA,
                (Some("pgderive/upload-v1/failed"), limits()),
                cache()
            )
            .await
            .is_err()
        );
        let mut cursor = base.cursor().await?;
        assert_eq!(cursor.current(), Some((&0, &0, 1)));
        cursor.advance().await?;
        assert_eq!(cursor.current(), Some((&1, &1, 1)));
        let output = ObjectBatch::compact_resident(
            store.clone(),
            &runs,
            SCHEMA,
            (Some("pgderive/upload-v1/retry"), limits()),
            cache(),
        )
        .await?;
        cold_oracle(store, &output, &Batch::from_updates([((0, 2), 1), ((1, 1), 1)])?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn equal_endpoints_cancel_to_empty_and_overflow_never_publishes() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let positive =
        object(store.clone(), vec![((7, 1), i64::MAX)], "pgderive/upload-v1/positive").await?;
    let negative =
        object(store.clone(), vec![((7, 1), -i64::MAX)], "pgderive/upload-v1/negative").await?;
    let output = ObjectBatch::compact_resident(
        store.clone(),
        &[Run::Object(positive.clone()), Run::Object(negative)],
        SCHEMA,
        (None, limits()),
        cache(),
    )
    .await?;
    let mut actual = Vec::new();
    for object in output {
        let mut cursor = object.cursor().await?;
        while let Some((key, value, weight)) = cursor.current() {
            actual.push(((*key, *value), weight));
            cursor.advance().await?;
        }
    }
    assert_eq!(Batch::from_updates(actual)?, Batch::default());
    let before = store.puts.load(Ordering::SeqCst);
    let overflow =
        [Run::Object(positive), Run::Memory(Arc::new(Batch::from_updates([((7, 1), 1)])?))];
    assert!(
        ObjectBatch::compact_resident(store.clone(), &overflow, SCHEMA, (None, limits()), cache())
            .await
            .is_err()
    );
    assert_eq!(store.puts.load(Ordering::SeqCst), before);
    Ok(())
}

#[tokio::test]
async fn narrower_target_block_limits_reencode_preexisting_pages() -> Result<()> {
    use crate::engine::reader::bounded;
    let store = Arc::new(Store::unpaced());
    let rows = (0..64).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let expected = Batch::from_updates(rows.clone())?;
    let base = object(store.clone(), rows, "pgderive/upload-v1/large-pages").await?;
    let old_path =
        base.block_range(0).await?.ok_or_else(|| anyhow::anyhow!("missing base page"))?.0;
    for target in
        [WriteLimits { block_rows: 1, ..limits() }, WriteLimits { block_bytes: 48, ..limits() }]
    {
        let output = ObjectBatch::compact_resident(
            store.clone(),
            &[Run::Object(base.clone())],
            SCHEMA,
            (None, target),
            cache(),
        )
        .await?;
        for object in &output {
            for ordinal in 0..object.block_count() {
                let rows = object.read(ordinal).await?;
                assert!(rows.len() <= target.block_rows);
                assert!(bounded::encode(rows.as_ref())?.len() <= target.block_bytes);
                let path = object
                    .block_range(ordinal)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("missing result page"))?
                    .0;
                assert_ne!(path, old_path, "oversized source page reused");
            }
        }
        cold_oracle(store.clone(), &output, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn dirty_pages_share_one_pending_pack_across_reused_middle_page() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let rows = (0..96).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let base = object(store.clone(), rows.clone(), "pgderive/upload-v1/interleaved-old").await?;
    let delta =
        Batch::from_updates([((0, 0), -1), ((0, 10_000), 1), ((64, 64), -1), ((64, 20_000), 1)])?;
    let mut expected = Batch::from_updates(rows)?;
    expected.apply(&delta)?;
    let runs = [Run::Object(base.clone()), Run::Memory(Arc::new(delta))];
    let puts = store.puts.load(Ordering::SeqCst);
    let output = ObjectBatch::compact_resident(
        store.clone(),
        &runs,
        SCHEMA,
        (Some("pgderive/upload-v1/interleaved-new"), limits()),
        cache(),
    )
    .await?;
    assert_eq!(output.len(), 1);
    assert_eq!(
        store.puts.load(Ordering::SeqCst) - puts,
        2,
        "dirty pages should share one data PUT plus root"
    );
    let result = &output[0];
    assert_eq!(result.block_count(), 3);
    let first =
        result.block_range(0).await?.ok_or_else(|| anyhow::anyhow!("missing first page"))?.0;
    let middle =
        result.block_range(1).await?.ok_or_else(|| anyhow::anyhow!("missing middle page"))?.0;
    let last = result.block_range(2).await?.ok_or_else(|| anyhow::anyhow!("missing last page"))?.0;
    assert_eq!(first, last);
    assert_ne!(first, middle);
    assert_eq!(Some(middle), base.block_range(1).await?.map(|range| range.0));
    let blocks = result.test_blocks().await?;
    assert_eq!(blocks.iter().map(|block| block.pack).collect::<Vec<_>>(), vec![0, 1, 0]);
    cold_oracle(store, &output, &expected).await?;
    Ok(())
}

#[tokio::test]
async fn bounded_root_splits_reuse_payload_without_extra_data_puts() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let rows = (0..128).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let expected = Batch::from_updates(rows.clone())?;
    let base = object(store.clone(), rows, "pgderive/upload-v1/split-old").await?;
    let puts = store.puts.load(Ordering::SeqCst);
    let output = ObjectBatch::compact_resident(
        store.clone(),
        &[Run::Object(base.clone())],
        SCHEMA,
        (Some("pgderive/upload-v1/split-new"), WriteLimits { index_blocks: 1, ..limits() }),
        cache(),
    )
    .await?;
    assert_eq!(output.len(), base.block_count());
    assert_eq!(
        store.puts.load(Ordering::SeqCst) - puts,
        output.len(),
        "each split needs one root PUT and no payload PUT"
    );
    for (ordinal, object) in output.iter().enumerate() {
        assert_eq!(object.block_count(), 1);
        assert_eq!(object.block_range(0).await?, base.block_range(ordinal).await?);
    }
    cold_oracle(store, &output, &expected).await?;
    Ok(())
}

#[tokio::test]
async fn repeated_hot_replacements_preserve_cold_ranges_without_extra_puts() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let rows = (-256..256).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let base = object(store.clone(), rows.clone(), "pgderive/upload-v1/cycles-base").await?;
    let original_cold = base.block_range(0).await?;
    let mut expected = Batch::from_updates(rows)?;
    let mut reused = vec![base.clone()];
    let mut rewritten = vec![base];
    let mut reuse_puts = 0;
    let mut rewrite_puts = 0;
    let mut value = 0;
    for cycle in 1..=8 {
        let next = 10_000 + cycle;
        let delta = Batch::from_updates([((0, value), -1), ((0, next), 1)])?;
        value = next;
        expected.apply(&delta)?;
        let mut runs = reused.into_iter().map(Run::Object).collect::<Vec<_>>();
        runs.push(Run::Memory(Arc::new(delta.clone())));
        let puts = store.puts.load(Ordering::SeqCst);
        reused = ObjectBatch::compact_resident(
            store.clone(),
            &runs,
            SCHEMA,
            (Some(&format!("pgderive/upload-v1/reuse-cycle-{cycle}")), limits()),
            cache(),
        )
        .await?;
        reuse_puts += store.puts.load(Ordering::SeqCst) - puts;
        let mut runs = rewritten.into_iter().map(Run::Object).collect::<Vec<_>>();
        runs.push(Run::Memory(Arc::new(delta)));
        let puts = store.puts.load(Ordering::SeqCst);
        rewritten = ObjectBatch::write_stream_resident(
            store.clone(),
            &mut snapshot(&runs)?.cursor().await?,
            SCHEMA,
            (Some(&format!("pgderive/upload-v1/rewrite-cycle-{cycle}")), limits()),
            cache(),
        )
        .await?;
        rewrite_puts += store.puts.load(Ordering::SeqCst) - puts;
        assert_eq!(reused[0].block_range(0).await?, original_cold);
        cold_oracle(store.clone(), &reused, &expected).await?;
        cold_oracle(store.clone(), &rewritten, &expected).await?;
    }
    assert!(reuse_puts <= rewrite_puts, "repeated PUT regression {reuse_puts}/{rewrite_puts}");
    let mut physical_packs = BTreeMap::new();
    let mut referenced_block_bytes = 0_u64;
    for object in &reused {
        for pack in object.test_packs().await? {
            physical_packs.insert(pack.path, pack.bytes);
        }
        for block in object.test_blocks().await? {
            referenced_block_bytes = referenced_block_bytes
                .checked_add(block.length)
                .ok_or_else(|| anyhow::anyhow!("referenced bytes overflow"))?;
        }
    }
    let physical_pack_bytes = physical_packs.values().try_fold(0_u64, |bytes, size| {
        bytes.checked_add(*size).ok_or_else(|| anyhow::anyhow!("physical bytes overflow"))
    })?;
    assert!(physical_pack_bytes >= referenced_block_bytes);
    println!(
        "8 complete hot-replacement compactions: PUTs={reuse_puts}/{rewrite_puts}; unique physical data packs={} bytes={physical_pack_bytes}; referenced block bytes={referenced_block_bytes}",
        physical_packs.len()
    );
    Ok(())
}

#[tokio::test]
async fn different_target_store_reencodes_instead_of_reusing_unavailable_packs() -> Result<()> {
    let source = Arc::new(Store::unpaced());
    let destination = Arc::new(Store::unpaced());
    let rows = (0..64).map(|key| ((key, key), 1)).collect::<Vec<_>>();
    let base = object(source, rows.clone(), "pgderive/upload-v1/source").await?;
    let output = ObjectBatch::compact_resident(
        destination.clone(),
        &[Run::Object(base)],
        SCHEMA,
        (Some("pgderive/upload-v1/destination"), limits()),
        cache(),
    )
    .await?;
    cold_oracle(destination, &output, &Batch::from_updates(rows)?).await?;
    Ok(())
}

#[tokio::test]
async fn overlapping_roots_coalesce_payload_reads_without_cross_input_cache_thrashing() -> Result<()>
{
    let mut store = Store::unpaced();
    store.record_ranges();
    let store = Arc::new(store);
    let mut runs = Vec::new();
    let mut updates = Vec::new();
    let mut payload_paths = BTreeSet::new();
    for value in 0..2 {
        // Equal full identities force both inputs through consolidate(), not
        // merely the ordinary disjoint span loading branch.
        let rows = (0..1024).map(|key| ((key, 0), value + 1)).collect::<Vec<_>>();
        updates.extend(rows.iter().copied());
        let object =
            object(store.clone(), rows, &format!("pgderive/upload-v1/overlap-{value}")).await?;
        assert!(object.block_count() >= 32);
        for pack in object.test_packs().await? {
            payload_paths.insert(pack.path);
        }
        runs.push(Run::Object(object));
    }
    assert_eq!(payload_paths.len(), 2);
    store.take_ranges()?;
    let output = ObjectBatch::compact_resident(
        store.clone(),
        &runs,
        SCHEMA,
        (Some("pgderive/upload-v1/overlap-output"), limits()),
        cache(),
    )
    .await?;
    let requests = store.take_ranges()?;
    let payload_gets = requests
        .iter()
        .filter(|((path, _, _), _)| payload_paths.contains(path))
        .try_fold(0_usize, |count, (_, requests)| {
            count
                .checked_add(*requests)
                .ok_or_else(|| anyhow::anyhow!("payload GET count overflow"))
        })?;
    assert!(payload_gets >= 2, "both overlapping packs must be read");
    assert!(
        payload_gets <= 6,
        "expected coalesced per-input reads, got {payload_gets} payload GETs for 64 pages"
    );
    println!("overlapping roots: 2048 rows, 64 source pages, {payload_gets} payload GETs");
    cold_oracle(store, &output, &Batch::from_updates(updates)?).await?;
    Ok(())
}
