use super::{Bloom, Filters};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, Statistics, TimedBatch},
    execution::Limits,
    reader::{BlockCache, ObjectBatch},
    tests::store::Store,
};
use anyhow::{Context as _, Result};
use num_bigint::BigInt;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

fn value(key: i64, count: i64) -> String {
    format!("{count}:{}", format!("{key:016x}").repeat(16))
}
fn operator() -> Statistics<i64, i64, String> {
    Statistics::with_measure(
        1,
        |_, v: &i64| Ok(vec![BigInt::from(*v)]),
        |v: &String| {
            Ok(vec![BigInt::from(v.split(':').next().context("missing count")?.parse::<i64>()?)])
        },
        |k, v| Ok(Some(value(*k, i64::try_from(v[0].clone())?))),
    )
}
async fn sparse(rows: i64, filtered: bool) -> Result<Vec<serde_json::Value>> {
    let store = Arc::new(Store::unpaced());
    let mut cache = BlockCache::new(16 * 1024 * 1024, 64);
    if !filtered {
        cache.filters = Arc::new(Filters::new(0, 0));
    }
    let cache = Arc::new(cache);
    let writer =
        Arrangement::new(store.clone(), "wide-sparse".into(), 4096)?.with_cache(cache.clone());
    let base = Batch::from_updates((0..rows).map(|key| ((key, value(key, 0)), 1)))?;
    let mut trace = writer.empty().append_runs_validated(writer.upload_flush(&base).await?, 1)?;
    let mut values = vec![0_i64; usize::try_from(rows)?];
    for time in 1..=16_i64 {
        let mut changes = Vec::new();
        for n in 0..1024_i64 {
            let key = (n * 8191 + time * 127) % rows;
            let count = &mut values[usize::try_from(key)?];
            changes.extend([((key, value(key, *count)), -1), ((key, value(key, *count + 1)), 1)]);
            *count += 1;
        }
        trace = trace.append_runs_validated(
            writer.upload_flush(&Batch::from_updates(changes)?).await?,
            u64::try_from(time + 1)?,
        )?;
    }
    let keys = (0..256_i64).map(|n| (n * 9973 + 13) % rows).collect::<Vec<_>>();
    let input = TimedBatch {
        time: trace.time() + 1,
        batch: Batch::from_updates(keys.iter().map(|k| ((*k, 1_i64), 1)))?,
    };
    let expected = Batch::from_updates(
        keys.iter()
            .map(|k| {
                let v = values[usize::try_from(*k)?];
                Ok([((*k, value(*k, v)), -1), ((*k, value(*k, v + 1)), 1)])
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten(),
    )?;
    let operator = operator();
    let mut report = Vec::new();
    for iteration in 0..4 {
        let before = cache.stats()?;
        let reads = store.reads.load(Ordering::SeqCst);
        let bytes = store.range_bytes.load(Ordering::SeqCst);
        let puts = store.puts.load(Ordering::SeqCst);
        let started = Instant::now();
        let output = operator.evaluate(&input, &trace, Limits::default()).await?;
        let ns = started.elapsed().as_nanos();
        assert_eq!(output.batch, expected);
        assert_eq!(store.puts.load(Ordering::SeqCst), puts);
        let after = cache.stats()?;
        report.push(serde_json::json!({"rows":rows,"filtered":filtered,"iteration":iteration,"ns":ns,
            "decoded_bytes":after.decoded_bytes-before.decoded_bytes,"decoded_rows":after.decoded_rows-before.decoded_rows,
            "reads":store.reads.load(Ordering::SeqCst)-reads,"read_bytes":store.range_bytes.load(Ordering::SeqCst)-bytes,
            "filter_rejections":after.filter_rejections-before.filter_rejections,"filter_bytes":after.filter_bytes}));
    }
    Ok(report)
}
#[tokio::test]
#[ignore = "explicit paired sparse point-probe growth measurement"]
async fn sparse_statistics_growth() -> Result<()> {
    for rows in [2000, 32_000, 128_000, 262_144] {
        for filtered in [false, true] {
            for report in sparse(rows, filtered).await? {
                println!("{report}");
            }
        }
    }
    Ok(())
}
#[test]
fn bloom_has_no_false_negatives_with_collisions_and_duplicate_keys() -> Result<()> {
    let rows = (0..1000_i64).map(|k| ((k, k), 1)).collect::<Vec<_>>();
    let mut bloom = Bloom::new(&rows)?;
    for key in 0..1000_i64 {
        assert!(bloom.contains(&key)?);
    }
    assert!((1000_i64..2000).filter(|k| bloom.contains(k).unwrap_or(true)).count() < 100);
    bloom.bits.fill(u64::MAX);
    for key in 0..2000_i64 {
        assert!(bloom.contains(&key)?);
    }
    Ok(())
}

#[tokio::test]
async fn exclusions_survive_page_eviction_without_changing_weighted_identity() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(64 * 1024, 1));
    let batch =
        Batch::from_updates([((2_i64, 0_i64), 1), ((4, -1), 2), ((4, 9), -3), ((20, 0), 1)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "weighted", 1000).await?;
    let object =
        ObjectBatch::<i64, i64>::open_with_cache(store.clone(), root, "weighted", cache.clone())
            .await?;
    object.read(0).await?;
    cache.insert("other", "eviction".into(), Arc::new(vec![((100_i64, 0_i64), 1)]), 16)?;
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(object.tuple_weight(&(3, 0)).await?, 0);
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert!(cache.stats()?.filter_rejections > 0);
    assert_eq!(object.tuple_weight(&(4, -1)).await?, 2);
    assert_eq!(object.tuple_weight(&(4, 9)).await?, -3);
    assert_eq!(object.tuple_weight(&(4, 999)).await?, 0);
    let trace = Arrangement::new(store, "weighted".into(), 1000)?.empty().append_runs_validated(
        vec![
            crate::engine::trace::Run::Object(object.clone()),
            crate::engine::trace::Run::Object(object),
        ],
        1,
    )?;
    let mut cursor = trace.key_cursor(&4).await?;
    assert_eq!(cursor.current(), Some((&4, &-1, 4)));
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&4, &9, -6)));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    Ok(())
}
#[tokio::test]
async fn rejected_page_metadata_cannot_seed_or_reuse_an_exclusion() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(64 * 1024, 1));
    let batch = Batch::from_updates([((2_i64, 0_i64), 1), ((20, 0), 1)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "metadata", 1000).await?;
    let mut object =
        ObjectBatch::<i64, i64>::open_with_cache(store, root, "metadata", cache.clone()).await?;
    object.test_block_mut(0)?.rows += 1;
    assert!(object.tuple_weight(&(3, 0)).await.is_err());
    assert_eq!(cache.stats()?.filter_entries, 0);
    object.test_block_mut(0)?.rows -= 1;
    object.read(0).await?;
    assert_eq!(object.tuple_weight(&(3, 0)).await?, 0);
    object.test_block_mut(0)?.rows += 1;
    assert!(object.tuple_weight(&(3, 0)).await.is_err());
    Ok(())
}
#[test]
fn native_views_have_the_same_hash_and_cache_budgets_evict_safely() -> Result<()> {
    use crate::engine::value::{NativeBudget, Value};
    let native = Value::from_native("key".into(), 7_i64, &NativeBudget::new(4096), 8, |v| *v);
    let plain = Value::from_canonical(7_i64);
    let bloom = Bloom::new(&[((native, ()), 1)])?;
    assert!(bloom.contains(&plain)?);
    let cache = BlockCache::new(4096, 1);
    for key in 0..100_i64 {
        cache.insert("s", key.to_string(), Arc::new(vec![((key, ()), 1)]), 16)?;
        let stats = cache.stats()?;
        assert!(stats.filter_bytes <= 1024 && stats.filter_entries <= 16);
    }
    assert!(cache.filters.enabled());
    assert!(!BlockCache::new(0, 1).filters.enabled());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cold_source_validation_overlaps_pages_without_extra_gets() -> Result<()> {
    let mut store = Store::unpaced();
    store.read_delay = std::time::Duration::from_millis(20);
    let store = Arc::new(store);
    let batch = Batch::from_updates((0..64_i64).map(|key| ((key, key), key % 7 - 3)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "concurrent-weights", 1).await?;
    let object =
        ObjectBatch::<i64, i64>::open(store.clone(), reference, "concurrent-weights").await?;
    let cache = object.cache.clone();
    let delta = Batch::from_updates((0..64_i64).step_by(2).map(|key| ((key, key), 1)))?;
    let trace = crate::engine::trace::TraceSnapshot::empty()
        .append_runs_validated(vec![crate::engine::trace::Run::Object(object)], 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    let cache_before = cache.stats()?;
    store.read_peak.store(0, Ordering::SeqCst);
    trace
        .validate_bag_delta(&Batch::from_updates(delta.iter().map(|(tuple, _)| (*tuple, 4)))?)
        .await?;
    let after = cache.stats()?;
    let expected = delta.iter().filter(|(tuple, _)| batch.weight(tuple) != 0).count();
    assert_eq!(
        after.foreground_io.gets - cache_before.foreground_io.gets,
        u64::try_from(expected)?
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst) - before,
        expected + usize::try_from(after.index_reads - cache_before.index_reads)?
    );

    assert!(store.read_peak.load(Ordering::SeqCst) > 1);
    assert!(store.read_peak.load(Ordering::SeqCst) <= crate::engine::reader::READ_CONCURRENCY);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn cold_statistics_overlap_keys_without_extra_gets() -> Result<()> {
    for entries in [1, 4096] {
        let mut store = Store::unpaced();
        store.read_delay = std::time::Duration::from_millis(20);
        let store = Arc::new(store);
        let cache = Arc::new(BlockCache::new(16 * 1024 * 1024, 256));
        let mut trace = crate::engine::trace::TraceSnapshot::empty();
        for key in 0..16_i64 {
            let reference = ObjectBatch::write(
                store.clone(),
                &Batch::from_updates([((key, value(key, 0)), 1)])?,
                "cold-statistics",
                1,
            )
            .await?;
            let data = ObjectBatch::open_with_cache(
                store.clone(),
                reference,
                "cold-statistics",
                cache.clone(),
            )
            .await?;
            trace = trace.append_validated(
                crate::engine::trace::Run::Object(data),
                u64::try_from(key + 1)?,
            )?;
        }
        let input = TimedBatch {
            time: trace.time() + 1,
            batch: Batch::from_updates((0..16_i64).map(|key| ((key, 1_i64), 1)))?,
        };
        let before = store.reads.load(Ordering::SeqCst);
        store.read_peak.store(0, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let output = operator()
            .evaluate(&input, &trace, Limits { resident_entries: entries, ..Limits::default() })
            .await?;
        assert_eq!(
            output.batch,
            Batch::from_updates(
                (0..16_i64).flat_map(|key| [((key, value(key, 0)), -1), ((key, value(key, 1)), 1)])
            )?
        );
        assert_eq!(store.reads.load(Ordering::SeqCst) - before, 16);
        let peak = store.read_peak.load(Ordering::SeqCst);
        assert!(peak <= crate::engine::reader::READ_CONCURRENCY);
        if entries == 1 {
            assert_eq!(peak, 1);
            assert!(started.elapsed() >= std::time::Duration::from_millis(320));
        } else {
            assert!(peak > 1);
            assert!(started.elapsed() < std::time::Duration::from_millis(160));
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_statistics_never_prefetch_unreadable_prior() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let root = ObjectBatch::write(
        store.clone(),
        &Batch::from_updates([((0_i64, value(0, 0)), 1)])?,
        "cancelled",
        1,
    )
    .await?;
    let data = ObjectBatch::<i64, String>::open_with_cache(
        store.clone(),
        root,
        "cancelled",
        Arc::new(BlockCache::new(16 * 1024 * 1024, 256)),
    )
    .await?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &data, 0).await?;
    let trace = crate::engine::trace::TraceSnapshot::empty()
        .append_runs_validated(vec![crate::engine::trace::Run::Object(data)], 1)?;
    let input = TimedBatch {
        time: 2,
        batch: Batch::from_updates([((0, 1_i64), 2), ((0, 2), -1), ((1, 1), 1), ((2, 1), 1)])?,
    };
    let reads = store.reads.load(Ordering::SeqCst);
    let output = operator().evaluate(&input, &trace, Limits::default()).await?;
    assert_eq!(output.batch, Batch::from_updates([((1, value(1, 1)), 1), ((2, value(2, 1)), 1)])?);
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn failed_and_cancelled_prefetch_preserve_heads_and_retry() -> Result<()> {
    use crate::engine::trace::{Run, TraceSnapshot};
    use object_store::{ObjectStoreExt as _, path::Path};
    let mut store = Store::unpaced();
    store.read_delay = std::time::Duration::from_millis(20);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(16 * 1024 * 1024, 256));
    let mut objects = Vec::new();
    for key in [7_i64, 9, 10] {
        let root = ObjectBatch::write(
            store.clone(),
            &Batch::from_updates([((key, key), 2)])?,
            "prefetch-retry",
            1,
        )
        .await?;
        objects.push(
            ObjectBatch::<i64, i64>::open_with_cache(
                store.clone(),
                root,
                "prefetch-retry",
                cache.clone(),
            )
            .await?,
        );
    }
    let trace = TraceSnapshot::empty().append_runs_validated(
        vec![
            Run::Object(objects[0].clone()),
            Run::Object(objects[0].clone()),
            Run::Object(objects[1].clone()),
            Run::Object(objects[2].clone()),
        ],
        1,
    )?;
    let mut probes = trace.probes();
    assert_eq!(probes.cursor(&7).await?.current(), Some((&7, &7, 4)));
    let path = Path::from(objects[1].block_range(0).await?.context("missing retry page")?.0);
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    assert!(probes.prefetch(&[&9, &10]).await.is_err());
    assert_eq!(probes.cursor(&7).await?.current(), Some((&7, &7, 4)));
    store.put(&path, bytes.into()).await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), probes.prefetch(&[&9, &10]))
            .await
            .is_err()
    );
    assert_eq!(probes.cursor(&7).await?.current(), Some((&7, &7, 4)));
    let _read_ahead = probes.prefetch(&[&9, &10]).await?;
    assert_eq!(probes.cursor(&9).await?.current(), Some((&9, &9, 2)));
    assert_eq!(store.read_active.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn clock_eviction_work_is_amortized_and_replacements_remain_bounded() -> Result<()> {
    use crate::engine::reader::format::Block;
    let filters = Filters::new(16 * 1024 * 1024, 256);
    for key in 0..4096_i64 {
        let hash = key.to_string();
        let identity = BlockCache::filter_identity::<i64, i64>("clock", &hash);
        filters.remember(identity.clone(), &[((key, key), 1)])?;
        let block = Block {
            pack: 0,
            offset: 0,
            first: (key, key),
            last: (key, key),
            length: 1,
            rows: 1,
            max_weight: 1,
            hash,
        };
        assert!(filters.contains(&identity, &key, &block)?);
    }
    assert_eq!(filters.stats()?.1, 4096);
    for key in 4096..4608_i64 {
        let identity = BlockCache::filter_identity::<i64, i64>("clock", &key.to_string());
        filters.remember(identity, &[((key, key), 1)])?;
    }
    let (bytes, entries, _, steps) = filters.stats()?;
    assert!(bytes <= 4 * 1024 * 1024);
    assert_eq!(entries, 4096);
    assert!((512..=4608).contains(&steps));
    for key in 4096..4608_i64 {
        let identity = BlockCache::filter_identity::<i64, i64>("clock", &key.to_string());
        filters.insert(identity, Bloom::new(&[((key, key), 1)])?)?;
    }
    assert_eq!(filters.stats()?.1, 4096);
    Ok(())
}

mod growth;

#[tokio::test]
async fn held_probe_heads_reference_synopses_before_clock_pressure() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let mut cache = BlockCache::new(64 * 1024, 1);
    cache.filters =
        Arc::new(Filters { bytes: 64 * 1024, entries: 2, state: std::sync::Mutex::default() });
    let cache = Arc::new(cache);
    let input = Batch::from_updates([((2_i64, 0_i64), 1), ((4, 0), 1)])?;
    let reference = ObjectBatch::write(store.clone(), &input, "a-held", 4096).await?;
    let object =
        ObjectBatch::<i64, i64>::open_with_cache(store, reference, "a-held", cache.clone()).await?;
    let trace = crate::engine::trace::TraceSnapshot::empty()
        .append_runs_validated(vec![crate::engine::trace::Run::Object(object)], 1)?;
    let mut probes = trace.probes();
    assert_eq!(probes.cursor(&2).await?.current(), Some((&2, &0, 1)));
    cache.insert("z-cold", "cold".into(), Arc::new(vec![((100_i64, 0_i64), 1)]), 16)?;
    let _prepared = probes.prepare_key(&3, 128 * 1024).await?;
    cache.insert("zz-pressure", "pressure".into(), Arc::new(vec![((200_i64, 0_i64), 1)]), 16)?;
    let state = cache.filters.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
    assert!(state.entries.keys().any(|identity| identity.1 == "a-held"));
    assert!(!state.entries.keys().any(|identity| identity.1 == "z-cold"));
    assert_eq!(state.rejected, 1);
    drop(state);
    Ok(())
}
