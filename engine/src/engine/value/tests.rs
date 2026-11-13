use super::{NativeBudget, Value};
use anyhow::Result;
use std::sync::Arc;
fn value(budget: &Arc<NativeBudget>) -> Value<Vec<String>> {
    Value::from_native(Arc::from("numbers-v1"), vec![7_u64], budget, 8, |v| {
        v.iter().map(u64::to_string).collect()
    })
}
#[test]
fn permits_follow_decoded_handles_after_row_drop_and_mutation() -> Result<()> {
    let budget = NativeBudget::new(4096);
    let mut row = value(&budget);
    let decoded =
        row.decode::<Vec<u64>>("numbers-v1", &budget, |_| anyhow::bail!("unexpected decode"))?;
    let pinned = decoded.clone();
    let charge = budget.used();
    assert!(charge >= 8);
    row.push("8".to_owned());
    drop(row);
    assert_eq!(budget.used(), charge);
    assert_eq!(*decoded, vec![7]);
    drop(decoded);
    assert_eq!(budget.used(), charge);
    drop(pinned);
    assert_eq!(budget.used(), 0);
    Ok(())
}
#[test]
fn exact_exhaustion_and_zero_disable_retention_without_changing_wire() -> Result<()> {
    let generous = NativeBudget::new(4096);
    let first = value(&generous);
    let charge = generous.used();
    let exact = NativeBudget::new(charge);
    let cached = value(&exact);
    let uncached = value(&exact);
    assert_eq!(exact.used(), charge);
    assert_eq!(cached, uncached);
    assert_eq!(serde_json::to_vec(&cached)?, serde_json::to_vec(&*first)?);
    drop(cached);
    assert_eq!(exact.used(), 0);
    let zero = NativeBudget::new(0);
    let row = value(&zero);
    assert_eq!(row, uncached);
    assert_eq!(*row.decode("numbers-v1", &zero, |_| Ok(vec![7_u64]))?, vec![7]);
    assert_eq!(zero.used(), 0);
    Ok(())
}
#[test]
fn failed_decoders_and_incompatible_ids_never_poison_identity() -> Result<()> {
    let budget = NativeBudget::new(4096);
    let row = Value::from(vec!["7".to_owned()]);
    assert!(row.decode::<u64>("number", &budget, |_| anyhow::bail!("failure")).is_err());
    assert_eq!(budget.used(), 0);
    let decoded = row.decode("number", &budget, |r| Ok(r[0].parse::<u64>()?))?;
    assert_eq!(*decoded, 7);
    let cached = value(&budget);
    assert!(cached.decode::<String>("numbers-v1", &budget, |_| Ok(String::new())).is_err());
    drop(cached);
    let different = row.decode("other", &budget, |_| Ok(9_u64))?;
    assert_eq!(*different, 9);
    let clone = row.clone();
    assert_eq!(row, clone);
    drop(row);
    drop(clone);
    drop(decoded);
    drop(different);
    assert_eq!(budget.used(), 0);
    Ok(())
}
#[test]
fn concurrent_reservations_remain_bounded() -> Result<()> {
    let budget = NativeBudget::new(512);
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers = (0..8)
        .map(|_| {
            let budget = budget.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                value(&budget)
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(workers.len(), 8);
    let values = workers
        .into_iter()
        .map(|w| w.join().map_err(|_| anyhow::anyhow!("test thread failed")))
        .collect::<Result<Vec<_>>>()?;
    assert!(budget.used() <= 512);
    assert!(values.iter().all(|v| v == &values[0]));
    drop(values);
    assert_eq!(budget.used(), 0);
    Ok(())
}
#[test]
fn canonical_values_do_not_allocate_cache_storage_and_cross_budget_is_transient() -> Result<()> {
    let plain = Value::from(vec!["7".to_owned()]);
    assert!(plain.view.is_none());
    let owner = NativeBudget::new(4096);
    let row = value(&owner);
    let other = NativeBudget::new(0);
    let decoded = row.decode("numbers-v1", &other, |_| Ok(vec![7_u64]))?;
    assert!(!decoded.belongs_to(&owner));
    assert_eq!(other.used(), 0);
    let huge =
        Value::from_native(Arc::from("large"), 7_u64, &owner, usize::MAX, |v| vec![v.to_string()]);
    assert!(huge.view.is_none());
    drop(row);
    assert_eq!(owner.used(), 0);
    Ok(())
}
#[test]
fn over_aligned_native_owner_is_charged_before_retention() {
    #[repr(align(128))]
    struct Aligned(u64);
    let budget = NativeBudget::new(4096);
    let row =
        Value::from_native(Arc::from("aligned"), Aligned(7), &budget, 0, |v| vec![v.0.to_string()]);
    assert!(budget.used() >= 256);
    assert!(row.view.is_some());
    drop(row);
    assert_eq!(budget.used(), 0);
}

#[test]
fn clones_mutate_independently_and_keep_canonical_wire_ordering() -> Result<()> {
    let budget = NativeBudget::new(4096);
    let original = value(&budget);
    let mut clone = original.clone();
    clone.push("8".to_owned());
    assert!(clone.view.is_none());
    assert!(original.view.is_some());
    assert!(original < clone);
    let bytes = serde_json::to_vec(&original)?;
    assert_eq!(bytes, serde_json::to_vec(&vec!["7".to_owned()])?);
    let restored: Value<Vec<String>> = serde_json::from_slice(&bytes)?;
    assert_eq!(restored, original);
    let set = std::collections::BTreeSet::from([original, clone]);
    assert!(set.contains(&restored));
    Ok(())
}

#[test]
fn shared_native_credit_follows_last_decoded_owner_without_registry_entries() -> Result<()> {
    let pool = crate::engine::capacity::Pool::new(8192, 4096)?;
    let budget = NativeBudget::with_capacity(4096, Some(pool.tenant("alice")));
    let row = value(&budget);
    let pin =
        row.decode::<Vec<u64>>("numbers-v1", &budget, |_| anyhow::bail!("unexpected decode"))?;
    let bytes = budget.used();
    assert!(bytes > 0);
    assert_eq!(pool.stats()?.used, bytes);
    drop(row);
    assert_eq!(pool.stats()?.used, bytes);
    drop(pin);
    assert_eq!(pool.stats()?.used, 0);
    Ok(())
}

#[test]
#[ignore = "manual retained-state admission and diagnostics scaling evidence"]
fn pooled_native_admission_scaling() -> Result<()> {
    use std::time::Instant;
    for (count, queries) in [(1000, 1), (10_000, 7), (100_000, 63)] {
        let pool = crate::engine::capacity::Pool::new(64 * 1024 * 1024, 64 * 1024 * 1024)?;
        let idle = (0..queries)
            .map(|index| {
                NativeBudget::with_capacity(
                    4 * 1024 * 1024,
                    Some(pool.tenant(&format!("idle-{index}"))),
                )
            })
            .collect::<Vec<_>>();
        let active = NativeBudget::with_capacity(1024 * 1024, Some(pool.tenant("active")));
        let began = Instant::now();
        let rows = (0..count).map(|index| value(&idle[index % idle.len()])).collect::<Vec<_>>();
        let build_us = began.elapsed().as_micros();
        let expected = idle.iter().map(|budget| budget.used()).sum::<usize>();
        assert!(expected > 0);
        let (active_us, diagnostics_us, stats_us) = sample_pool(&pool, &active, expected)?;
        eprintln!(
            "{}",
            serde_json::json!({"retained_values":count,"idle_queries":queries,"build_us":build_us,"active_20k_us":active_us,"diagnostics_1k_us":diagnostics_us,"stats_1k_us":stats_us,"retained_bytes":expected})
        );
        drop(rows);
        assert_eq!(pool.stats()?.used, 0);
    }
    Ok(())
}

fn sample_pool(
    pool: &crate::engine::capacity::Pool,
    active: &Arc<NativeBudget>,
    expected: usize,
) -> Result<(u128, u128, u128)> {
    use std::time::Instant;
    let began = Instant::now();
    for _ in 0..20_000 {
        drop(value(active));
    }
    let active_us = began.elapsed().as_micros();
    let expected_json = serde_json::json!(expected);
    let began = Instant::now();
    for _ in 0..1000 {
        let observed = serde_json::to_value(pool.observation("active")?)?;
        assert_eq!(observed["used"], expected_json);
    }
    let diagnostics_us = began.elapsed().as_micros();
    let began = Instant::now();
    for _ in 0..1000 {
        assert_eq!(pool.stats()?.used, expected);
    }
    Ok((active_us, diagnostics_us, began.elapsed().as_micros()))
}

#[test]
#[ignore = "manual weak-cache retained-state admission scaling evidence"]
fn pooled_weak_cache_admission_scaling() -> Result<()> {
    let pool = crate::engine::capacity::Pool::new(64 * 1024 * 1024, 64 * 1024 * 1024)?;
    let admission = pool.tenant("idle");
    let rows = (0..100_000).map(|_| Arc::new(vec![42_u8])).collect::<Vec<_>>();
    for row in &rows {
        assert!(admission.try_admit(row.clone(), 256)?);
    }
    let active = NativeBudget::with_capacity(1024 * 1024, Some(pool.tenant("active")));
    let expected = 256 * rows.len();
    let (active_us, diagnostics_us, stats_us) = sample_pool(&pool, &active, expected)?;
    eprintln!(
        "{}",
        serde_json::json!({"retained_cache_entries":rows.len(),"active_20k_us":active_us,"diagnostics_1k_us":diagnostics_us,"explicit_stats_1k_us":stats_us})
    );
    drop(rows);
    assert_eq!(pool.stats()?.used, 0);
    Ok(())
}

#[test]
fn failed_native_pool_admission_rolls_back_local_reservation() -> Result<()> {
    let pool = crate::engine::capacity::Pool::new(1, 1)?;
    let budget = NativeBudget::with_capacity(4096, Some(pool.tenant("alice")));
    let row = value(&budget);
    assert_eq!(budget.used(), 0);
    assert_eq!(pool.stats()?.used, 0);
    let decoded = row.decode::<Vec<u64>>("numbers-v1", &budget, |wire| {
        Ok(wire
            .iter()
            .map(|item| item.parse::<u64>())
            .collect::<std::result::Result<Vec<_>, _>>()?)
    })?;
    assert_eq!(*decoded, vec![7]);
    assert_eq!(budget.used(), 0);
    assert_eq!(pool.stats()?.used, 0);
    Ok(())
}
