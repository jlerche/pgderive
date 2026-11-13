use super::{Additive, Budget, tuple_size};
use crate::engine::execution::{CapacityExceeded, Limits, paired_record_size, record_size};
use anyhow::Result;
use num_bigint::BigInt;

#[test]
fn cached_tuple_and_scalar_sizes_match_canonical_json_at_exact_boundaries() -> Result<()> {
    for key in ["", "quoted\"\\\nλ", &"wide".repeat(1024)] {
        for column in [0, 9, 10, 99, 100, 128] {
            let tuple = (key, column);
            let bytes = tuple_size(record_size(&key, usize::MAX)?, column)?;
            assert_eq!(bytes, serde_json::to_vec(&tuple)?.len());
            for value in [BigInt::from(i64::MIN), BigInt::from(0), BigInt::from(i64::MAX)] {
                let exact = serde_json::to_vec(&(tuple, value.to_string()))?.len();
                assert_eq!(value.record_size(bytes, exact)?, exact);
                let error = value.record_size(bytes, exact - 1).err();
                assert!(error.is_some_and(|error| error.is::<CapacityExceeded>()));
            }
            for value in [vec![], vec![0_u64, u64::MAX]] {
                let exact = serde_json::to_vec(&(tuple, &value))?.len();
                assert_eq!(paired_record_size(bytes, &value, exact)?, exact);
                let error = paired_record_size(bytes, &value, exact - 1).err();
                assert!(error.is_some_and(|error| error.is::<CapacityExceeded>()));
            }
        }
    }
    Ok(())
}

#[test]
fn group_boundaries_reset_cached_key_size_and_keep_running_record_checks() -> Result<()> {
    let limits = Limits { record_bytes: 128, ..Limits::default() };
    let mut budget = Budget::new(limits);
    for key in ["a", "longer\"λ", "z"] {
        let key = key.to_owned();
        budget.add(&key, 0, &BigInt::from(1))?;
        budget.add(&key, 1, &BigInt::from(2))?;
        let before = budget.bytes;
        budget.finish(&key, &[BigInt::from(1), BigInt::from(2)])?;
        let expected = serde_json::to_vec(&((&key, 0), "1"))?.len()
            + serde_json::to_vec(&((&key, 1), "2"))?.len();
        assert_eq!(budget.bytes - before, u64::try_from(expected)?);
    }
    let key = "k".to_owned();
    budget.add(&key, 0, &BigInt::from(1))?;
    let huge = BigInt::from(10).pow(128);
    assert!(budget.add(&key, 0, &huge).err().is_some_and(|error| error.is::<CapacityExceeded>()));
    Ok(())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Representative(u64, String);
impl PartialEq for Representative {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Representative {}
impl std::hash::Hash for Representative {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0, state);
    }
}
impl PartialOrd for Representative {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Representative {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
#[tokio::test]
async fn equal_ordering_keys_preserve_each_rows_serialized_record_guard() -> Result<()> {
    use crate::engine::{
        Batch,
        dataflow::{Arrangement, Statistics, TimedBatch},
    };
    use object_store::memory::InMemory;
    use std::sync::Arc;
    let input = TimedBatch {
        time: 1,
        batch: Batch::from_updates([
            ((Representative(1, String::new()), 0_u64), 1),
            ((Representative(1, "x".repeat(80)), 1_u64), 1),
        ])?,
    };
    let limits = Limits { record_bytes: 128, ..Limits::default() };
    limits.check_batch(&input.batch)?;
    let operator = Statistics::with_measure(
        1,
        |_, _: &u64| Ok(vec![BigInt::from(10).pow(60)]),
        |_: &String| Ok(vec![BigInt::default()]),
        |_: &Representative, values| Ok(Some(values[0].to_string())),
    );
    let writer = Arrangement::<Representative, String>::new(
        Arc::new(InMemory::new()),
        "representative-size".into(),
        8,
    )?;
    let result = operator.evaluate(&input, &writer.empty(), limits).await;
    assert!(result.err().is_some_and(|error| error.is::<CapacityExceeded>()));
    Ok(())
}
