use super::{Consolidator, Limits};
use anyhow::Result;
use num_bigint::BigInt;

fn limits(entries: usize) -> Limits {
    Limits { resident_entries: entries, ..Limits::default() }
}
#[test]
fn spill_boundaries_never_finalize_intermediate_weights() -> Result<()> {
    for size in [1, 2, 7, 4096] {
        let mut weights = Consolidator::new(limits(size))?;
        for n in 0..20 {
            weights.add((n, 0), BigInt::from(i64::MAX) * 2)?;
        }
        for n in (0..20).rev() {
            weights.add((n, 0), -BigInt::from(i64::MAX))?;
        }
        let batch = weights.finish_batch()?;
        assert_eq!(batch.iter().count(), 20);
        assert!(batch.iter().all(|(_, weight)| *weight == i64::MAX));
    }
    let mut overflow = Consolidator::new(limits(1))?;
    overflow.add((0, 0), BigInt::from(i64::MAX) + 1)?;
    assert!(overflow.finish_batch().is_err());
    Ok(())
}
#[test]
fn cancellation_and_resource_failures_are_explicit() -> Result<()> {
    let mut weights = Consolidator::new(limits(1))?;
    weights.add((0, 0), 3)?;
    weights.add((1, 0), 2)?;
    weights.add((0, 0), -3)?;
    weights.add((1, 0), -2)?;
    assert_eq!(weights.finish_batch()?.iter().count(), 0);
    let mut contributions = Consolidator::new(Limits { contributions: 1, ..limits(1) })?;
    contributions.add((0, 0), 1)?;
    assert!(contributions.add((0, 0), 1).is_err());
    assert!(contributions.finish_batch().is_err());
    let mut runs = Consolidator::new(Limits { spill_runs: 1, ..limits(1) })?;
    runs.add((0, 0), 1)?;
    runs.add((1, 0), 1)?;
    assert!(runs.add((2, 0), 1).is_err());
    let mut outputs = Consolidator::new(Limits { output_entries: 1, ..limits(1) })?;
    outputs.add((0, 0), 1)?;
    outputs.add((1, 0), 1)?;
    assert!(outputs.finish_batch().is_err());
    let mut disk = Consolidator::new(Limits { scratch_bytes: 1, ..limits(1) })?;
    disk.add((0, 0), 1)?;
    assert!(disk.add((1, 0), 1).is_err());
    assert!(disk.finish_batch().is_err());
    let mut record = Consolidator::new(Limits { record_bytes: 32, ..limits(1) })?;
    assert!(record.add((0, "x".repeat(64)), 1).is_err());
    assert!(Consolidator::<(i64, i64)>::new(Limits { resident_entries: 0, ..limits(1) }).is_err());
    Ok(())
}
#[test]
fn malformed_scratch_fails_closed() -> Result<()> {
    use std::io::Cursor;
    let bytes = super::record::encode(&(1, 2), &BigInt::from(3), 100)?;
    let mut record = Vec::new();
    super::record::write(&mut record, &bytes, 0, 1024)?;
    assert_eq!(
        super::record::read::<(i64, i64)>(&mut Cursor::new(&record), 100)?,
        Some(((1, 2), BigInt::from(3)))
    );
    record[36] ^= 1;
    assert!(super::record::read::<(i64, i64)>(&mut Cursor::new(&record), 100).is_err());
    assert!(super::record::read::<(i64, i64)>(&mut Cursor::new(&record[..8]), 100).is_err());
    assert!(super::record::read::<(i64, i64)>(&mut Cursor::new(&record), 1).is_err());
    Ok(())
}

#[test]
fn byte_spills_and_output_byte_budget_are_enforced() -> Result<()> {
    let limits = Limits { resident_bytes: 64, record_bytes: 64, ..limits(4096) };
    let mut weights = Consolidator::new(limits)?;
    for key in 0..20 {
        weights.add((key, "x".repeat(20)), 1)?;
        assert!(weights.bytes <= limits.resident_bytes);
        assert!(weights.entries.len() <= limits.resident_entries);
    }
    assert!(!weights.runs.is_empty());
    assert_eq!(weights.finish_batch()?.iter().count(), 20);
    let mut outputs = Consolidator::new(Limits { output_bytes: 1, ..limits })?;
    outputs.add((0, 0), 1)?;
    assert!(outputs.finish_batch().is_err());
    let mut enormous = Consolidator::new(Limits { record_bytes: 10, ..limits })?;
    assert!(enormous.add((0, 0), BigInt::from(1) << 100).is_err());
    Ok(())
}

#[test]
fn resident_finalization_needs_no_scratch_bytes_and_keeps_final_limits() -> Result<()> {
    let small = Limits { scratch_bytes: 1, ..Limits::default() };
    let mut weights = Consolidator::new(small)?;
    weights.add((0, 0), BigInt::from(i64::MAX) * 2)?;
    weights.add((0, 0), -BigInt::from(i64::MAX))?;
    weights.add((1, 0), 3)?;
    weights.add((1, 0), -3)?;
    assert_eq!(weights.finish_batch()?, crate::engine::Batch::from_updates([((0, 0), i64::MAX)])?);
    let mut overflow = Consolidator::new(small)?;
    overflow.add((0, 0), BigInt::from(i64::MAX) + 1)?;
    assert!(overflow.finish_batch().is_err());
    let mut limited = Consolidator::new(Limits { output_entries: 1, ..small })?;
    limited.add((0, 0), 1)?;
    limited.add((1, 0), 1)?;
    assert!(limited.finish_batch().is_err());
    Ok(())
}

#[test]
fn weighted_sizes_match_wire_encoding_at_exact_boundaries() -> Result<()> {
    let tuple = ("quoted\"\\\nλ", [None, Some("value")]);
    for weight in [
        BigInt::from(i64::MIN),
        BigInt::from(i64::MAX),
        BigInt::from(0),
        BigInt::from(1) << 256_usize,
        -(BigInt::from(1) << 256_usize),
    ] {
        let expected = super::record::encode(&tuple, &weight, 4096)?.len();
        assert_eq!(super::record::size(&tuple, &weight, expected)?, expected);
        assert!(super::record::size(&tuple, &weight, expected - 1).is_err());
        assert!(super::record::encode(&tuple, &weight, expected - 1).is_err());
    }
    assert!(super::record::weighted_size(usize::MAX, &BigInt::from(1), usize::MAX).is_err());
    Ok(())
}

#[test]
fn canonical_handoff_rejects_invalid_order_and_zero_weights() -> Result<()> {
    use crate::engine::Batch;
    assert!(Batch::from_canonical(vec![((1, 1), 0)]).is_err());
    assert!(Batch::from_canonical(vec![((1, 1), 1), ((1, 1), -1)]).is_err());
    assert!(Batch::from_canonical(vec![((2, 1), 1), ((1, 1), 1)]).is_err());
    assert_eq!(
        Batch::from_canonical(vec![((1, 1), -1), ((2, 1), 1)])?,
        Batch::from_updates([((2, 1), 1), ((1, 1), -1)])?
    );
    Ok(())
}

#[test]
fn resident_handoff_reuses_tuple_sizes_without_reserializing_rows() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
    struct Counted(String);
    impl serde::Serialize for Counted {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            serializer.serialize_str(&self.0)
        }
    }
    let mut weights = Consolidator::new(Limits::default())?;
    for key in ["quoted\"λ", "second"] {
        weights.add((Counted(key.into()), ()), i64::MAX)?;
        weights.add((Counted(key.into()), ()), -1)?;
    }
    assert_eq!(CALLS.load(Ordering::Relaxed), 2);
    let batch = weights.finish_batch()?;
    Limits::default().check_batch(&batch)?;
    assert_eq!(CALLS.load(Ordering::Relaxed), 2);
    let size = batch.encoded_size(4096, u64::MAX)?;
    let expected = batch
        .iter()
        .map(|row| serde_json::to_vec(&row))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        size.bytes,
        expected
            .iter()
            .map(|row| u64::try_from(row.len()))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>()
    );
    assert_eq!(size.largest, expected.iter().map(Vec::len).max().unwrap_or(0));
    assert!(batch.encoded_size(size.largest - 1, u64::MAX).is_err());
    assert!(batch.encoded_size(4096, size.bytes - 1).is_err());
    Ok(())
}

#[test]
fn canonical_sizes_match_after_cross_run_cancellation() -> Result<()> {
    for entries in [1, 3, 4096] {
        let mut weights = Consolidator::new(limits(entries))?;
        for key in 0..12 {
            weights.add((key, format!("\\\"λ{key}")), BigInt::from(i64::MAX) * 2)?;
        }
        for key in (0..12).rev() {
            weights.add((key, format!("\\\"λ{key}")), -BigInt::from(i64::MAX))?;
        }
        let batch = weights.finish_batch()?;
        let size = batch.encoded_size(4096, u64::MAX)?;
        assert_eq!(size.entries, 12);
        let expected = batch
            .iter()
            .map(|row| Ok(u64::try_from(serde_json::to_vec(&row)?.len())?))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum::<u64>();
        assert_eq!(size.bytes, expected);
        assert!(batch.iter().all(|(_, weight)| *weight == i64::MAX));
    }
    Ok(())
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Representative {
    id: u64,
    payload: String,
}
impl PartialEq for Representative {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Representative {}
impl PartialOrd for Representative {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Representative {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
#[test]
fn occupied_spill_rechecks_the_new_representative_size() -> Result<()> {
    let a = Representative { id: 0, payload: "a".into() };
    let b = Representative { id: 1, payload: "b".into() };
    let bound = super::record::encode(&(a.clone(), ()), &BigInt::from(9), 4096)?.len()
        + super::record::encode(&(b.clone(), ()), &BigInt::from(9), 4096)?.len();
    let mut weights = Consolidator::new(Limits {
        resident_bytes: bound,
        record_bytes: bound,
        ..Limits::default()
    })?;
    weights.add((a, ()), 9)?;
    weights.add((b, ()), 9)?;
    assert_eq!(weights.bytes, bound);
    assert!(
        weights.add((Representative { id: 0, payload: "large".repeat(bound) }, ()), 1).is_err()
    );
    assert!(weights.bytes <= bound);
    assert!(weights.finish_batch().is_err());
    Ok(())
}
