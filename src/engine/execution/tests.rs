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
