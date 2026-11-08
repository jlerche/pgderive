use super::*;
use crate::engine::{
    Batch,
    reader::BatchReader,
    trace::{Run, TraceSnapshot},
};

#[test]
fn unused_capacity_is_borrowed_but_tenant_ceilings_hold() -> Result<()> {
    let pool = Pool::new(16, 12)?;
    let alice = pool.tenant("alice");
    let bob = pool.tenant("bob");
    let first = Arc::new(vec![1_u8]);
    let second = Arc::new(vec![2_u8]);
    let extra = Arc::new(vec![3_u8]);
    assert!(alice.try_admit(first.clone(), 8)?);
    assert!(bob.try_admit(second.clone(), 8)?);
    assert!(!alice.try_admit(extra.clone(), 4)?);
    drop(second);
    assert!(alice.try_admit(extra.clone(), 4)?);
    assert!(!alice.try_admit(Arc::new(vec![4_u8]), 1)?);
    assert_eq!(pool.stats()?.used, 12);
    assert_eq!(pool.stats()?.peak, 16);
    drop((first, extra));
    assert_eq!(pool.stats()?.used, 0);
    Ok(())
}

#[test]
fn failed_multi_allocation_admission_is_atomic() -> Result<()> {
    let pool = Pool::new(16, 16)?;
    let admission = pool.tenant("alice");
    let first = Arc::new(vec![1_u8]);
    let second = Arc::new(vec![2_u8]);
    assert!(admission.admit_memory(vec![(first.clone(), 10), (second.clone(), 10)]).is_err());
    assert_eq!(pool.stats()?.used, 0);
    let held = Arc::new(vec![3_u8]);
    assert!(admission.try_admit(held.clone(), 8)?);
    assert!(admission.admit_memory(vec![(first.clone(), 6), (second.clone(), 6)]).is_err());
    assert_eq!(pool.stats()?.used, 8);
    drop((first, second, held));
    Ok(())
}

#[tokio::test]
async fn memtable_credit_follows_rows_pinned_by_a_cursor() -> Result<()> {
    let batch = Arc::new(Batch::from_updates([((1_i64, 2_i64), 1)])?);
    let trace = TraceSnapshot::empty().append_validated(Run::Memory(batch.clone()), 1)?;
    let charge = trace.memory_charge();
    let pool = Pool::new(charge, charge)?;
    pool.tenant("alice").admit_memory(trace.memory_allocations()?)?;
    let pin = batch.cursor().await?;
    drop((trace, batch));
    assert_eq!(pool.stats()?.used, charge);
    drop(pin);
    assert_eq!(pool.stats()?.used, 0);
    Ok(())
}

#[test]
fn allocation_identity_cannot_be_reassigned_to_another_tenant() -> Result<()> {
    let pool = Pool::new(16, 16)?;
    let value = Arc::new(vec![1_u8]);
    assert!(pool.tenant("alice").try_admit(value.clone(), 8)?);
    assert!(pool.tenant("bob").try_admit(value, 8).is_err());
    Ok(())
}
