use super::Integers;
use crate::{engine::value::NativeBudget, transaction::Row};
use anyhow::{Context, Result};
use num_bigint::BigInt;

fn row() -> Row {
    [("stat_0".into(), Some("1".into())), ("stat_1".into(), Some("0".into()))].into()
}
#[test]
fn normalized_integers_discard_arithmetic_capacity_before_retention() -> Result<()> {
    let mut huge = BigInt::from(1) << 65_536;
    huge >>= 65_536;
    let budget = NativeBudget::new(1024);
    let codec = Integers::new("integer-test-v1", 2, budget.clone());
    let cached = codec.retain(row(), &[huge, 0.into()])?;
    assert!(budget.used() > 0 && budget.used() <= 1024);
    let decoded = codec.decode(&cached, |_| anyhow::bail!("normalized value was not retained"))?;
    assert_eq!(*decoded, vec![1.into(), 0.into()]);
    drop(cached);
    assert!(budget.used() > 0);
    drop(decoded);
    assert_eq!(budget.used(), 0);
    Ok(())
}
#[test]
fn cold_and_exhausted_vectors_keep_identical_canonical_state() -> Result<()> {
    for bytes in [0, 1, 4096] {
        let budget = NativeBudget::new(bytes);
        let codec = Integers::new("integer-test-v1", 2, budget.clone());
        let cached = codec.retain(row(), &[1.into(), 0.into()])?;
        assert_eq!(cached, row());
        let cold: Row = serde_json::from_slice(&serde_json::to_vec(&cached)?)?;
        let decoded = codec.decode(&cold, |row| {
            ["stat_0", "stat_1"]
                .into_iter()
                .map(|field| {
                    row.get(field)
                        .and_then(Option::as_deref)
                        .context("missing integer field")?
                        .parse()
                        .map_err(Into::into)
                })
                .collect()
        })?;
        assert_eq!(*decoded, vec![1.into(), 0.into()]);
        assert!(budget.used() <= bytes);
    }
    Ok(())
}
#[test]
fn native_group_keys_charge_retained_string_capacity() -> Result<()> {
    let budget = NativeBudget::new(1024);
    let mut large = String::with_capacity(8192);
    large.push('x');
    let codec = crate::worker::program::keys::GroupCodec::new(budget.clone());
    let key = codec.group(vec![Some(large), None])?;
    assert_eq!(budget.used(), 0);
    assert_eq!(*crate::worker::program::keys::values(&key, &budget)?, vec![Some("x".into()), None]);
    let small = codec.group(vec![Some("x".into()), None])?;
    assert_eq!(key, small);
    assert!(budget.used() > 0 && budget.used() <= 1024);
    Ok(())
}
