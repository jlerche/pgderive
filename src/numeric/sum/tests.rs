use super::Sum;
use anyhow::Result;
use num_bigint::BigInt;
#[test]
fn scales_and_specials_retract_without_losing_hidden_finite_sum() -> Result<()> {
    let mut sum = Sum::default();
    let mut count = BigInt::default();
    for (value, weight, expected) in [
        ("1.2000", 1, Some("1.2000")),
        ("2.30", 1, Some("3.5000")),
        ("NaN", 1, Some("NaN")),
        ("Infinity", 1, Some("NaN")),
        ("-Infinity", 1, Some("NaN")),
        ("NaN", -1, Some("NaN")),
        ("-Infinity", -1, Some("Infinity")),
        ("1.2000", -1, Some("Infinity")),
        ("Infinity", -1, Some("2.30")),
        ("2.30", -1, None),
        ("1", 1, Some("1")),
        ("1.00", 1, Some("2.00")),
        ("1.00", -1, Some("1")),
    ] {
        sum.add_scaled(&Sum::measure(value)?, weight);
        count += weight;
        assert_eq!(sum.result(&count)?.as_deref(), expected);
        sum = serde_json::from_slice(&serde_json::to_vec(&sum.canonical(&count)?)?)?;
    }
    Ok(())
}
#[test]
fn exact_signed_cancellation_defers_display_limits_until_final_result() -> Result<()> {
    let large = "9".repeat(4096);
    let mut sum = Sum::default();
    let value = Sum::measure(&large)?;
    sum.add_scaled(&value, 2);
    assert!(sum.result(&2.into()).is_err());
    let nan = Sum::measure("NaN")?;
    sum.add_scaled(&nan, 1);
    assert_eq!(sum.result(&3.into())?.as_deref(), Some("NaN"));
    sum.add_scaled(&value, -1);
    sum.add_scaled(&nan, -1);
    assert_eq!(sum.result(&1.into())?.as_deref(), Some(large.as_str()));
    sum.add_scaled(&value, -1);
    assert!(sum.is_zero());
    let small = Sum::measure("1.00")?;
    sum.add_scaled(&small, i64::MIN);
    sum.add_scaled(&small, i64::MAX);
    sum.add_scaled(&small, 1);
    assert!(sum.is_zero());
    assert_eq!(sum.result(&0.into())?, None);
    Ok(())
}
#[test]
fn malformed_persisted_summaries_are_rejected_before_arithmetic() -> Result<()> {
    let valid = Sum::measure("1.00")?;
    for key in ["nan", "positive", "negative"] {
        let mut encoded = serde_json::to_value(&valid)?;
        encoded[key] = serde_json::to_value(BigInt::from(-1))?;
        assert!(serde_json::from_value::<Sum>(encoded)?.canonical(&0.into()).is_err());
    }
    let mut bad = valid.clone();
    bad.scale = u32::MAX;
    assert!(bad.canonical(&1.into()).is_err());
    let mut bad = valid.clone();
    bad.scales.clear();
    assert!(bad.canonical(&1.into()).is_err());
    let mut bad = valid.clone();
    bad.scales.insert(2, (-1).into());
    assert!(bad.canonical(&1.into()).is_err());
    let mut bad = valid.clone();
    bad.coefficient += 1;
    bad.scales.clear();
    bad.scales.insert(0, 1.into());
    assert!(bad.canonical(&1.into()).is_err());
    assert!(valid.canonical(&2.into()).is_err());
    assert!(valid.heap_bytes()? > 0);
    Ok(())
}
