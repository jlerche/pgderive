use super::{Numeric, Rounding, Typmod};
use anyhow::Result;
#[test]
fn division_matches_pg_significant_scale_and_special_values() -> Result<()> {
    for (left, right, expected) in [
        ("1", "3", "0.33333333333333333333"),
        ("5.0", "2", "2.5000000000000000"),
        ("-1", "6", "-0.16666666666666666667"),
        ("1.0000000000000000000000000", "2", "0.5000000000000000000000000"),
        ("0.00001", "2", "0.000005000000000000000000"),
        ("0.00", "3", "0.00000000000000000000"),
        ("NaN", "0", "NaN"),
        ("Infinity", "Infinity", "NaN"),
        ("-Infinity", "-2", "Infinity"),
        ("Infinity", "-2", "-Infinity"),
        ("1.20", "Infinity", "0"),
    ] {
        assert_eq!(
            Numeric::parse(left)?.divide(&Numeric::parse(right)?)?.text(),
            expected,
            "{left}/{right}"
        );
    }
    for left in ["0", "1", "Infinity", "-Infinity"] {
        assert!(Numeric::parse(left)?.divide(&Numeric::parse("0")?).is_err());
    }
    assert_eq!(
        Numeric::average("1.200000000000000000000000/2")?.text(),
        "0.600000000000000000000000"
    );
    Ok(())
}
#[test]
fn average_preserves_native_values_and_validates_count() -> Result<()> {
    for (ratio, expected) in [
        ("-15.00/10", "-1.5000000000000000"),
        ("1/9223372036854775807", "0.000000000000000000108420217248550443"),
        ("NaN/1", "NaN"),
        ("Infinity/2", "Infinity"),
        ("-Infinity/2", "-Infinity"),
    ] {
        assert_eq!(Numeric::average(ratio)?.text(), expected, "{ratio}");
    }
    for ratio in ["1", "1/0", "1/-1", "1/9223372036854775808", "1/x"] {
        assert!(Numeric::average(ratio).is_err(), "{ratio}");
    }
    Ok(())
}

#[test]
fn quantization_preserves_display_scale_rounding_and_extremes() -> Result<()> {
    for (value, scale, mode, expected) in [
        ("2.5", 0, Rounding::Nearest, "3"),
        ("-2.5", 0, Rounding::Nearest, "-3"),
        ("1250", -2, Rounding::Nearest, "1300"),
        ("-1250", -2, Rounding::Nearest, "-1300"),
        ("1250", -2, Rounding::Truncate, "1200"),
        ("-1250", -2, Rounding::Truncate, "-1200"),
        ("-1.001", 0, Rounding::Ceiling, "-1"),
        ("-1.001", 0, Rounding::Floor, "-2"),
        ("1e-100", 0, Rounding::Ceiling, "1"),
        ("-1e-100", 0, Rounding::Floor, "-1"),
        ("1e-100", 2, Rounding::Nearest, "0.00"),
        ("0", 4, Rounding::Nearest, "0.0000"),
        ("1", i32::MIN, Rounding::Nearest, "0"),
        ("NaN", i32::MAX, Rounding::Nearest, "NaN"),
    ] {
        assert_eq!(Numeric::parse(value)?.rounded(scale, mode)?.text(), expected);
    }
    assert!(Numeric::parse("1")?.rounded(i32::MAX, Rounding::Nearest).is_err());
    assert_eq!(Numeric::parse("-Infinity")?.absolute().text(), "Infinity");
    Ok(())
}
#[test]
fn constrained_numeric_and_integer_casts_round_before_checking_bounds() -> Result<()> {
    for (value, p, s, expected) in [
        ("12.345", 5, 2, "12.35"),
        ("-12.345", 5, 2, "-12.35"),
        ("1499", 2, -3, "1000"),
        ("0.00994", 2, 4, "0.0099"),
        ("0", 1, 1000, &format!("0.{}", "0".repeat(1000))),
        ("NaN", 1, -1000, "NaN"),
    ] {
        assert_eq!(Numeric::parse(value)?.constrained(Typmod::new(p, s)?)?.text(), expected);
    }
    for (value, p, s) in
        [("999.995", 5, 2), ("0.00995", 2, 4), ("Infinity", 5, 2), ("-Infinity", 5, 2)]
    {
        assert!(Numeric::parse(value)?.constrained(Typmod::new(p, s)?).is_err());
    }
    assert_eq!(Numeric::parse("-9223372036854775808.49")?.integer()?, i64::MIN);
    for value in ["9223372036854775807.5", "NaN", "Infinity"] {
        assert!(Numeric::parse(value)?.integer().is_err());
    }
    for (p, s) in [(0, 0), (1001, 0), (1, -1001), (1, 1001)] {
        assert!(Typmod::new(p, s).is_err());
    }
    Ok(())
}
#[test]
fn numeric_average_bounds_the_final_value_instead_of_the_intermediate_sum() -> Result<()> {
    let text = "9".repeat(4096);
    let mut total = super::sum::Sum::default();
    total.add_scaled(&super::sum::Sum::measure(&text)?, 2);
    assert!(total.result(&2.into()).is_err());
    let ratio =
        total.average_ratio(&2.into())?.ok_or_else(|| anyhow::anyhow!("missing average"))?;
    assert_eq!(Numeric::average(&ratio)?.text(), text);
    Ok(())
}

#[test]
fn average_keeps_aligned_sum_spanning_both_input_digit_extremes() -> Result<()> {
    let mut total = super::sum::Sum::default();
    for value in ["1e100", "1e-4090"] {
        total.add_scaled(&super::sum::Sum::measure(value)?, 1);
    }
    assert!(total.result(&2.into()).is_err());
    let ratio =
        total.average_ratio(&2.into())?.ok_or_else(|| anyhow::anyhow!("missing average"))?;
    assert_eq!(
        Numeric::average(&ratio)?.text(),
        format!("5{}.{}", "0".repeat(99), "0".repeat(1000))
    );
    Ok(())
}
