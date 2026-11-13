use super::Timestamp;
use anyhow::Result;
#[test]
fn native_range_epoch_leaps_offsets_and_infinities() -> Result<()> {
    let values = [
        ("2000-01-01 00:00:00", 0),
        ("1999-12-31 23:59:59.999999", -1),
        ("1970-01-01 00:00:00", -946_684_800_000_000),
        ("2000-02-29 00:00:00", 59 * super::DAY),
        ("294276-12-31 23:59:59.999999", super::END - 1),
        ("4714-11-24 00:00:00 BC", super::MIN),
    ];
    for (text, micros) in values {
        let value = Timestamp::parse(text, 1114)?;
        assert_eq!(value.sort_value(), micros);
        assert_eq!(value.json(1114)?, text.replacen(' ', "T", 1));
    }
    let utc = Timestamp::parse("2000-01-01 00:00:00+00", 1184)?;
    assert_eq!(utc, Timestamp::parse("2000-01-01T05:30:00+05:30", 1184)?);
    assert_eq!(utc, Timestamp::parse("1999-12-31T16:00:00-08:00", 1184)?);
    assert_eq!(utc.json(1184)?, "2000-01-01T00:00:00+00:00");
    let before = Timestamp::parse("0001-02-29 00:00:00 BC", 1114)?;
    assert_eq!(before.json(1114)?, "0001-02-29T00:00:00 BC");
    let negative = Timestamp::parse("-infinity", 1114)?;
    let positive = Timestamp::parse("infinity", 1184)?;
    assert!(negative < before && before < utc && utc < positive);
    assert_eq!(negative.json(1114)?, "-infinity");
    assert_eq!(positive.json(1184)?, "infinity");
    Ok(())
}
#[test]
fn invalid_or_context_dependent_timestamp_inputs_fail() {
    for (text, oid) in [
        ("0000-01-01 00:00:00", 1114),
        ("1900-02-29 00:00:00", 1114),
        ("2000-13-01 00:00:00", 1114),
        ("2000-01-32 00:00:00", 1114),
        ("2000-01-01 24:00:00", 1114),
        ("2000-01-01 00:60:00", 1114),
        ("2000-01-01 00:00:00.1234567", 1114),
        ("2000-01-01 00:00:00 America/Los_Angeles", 1184),
        ("2000-01-01 00:00:00+16", 1184),
        ("2000-01-01 00:00:00+00:60", 1184),
        ("2000-01-01 00:00:00+00:00:60", 1184),
        ("2000-01-01 00:00:00+00:00:00:00", 1184),
        ("294277-01-01 00:00:00", 1114),
        ("4714-11-23 00:00:00 BC", 1114),
        ("2000-01-01 00:00:00", 20),
    ] {
        assert!(Timestamp::parse_literal(text, oid).is_err(), "{text}");
    }
    assert!(Timestamp::parse("2000-01-01", 1114).is_err());
    assert!(Timestamp::parse("2000-01-01 00:00:00+00", 1114).is_err());
    assert!(Timestamp::parse("2000-01-01 00:00:00", 1184).is_err());
    assert!(Timestamp(0).json(20).is_err());
}

#[test]
fn bins_match_independent_floor_and_postgres_checked_boundaries() -> Result<()> {
    for stride in [1, 10, 1_000_000, 10_000_000, super::DAY] {
        for origin in [-1, 0, 7] {
            for value in [-10_000_001, -10_000_000, -1, 0, 1, 10_000_000] {
                let expected = i128::from(origin)
                    + (i128::from(value) - i128::from(origin)).div_euclid(i128::from(stride))
                        * i128::from(stride);
                assert_eq!(
                    Timestamp(value).bin(stride, Timestamp(origin))?.sort_value(),
                    i64::try_from(expected)?
                );
            }
        }
    }
    assert!(Timestamp(super::END - 1).bin(1, Timestamp(super::MIN)).is_err());
    assert!(Timestamp(super::MIN).bin(super::DAY, Timestamp(1)).is_err());
    assert!(Timestamp(0).bin(0, Timestamp(0)).is_err());
    assert!(Timestamp(0).bin(1, Timestamp(i64::MAX)).is_err());
    assert_eq!(Timestamp(i64::MAX).bin(10, Timestamp(0))?, Timestamp(i64::MAX));
    assert_eq!(Timestamp(i64::MIN).bin(10, Timestamp(0))?, Timestamp(i64::MIN));
    Ok(())
}

#[test]
fn timestamp_difference_preserves_pg_infinity_and_overflow_rules() -> Result<()> {
    use super::Interval;
    let finite = Timestamp::parse("2000-01-01 00:00:00", 1114)?;
    let later = Timestamp::parse("2000-01-01 00:00:10.000001", 1114)?;
    let negative = Timestamp::parse("-infinity", 1114)?;
    let positive = Timestamp::parse("infinity", 1114)?;
    assert_eq!(later.difference(finite)?, Interval::duration(10_000_001));
    assert_eq!(finite.difference(later)?, Interval::duration(-10_000_001));
    for (left, right, expected) in [
        (positive, finite, Interval::PositiveInfinity),
        (finite, negative, Interval::PositiveInfinity),
        (negative, finite, Interval::NegativeInfinity),
        (finite, positive, Interval::NegativeInfinity),
        (positive, negative, Interval::PositiveInfinity),
        (negative, positive, Interval::NegativeInfinity),
    ] {
        assert_eq!(left.difference(right)?, expected);
    }
    assert!(positive.difference(positive).is_err());
    assert!(negative.difference(negative).is_err());
    let min = Timestamp::parse("4714-11-24 00:00:00 BC", 1114)?;
    let max = Timestamp::parse("294276-12-31 23:59:59.999999", 1114)?;
    assert!(max.difference(min).is_err());
    assert!(min.difference(max).is_err());
    assert_eq!(finite.difference(finite)?, Interval::duration(0));
    Ok(())
}

#[test]
fn native_dates_intervals_and_calendar_edges() -> Result<()> {
    use super::{Date, Interval, extract, truncate};
    for text in
        ["4714-11-24 BC", "0001-01-01 BC", "2000-02-29", "5874897-12-31", "infinity", "-infinity"]
    {
        assert_eq!(Date::parse(text)?.text(), text);
    }
    assert!(Date::parse("5874897-12-31")?.timestamp().is_err());
    let components = Interval::parse("P13M-2DT90000.123456S")?;
    assert_eq!(Interval::parse(&components.text())?.text(), components.text());
    assert_eq!(Interval::parse("1 month")?, Interval::parse("30 days")?);
    assert_ne!(Interval::parse("1 month")?.text(), Interval::parse("30 days")?.text());
    assert!(Interval::finite(i32::MAX, i32::MAX, i64::MAX).is_err());
    for (field, text, oid, expected) in [
        ("year", "0001-01-01 BC", 1082, Some("-1")),
        ("century", "0001-01-01 BC", 1082, Some("-1")),
        ("month", "infinity", 1114, None),
        ("epoch", "infinity", 1186, Some("Infinity")),
        ("quarter", "-4 months", 1186, Some("0")),
        ("epoch", "1 year 1 month 1 day 0.000001 second", 1186, Some("34236000.000001")),
        ("epoch", "294276-12-31 23:59:59.999999", 1114, Some("9224318016000.000000")),
    ] {
        assert_eq!(extract(field, text, oid)?.as_deref(), expected, "{field}/{text}");
    }
    for (text, expected) in
        [("0002-01-01 BC", "-1"), ("0011-01-01 BC", "-1"), ("0012-01-01 BC", "-2")]
    {
        assert_eq!(extract("decade", text, 1082)?.as_deref(), Some(expected));
    }
    assert_eq!(truncate("week", "2021-01-01 12:00:00", 1114)?, "2020-12-28 00:00:00");
    let later = Timestamp::parse("2000-01-03 01:00:00", 1114)?;
    let earlier = Timestamp::parse("2000-01-01 02:00:00", 1114)?;
    assert_eq!(later.difference(earlier)?.text(), "P0M1DT23H0.000000S");
    Ok(())
}

#[test]
fn interval_text_keeps_exact_microseconds_through_parsing() -> Result<()> {
    use super::Interval;
    for (months, days, micros, text) in [
        (0, 0, i64::MAX, "P0M0DT2562047788H54.775807S"),
        (0, 0, i64::MIN, "P0M0DT-2562047788H-54.775808S"),
        (1, -2, -1, "P1M-2DT0H-0.000001S"),
        (0, 0, 0, "P0M0DT0H0.000000S"),
    ] {
        assert_eq!(Interval::finite(months, days, micros)?.text(), text);
        let Interval::Finite { months: parsed_months, days: parsed_days, micros: parsed } =
            Interval::parse(text)?
        else {
            anyhow::bail!("finite interval text parsed as infinity");
        };
        assert_eq!((parsed_months, parsed_days, parsed), (months, days, micros));
    }
    Ok(())
}

#[test]
fn calendar_units_null_infinity_and_overflow_are_closed() -> Result<()> {
    use super::Interval;
    for invalid in [
        "",
        "P",
        "PT",
        "P1YT",
        "P1M2M",
        "P1Q",
        "1",
        "1 fortnight",
        "0.0000001 seconds",
        "1 day 00:99:00",
        "2147483648 months",
        "9223372036854775808 microseconds",
    ] {
        assert!(Interval::parse(invalid).is_err(), "{invalid}");
    }
    for literal in [
        "1 year 2 mons 3 days 04:05:06.123456",
        "-25:00:00",
        "1 week",
        "2 milliseconds",
        "3 microseconds",
        "-infinity",
        "infinity",
    ] {
        let value = Interval::parse(literal)?;
        assert_eq!(Interval::parse(&value.text())?, value);
    }
    Ok(())
}
#[test]
fn calendar_all_supported_fields_and_units() -> Result<()> {
    use super::{extract, truncate};
    for field in [
        "year",
        "month",
        "day",
        "quarter",
        "week",
        "isoyear",
        "dow",
        "isodow",
        "doy",
        "decade",
        "century",
        "millennium",
        "julian",
        "epoch",
        "hour",
        "minute",
        "second",
        "milliseconds",
        "microseconds",
        "timezone",
        "timezone_hour",
        "timezone_minute",
    ] {
        extract(field, "2024-02-29 12:34:56.123456+00", 1184)?;
    }
    for field in [
        "microseconds",
        "milliseconds",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "decade",
        "century",
        "millennium",
    ] {
        truncate(field, "2024-02-29 12:34:56.123456", 1114)?;
        if field != "week" {
            truncate(field, "-13 months -2 days -25 hours -2.123456 seconds", 1186)?;
        }
    }
    for field in [
        "year",
        "month",
        "day",
        "quarter",
        "decade",
        "century",
        "millennium",
        "hour",
        "minute",
        "second",
        "milliseconds",
        "microseconds",
        "epoch",
    ] {
        extract(field, "-13 months -2 days -25 hours -2.123456 seconds", 1186)?;
    }
    Ok(())
}
#[test]
fn calendar_invalid_fields_and_arithmetic_boundaries() -> Result<()> {
    use super::{Date, Interval, extract, truncate};
    assert!(extract("timezone", "2000-01-01", 1082).is_err());
    assert!(truncate("week", "1 month", 1186).is_err());
    assert!(Date::infinity().difference(Date::infinity()).is_err());
    assert!(Date::parse("5874897-12-31")?.add_days(1).is_err());
    assert!(Interval::PositiveInfinity.binary(Interval::NegativeInfinity, false).is_err());
    assert_eq!(
        Interval::PositiveInfinity.binary(Interval::duration(i64::MIN), true)?,
        Interval::PositiveInfinity
    );
    assert!(Interval::duration(i64::MIN).negate().is_err());
    assert!(Interval::finite(i32::MAX, 0, 0)?.binary(Interval::parse("1 month")?, false).is_err());
    assert_eq!(
        extract("epoch", "294247-01-10 04:00:54.775807", 1114)?.as_deref(),
        Some("9223372036854.775800")
    );
    Ok(())
}

#[test]
fn calendar_day_addition_checks_the_result_after_adding_the_offset() -> Result<()> {
    let base = Timestamp::from_micros(super::MIN)?;
    let offset = super::Interval::finite(0, 108_000_000, 0)?;
    assert_eq!(base.add_interval(offset)?.sort_value(), 9_119_386_512_000_000_000);
    Ok(())
}
