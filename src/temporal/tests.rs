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
        ("2000-01-01", 1114),
        ("0000-01-01 00:00:00", 1114),
        ("1900-02-29 00:00:00", 1114),
        ("2000-13-01 00:00:00", 1114),
        ("2000-01-32 00:00:00", 1114),
        ("2000-01-01 24:00:00", 1114),
        ("2000-01-01 00:60:00", 1114),
        ("2000-01-01 00:00:00.1234567", 1114),
        ("2000-01-01 00:00:00+00", 1114),
        ("2000-01-01 00:00:00", 1184),
        ("2000-01-01 00:00:00 America/Los_Angeles", 1184),
        ("2000-01-01 00:00:00+16", 1184),
        ("2000-01-01 00:00:00+00:60", 1184),
        ("2000-01-01 00:00:00+00:00:60", 1184),
        ("2000-01-01 00:00:00+00:00:00:00", 1184),
        ("294277-01-01 00:00:00", 1114),
        ("4714-11-23 00:00:00 BC", 1114),
        ("2000-01-01 00:00:00", 20),
    ] {
        assert!(Timestamp::parse(text, oid).is_err(), "{text}");
    }
    assert!(Timestamp(0).json(20).is_err());
}
