use super::expression;
use crate::transaction::Row;
use anyhow::Result;

#[test]
fn varchar_casts_truncate_unicode_characters_and_keep_metadata() -> Result<()> {
    for (sql, modifier, expected) in [
        ("'é🙂界'::varchar(2)", 6, Some("é🙂")),
        ("CAST('a  x'::text AS character varying(3))", 7, Some("a  ")),
        ("'a'::varchar(3)", 7, Some("a")),
        ("'e\u{301}x'::varchar(1)", 5, Some("e")),
        ("'abcd'::varchar(2)::varchar(5)", 9, Some("ab")),
        ("'abcd'::varchar(2)::varchar", -1, Some("ab")),
        ("NULL::varchar(1)", 5, None),
        ("NULL::varchar(10485760)", 10_485_764, None),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 1043, "{sql}");
        assert_eq!(value.modifier(&|_| None), modifier, "{sql}");
        assert_eq!(value.collation(&|_| Ok(0))?, 100, "{sql}");
        assert_eq!(value.evaluate(&Row::new())?.as_deref(), expected, "{sql}");
    }
    for sql in ["'x'::varchar(0)", "'x'::varchar(10485761)", "1::varchar(2)"] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn temporal_casts_round_native_microseconds_and_keep_metadata() -> Result<()> {
    let interval = 0x7fff_0000;
    for (sql, oid, modifier, expected) in [
        ("timestamp(3) '1999-12-31 23:59:59.9995'", 1114, 3, Some("1999-12-31 23:59:59.999")),
        (
            "timestamp '2000-01-01 00:00:00.0005'::timestamp(3)",
            1114,
            3,
            Some("2000-01-01 00:00:00.001"),
        ),
        ("'2000-01-01 23:59:59.9995'::timestamp(3)", 1114, 3, Some("2000-01-02 00:00:00")),
        ("'294276-12-31 23:59:59.999999'::timestamp(0)", 1114, 0, Some("294277-01-01 00:00:00")),
        (
            "'1999-12-31 23:59:59.999501+00'::timestamptz(3)",
            1184,
            3,
            Some("2000-01-01 00:00:00+00"),
        ),
        (
            "'2000-01-01 00:00:00.123456-08'::timestamp(8) with time zone",
            1184,
            6,
            Some("2000-01-01 08:00:00.123456+00"),
        ),
        ("date '2000-01-01'::timestamp(1)", 1114, 1, Some("2000-01-01 00:00:00")),
        ("'infinity'::timestamptz(2)", 1184, 2, Some("infinity")),
        ("NULL::timestamp(3)", 1114, 3, None),
        ("interval(3) 'P1M2DT-23H-59M-59.9995S'", 1186, interval + 3, Some("P1M2DT-24H0.000000S")),
        ("'PT1.2345S'::interval(3)", 1186, interval + 3, Some("P0M0DT0H1.235000S")),
        ("'PT1.234567S'::interval(8)", 1186, interval + 6, Some("P0M0DT0H1.234567S")),
        ("'-infinity'::interval(0)", 1186, interval, Some("-infinity")),
        ("NULL::interval(2)", 1186, interval + 2, None),
        (
            "'9223372036854.775750 seconds'::interval(4)::interval(4)",
            1186,
            interval + 4,
            Some("P0M0DT2562047788H54.775800S"),
        ),
        (
            "'-9223372036854.775750 seconds'::interval(4)::interval(5)",
            1186,
            interval + 5,
            Some("P0M0DT-2562047788H-54.775800S"),
        ),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), oid, "{sql}");
        assert_eq!(value.modifier(&|_| None), modifier, "{sql}");
        assert_eq!(value.collation(&|_| Ok(0))?, 0, "{sql}");
        assert_eq!(value.evaluate(&Row::new())?.as_deref(), expected, "{sql}");
    }
    let sql = "(CASE WHEN true THEN '9223372036854.775750 seconds'::interval(4) ELSE NULL::interval(4) END)::interval(4)";
    assert_eq!(
        expression(sql)?.evaluate(&Row::new())?.as_deref(),
        Some("P0M0DT2562047788H54.775800S")
    );
    for sql in [
        "(CASE WHEN true THEN '9223372036854.775750 seconds'::interval(4) ELSE NULL END)::interval(4)",
        "NULL::pg_catalog.timestamp(-1)",
        "(TIMESTAMP(0) '294276-12-31 23:59:59.999999')::timestamptz(3)",
        "(TIMESTAMPTZ(0) '294276-12-31 23:59:59.999999+00')::timestamp(3)",
        "'9223372036854.775800 seconds'::interval::interval(4)",
        "NULL::interval day to second(3)",
        "'294277-01-01 00:00:00'::timestamp(0)",
        "'9223372036854.775807 seconds'::interval(0)",
        "CASE WHEN false THEN '9223372036854.775807 seconds'::interval(0) END",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}
