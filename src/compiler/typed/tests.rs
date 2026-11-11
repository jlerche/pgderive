use super::{Typed, bind};
use crate::compiler::{
    ColumnRef,
    parser::expressions::{node, target},
};
use anyhow::{Context, Result};
use pg_query::NodeEnum;
pub(super) fn expression(sql: &str) -> Result<Typed> {
    let parsed = pg_query::parse(&format!("SELECT {sql}"))?;
    let statement = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|value| value.stmt.as_ref())
        .context("missing statement")?;
    let NodeEnum::SelectStmt(statement) = node(statement)? else {
        anyhow::bail!("missing SELECT");
    };
    bind(
        target(statement.target_list.first().context("missing target")?)?,
        &|name| {
            Ok(ColumnRef {
                name: name.0.last().context("missing name")?.clone(),
                right: false,
                oid: 23,
                nullable: true,
            })
        },
        0,
    )?
    .fold()
}
#[test]
fn typed_integral_widths_nulls_and_lazy_coalesce() -> Result<()> {
    let row =
        [("a".into(), Some("7".into())), ("b".into(), None), ("c".into(), Some("32768".into()))]
            .into();
    for (sql, expected) in [
        ("a + 2", Some("9")),
        ("CASE WHEN a > 1 THEN a * 2 ELSE b END", Some("14")),
        ("CASE WHEN b > 1 THEN a * 2 END", None),
        ("a - 2", Some("5")),
        ("a * 2", Some("14")),
        ("a::smallint", Some("7")),
        ("NULL::bigint", None),
        ("COALESCE(b,a)", Some("7")),
        ("COALESCE(a, c::smallint)", Some("7")),
        ("b * a", None),
    ] {
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    for sql in ["32768::smallint", "2147483647 + 1", "COALESCE(b,32768::smallint)"] {
        assert!(expression(sql).and_then(|value| value.evaluate(&row)).is_err(), "{sql}");
    }
    for sql in ["COALESCE(NULL,NULL)", "random()", "a ^ 2", "NULL + NULL"] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn typed_predicates_preserve_null_unknown_and_width_errors() -> Result<()> {
    let row =
        [("a".into(), Some("7".into())), ("b".into(), None), ("c".into(), Some("32768".into()))]
            .into();
    for (sql, expected) in [
        ("a + 2 = 9", Some(true)),
        ("COALESCE(b,a) > 5", Some(true)),
        ("b::smallint IS NULL", Some(true)),
        ("b * a > 1", None),
    ] {
        let parsed = pg_query::parse(&format!("SELECT a WHERE {sql}"))?;
        let statement = parsed
            .protobuf
            .stmts
            .first()
            .and_then(|value| value.stmt.as_ref())
            .context("missing statement")?;
        let NodeEnum::SelectStmt(statement) = node(statement)? else {
            anyhow::bail!("missing SELECT");
        };
        let predicate = crate::compiler::syntax::predicate(statement.where_clause.as_deref())?
            .context("missing predicate")?;
        let predicate = crate::compiler::expression::bind(&predicate, &|name| {
            Ok(ColumnRef {
                name: name.0.last().context("missing name")?.clone(),
                right: false,
                oid: 23,
                nullable: true,
            })
        })?;
        assert_eq!(predicate.evaluate((&row, &row))?, expected, "{sql}");
    }
    Ok(())
}

#[test]
fn exact_numeric_computation_preserves_scale_and_special_values() -> Result<()> {
    let row = [("a".into(), Some("7".into()))].into();
    for (sql, expected) in [
        ("a::numeric * 1.20", "8.40"),
        ("1.20 + 2.3", "3.50"),
        ("1.20 - 2.3", "-1.10"),
        ("'NaN'::numeric + 1", "NaN"),
        ("'Infinity'::numeric * 0", "NaN"),
        ("COALESCE(NULL::numeric, 1.20)", "1.20"),
    ] {
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), Some(expected), "{sql}");
    }
    for sql in ["1.2 / 0", "32767.5::smallint", "999.995::numeric(5,2)"] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[path = "tests/functions.rs"]
mod functions;

#[path = "tests/numeric.rs"]
mod numeric;

#[test]
fn calendar_functions_and_arithmetic_use_native_kernels() -> Result<()> {
    let row = crate::transaction::Row::new();
    for (sql, expected) in [
        ("EXTRACT(second FROM TIMESTAMP '2024-02-29 12:34:56.123456')", "56.123456"),
        ("EXTRACT(milliseconds FROM INTERVAL '25 hours 2.123456 seconds')", "2123.456"),
        ("EXTRACT(hour FROM INTERVAL '25 hours')", "25"),
        ("EXTRACT(epoch FROM DATE '2000-01-01')", "946684800"),
        ("EXTRACT(epoch FROM TIMESTAMP '2000-01-01 00:00:00')", "946684800.000000"),
        ("EXTRACT(week FROM DATE '2021-01-01')", "53"),
        ("EXTRACT(isoyear FROM DATE '2021-01-01')", "2020"),
        ("DATE '2024-02-28' + 2", "2024-03-01"),
        ("DATE '2024-03-01' - DATE '2024-02-28'", "2"),
        ("TIMESTAMP '2024-01-31 12:00:00' + INTERVAL '1 month'", "2024-02-29 12:00:00"),
        ("date_trunc('month', TIMESTAMP '2024-02-29 12:34:56')", "2024-02-01 00:00:00"),
        ("date_trunc('month', DATE '2024-02-29')", "2024-02-01 00:00:00+00"),
        ("(INTERVAL '1 month' = INTERVAL '30 days')", "t"),
        ("DATE '2024-02-29' = TIMESTAMP '2024-02-29 00:00:00'", "t"),
        ("DATE '5874897-12-31' > TIMESTAMP '294276-12-31 00:00:00'", "t"),
        ("TIMESTAMPTZ 'infinity' > DATE '5874897-12-31'", "t"),
        ("DATE 'infinity' = TIMESTAMP 'infinity'", "t"),
        ("TIMESTAMP '2000-01-01 12:00:00' = DATE '2000-01-01'", "f"),
    ] {
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), Some(expected), "{sql}");
    }
    for sql in [
        "date_trunc('year',NULL)",
        "EXTRACT(year FROM NULL)",
        "DATE '2000-01-01'+NULL",
        "DATE '2000-01-01'+1::bigint",
        "date_trunc('day',TIMESTAMPTZ '2000-01-01 00:00:00+00','America/New_York')",
        "date_part('year',DATE '2000-01-01')",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn boolean_tests_are_total_and_evaluate_their_input() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (input, expected) in [
        ("a > 0", ["t", "f", "f", "t", "f", "t"]),
        ("a < 0", ["f", "t", "t", "f", "f", "t"]),
        ("b > 0", ["f", "t", "f", "t", "t", "f"]),
        ("NULL", ["f", "t", "f", "t", "t", "f"]),
        ("'yes'", ["t", "f", "f", "t", "f", "t"]),
    ] {
        for (test, expected) in ["TRUE", "NOT TRUE", "FALSE", "NOT FALSE", "UNKNOWN", "NOT UNKNOWN"]
            .into_iter()
            .zip(expected)
        {
            let sql = format!("({input}) IS {test}");
            let value = expression(&sql)?;
            assert_eq!(value.oid(), 16, "{sql}");
            assert!(!value.nullable(), "{sql}");
            assert_eq!(value.evaluate(&row)?.as_deref(), Some(expected), "{sql}");
        }
    }
    assert_eq!(expression("NOT ((a > 0) IS NOT TRUE)")?, expression("(a > 0) IS TRUE")?);
    assert!(expression("a IS TRUE").is_err());
    assert!(expression("(1 / 0 = 1) IS UNKNOWN").is_err());
    assert!(expression("(1 / (a - 7) = 1) IS UNKNOWN")?.evaluate(&row).is_err());
    Ok(())
}

#[test]
fn distinctness_is_total_and_reuses_native_equality() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (left, right, distinct) in [
        ("a", "7::bigint", false),
        ("a", "8", true),
        ("a", "7.000", false),
        ("7.000", "a", false),
        ("a", "'7'", false),
        ("a", "b", true),
        ("b", "a", true),
        ("b", "b", false),
        ("NULL", "a", true),
        ("b", "NULL", false),
        ("NULL", "NULL", false),
        ("1", "NULL", true),
        ("NULL::integer", "NULL::bigint", false),
        ("'NaN'::numeric", "'NaN'::numeric", false),
        ("DATE '2020-01-01'", "TIMESTAMP '2020-01-01 00:00:00'", false),
        ("'é'::text", "'e'", true),
        ("TRUE", "'yes'", false),
    ] {
        for (test, expected) in [("DISTINCT", distinct), ("NOT DISTINCT", !distinct)] {
            let sql = format!("{left} IS {test} FROM {right}");
            let value = expression(&sql)?;
            assert_eq!(value.oid(), 16, "{sql}");
            assert!(!value.nullable(), "{sql}");
            assert_eq!(
                value.evaluate(&row)?.as_deref(),
                Some(if expected { "t" } else { "f" }),
                "{sql}"
            );
        }
    }
    for sql in [
        "a IS DISTINCT FROM TRUE",
        "ROW(a) IS DISTINCT FROM ROW(b)",
        "NULL IS DISTINCT FROM (1 / 0)",
        "(1 / 0) IS NOT DISTINCT FROM NULL::integer",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    for sql in [
        "NULL IS DISTINCT FROM (1 / (a - 7))",
        "b IS DISTINCT FROM (1 / (a - 7))",
        "(1 / (a - 7)) IS NOT DISTINCT FROM b",
    ] {
        assert!(expression(sql)?.evaluate(&row).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn simple_case_resolves_matches_and_results_independently() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (sql, oid, expected) in [
        ("CASE a WHEN 7.0 THEN '7.00' ELSE 1.5 END", 1700, Some("7.00")),
        ("CASE a WHEN '7' THEN 1 ELSE 2147483648 END", 20, Some("1")),
        ("CASE a WHEN b THEN 1 WHEN a THEN 2 WHEN a THEN 3 END", 23, Some("2")),
        ("CASE b WHEN NULL THEN 1 WHEN b THEN 2 ELSE 3 END", 23, Some("3")),
        ("CASE b WHEN NULL THEN 1 END", 23, None),
        ("CASE NULL WHEN NULL THEN 1 / 0 ELSE 2 END", 23, Some("2")),
        ("CASE 1 WHEN 1 THEN 7 WHEN 1 / 0 THEN 8 ELSE 9 / 0 END", 23, Some("7")),
        ("CASE a WHEN 7 THEN NULL END", 25, None),
        ("CASE 'yes' WHEN 'yes' THEN true ELSE 'no' END", 16, Some("t")),
        ("CASE a WHEN 7 THEN 'text' ELSE 'varchar'::varchar END", 1043, Some("text")),
        (
            "CASE a WHEN 7 THEN DATE '2020-01-01' ELSE TIMESTAMP '2020-01-02 00:00:00' END",
            1114,
            Some("2020-01-01 00:00:00"),
        ),
        ("CASE 1 WHEN a THEN 1 WHEN 1 THEN 2 ELSE 1 / 0 END", 23, Some("2")),
        ("CASE a WHEN 7 THEN 1 ELSE 1 / (a - 7) END", 23, Some("1")),
        ("CASE 1 / (a - 7) WHEN NULL THEN 1 ELSE 2 END", 23, Some("2")),
        ("CASE a WHEN 7 THEN 1 WHEN 1 / (a - 7) THEN 2 END", 23, Some("1")),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), oid, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    let typed = expression("CASE a WHEN 7 THEN 1::numeric(4,2) ELSE 2::numeric(4,2) END")?;
    assert_eq!(typed.modifier(&|_| None), crate::numeric::Typmod::new(4, 2)?.encoded());
    for sql in [
        "CASE NULL WHEN 1 THEN 2 END",
        "CASE '7' WHEN 7 THEN 2 END",
        "CASE a WHEN true THEN 2 END",
        "CASE a WHEN 7 THEN 'bad' ELSE 1 END",
        "CASE a WHEN 7 THEN true ELSE 1 END",
        "CASE NULL WHEN 1 / 0::numeric THEN 2 END",
        "CASE NULL::int WHEN 1 / 0 THEN 2 END",
        "CASE 1 / 0 WHEN NULL THEN 2 END",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    for sql in
        ["CASE b WHEN 1 / (a - 7) THEN 1 ELSE 2 END", "CASE 1 / (a - 7) WHEN a THEN 1 ELSE 2 END"]
    {
        assert!(expression(sql)?.evaluate(&row).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn nullif_returns_the_resolved_left_argument() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (sql, oid, expected) in [
        ("NULLIF(a, 7.00)", 1700, None),
        ("NULLIF(a, 8.00)", 1700, Some("7")),
        ("NULLIF(7.00, a + 1)", 1700, Some("7.00")),
        ("NULLIF(a, 8::bigint)", 23, Some("7")),
        ("NULLIF(7::smallint, a + 1)", 21, Some("7")),
        ("NULLIF(a, '7')", 23, None),
        ("NULLIF(a, b)", 23, Some("7")),
        ("NULLIF(b, a)", 23, None),
        ("NULLIF(b, NULL)", 23, None),
        ("NULLIF(NULL, NULL)", 25, None),
        ("NULLIF(NULL, a)", 23, None),
        ("NULLIF(a, NULL)", 23, Some("7")),
        ("NULLIF('é'::varchar, 'e')", 25, Some("é")),
        ("NULLIF('é'::varchar, 'é')", 25, None),
        ("NULLIF(TRUE, 'no')", 16, Some("t")),
        ("NULLIF('NaN'::numeric, 'NaN')", 1700, None),
        ("NULLIF(DATE '2020-01-01', TIMESTAMP '2020-01-02 00:00:00')", 1082, Some("2020-01-01")),
        ("NULLIF(NULL, 1 / (a - 7))", 23, None),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), oid, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    let typed = expression("NULLIF(7::numeric(4,2), a)")?;
    assert!(typed.nullable());
    assert_eq!(typed.modifier(&|_| None), crate::numeric::Typmod::new(4, 2)?.encoded());
    for sql in ["NULLIF(NULL, 1 / 0)", "NULLIF(1 / 0, NULL)", "NULLIF(a, TRUE)"] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    for sql in ["NULLIF(b, 1 / (a - 7))", "NULLIF(1 / (a - 7), b)"] {
        assert!(expression(sql)?.evaluate(&row).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn between_uses_independent_comparisons_and_three_valued_logic() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (sql, expected) in [
        ("a BETWEEN 7 AND 7::bigint", Some("t")),
        ("a BETWEEN 8 AND 6", Some("f")),
        ("a NOT BETWEEN 8 AND 6", Some("t")),
        ("a BETWEEN NULL AND 6", Some("f")),
        ("a NOT BETWEEN NULL AND 6", Some("t")),
        ("a BETWEEN NULL AND 7", None),
        ("a NOT BETWEEN 6 AND NULL", None),
        ("b BETWEEN 1 AND 9", None),
        ("b NOT BETWEEN 9 AND 1", None),
        ("'7' BETWEEN 7::smallint AND 7.01", Some("t")),
        ("a BETWEEN '7' AND 7.01", Some("t")),
        ("a BETWEEN 6.99 AND '7'", Some("t")),
        ("'NaN'::numeric BETWEEN 'Infinity'::numeric AND 'NaN'::numeric", Some("t")),
        (
            "DATE '2020-01-01' BETWEEN TIMESTAMP '2020-01-01 00:00:00' AND DATE '2020-01-02'",
            Some("t"),
        ),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 16, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    for sql in ["a BETWEEN false AND 6", "a BETWEEN 8 AND 'bad'", "a NOT BETWEEN SYMMETRIC 1 AND 2"]
    {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn text_concatenation_is_strict_and_has_text_result_type() -> Result<()> {
    let row = [("a".into(), Some("0".into())), ("b".into(), None)].into();
    for (sql, expected) in [
        ("'é' || '🙂' || '界'", Some("é🙂界")),
        ("''::text || 'text'", Some("text")),
        ("'text' || ''", Some("text")),
        ("NULL || 'text'", None),
        ("'text' || NULL::text", None),
        ("NULL || NULL", None),
        ("NULL::text || chr(a)", None),
        ("(CASE WHEN a = 0 THEN 'left' ELSE 'right' END) || ' side'", Some("left side")),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 25, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    for sql in [
        "'text' || 1",
        "'text'::varchar || 'suffix'",
        "'text' || ARRAY['suffix']",
        "NULL || chr(0)",
        "chr(0) || NULL",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    let value = expression("(CASE WHEN b IS NULL THEN NULL::text ELSE '' END) || chr(a)")?;
    assert!(value.evaluate(&row).is_err());
    Ok(())
}

#[test]
fn text_concatenation_checks_combined_utf8_bytes() -> Result<()> {
    let at_limit = expression("repeat('🙂', 4194303) || 'éé'")?;
    let value = at_limit.evaluate(&crate::transaction::Row::new())?.context("NULL text")?;
    assert_eq!(value.len(), super::TEXT_OUTPUT_LIMIT);
    assert!(value.ends_with("🙂éé"));
    assert!(
        expression("repeat('🙂', 4194303) || 'ééx'")
            .is_err_and(|error| format!("{error:#}").contains("concatenation output exceeds"))
    );
    Ok(())
}

#[test]
fn numeric_extrema_resolve_common_types_and_preserve_equal_representations() -> Result<()> {
    let row = [("a".into(), Some("7".into())), ("b".into(), None)].into();
    for (sql, oid, expected) in [
        ("GREATEST(NULL, NULL)", 25, None),
        ("LEAST(NULL, NULL::smallint)", 21, None),
        ("GREATEST(b, NULL::numeric)", 1700, None),
        ("GREATEST(NULL, a::smallint)", 21, Some("7")),
        ("LEAST(a::smallint, 8)", 23, Some("7")),
        ("GREATEST(a, NULL::bigint, 2147483648)", 20, Some("2147483648")),
        ("LEAST(b, a)", 23, Some("7")),
        ("LEAST(9)", 23, Some("9")),
        ("GREATEST('8', a, NULL)", 23, Some("8")),
        ("LEAST('7.000', a, 7.0)", 1700, Some("7.000")),
        ("GREATEST(a, 7.000)", 1700, Some("7")),
        ("GREATEST(7.000, a)", 1700, Some("7.000")),
        ("GREATEST('NaN'::numeric, 'Infinity', 42)", 1700, Some("NaN")),
        ("LEAST('NaN'::numeric, '-Infinity', 42)", 1700, Some("-Infinity")),
        ("LEAST('Infinity'::numeric, 42.00)", 1700, Some("42.00")),
        ("LEAST(' 7.00 ', 8.0)", 1700, Some("7.00")),
        ("GREATEST(' iNF ', 1.0)", 1700, Some("Infinity")),
        ("LEAST(' -inf '::numeric, 1.0)", 1700, Some("-Infinity")),
        ("GREATEST('nan'::numeric, 1.0)", 1700, Some("NaN")),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), oid, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    assert!(expression("GREATEST(b, NULL)")?.nullable());
    assert!(!expression("LEAST(b, a, 0)")?.nullable());
    for sql in [
        "GREATEST('a', 'b')",
        "LEAST(TRUE, 1)",
        "LEAST('+NaN', 1.0)",
        "GREATEST(32768, '40000'::smallint)",
        "LEAST(7, 'bad')",
        "GREATEST(NULL, 1 / 0)",
        "LEAST(-2147483648, 1 / 0)",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    let value = expression("GREATEST(2147483647, NULL, 1 / (a - 7))")?;
    assert!(value.evaluate(&row).is_err());
    Ok(())
}

#[test]
fn integral_bitwise_operators_preserve_width_and_signed_bits() -> Result<()> {
    let row = [("a".into(), Some("-2".into())), ("b".into(), None)].into();
    for (sql, oid, expected) in [
        ("a & 3", 23, Some("2")),
        ("1::smallint | 2::bigint", 20, Some("3")),
        ("1::smallint # '3'", 21, Some("2")),
        ("~1::smallint", 21, Some("-2")),
        ("~0::bigint", 20, Some("-1")),
        ("1::smallint << 15", 21, Some("-32768")),
        ("a >> 1", 23, Some("-1")),
        ("1::bigint << 63", 20, Some("-9223372036854775808")),
        ("'1' << 2", 23, Some("4")),
        ("a & b", 23, None),
        ("a << NULL", 23, None),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), oid, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    for sql in [
        "1 << 2::bigint",
        "1.0 & 2",
        "~NULL",
        "NULL & NULL",
        "NULL << NULL",
        "'1' << 2::smallint",
        "NULL::int & (1/0)",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[test]
fn shift_counts_match_postgres_x86_64() -> Result<()> {
    for (sql, expected) in [
        ("1::smallint << -1", "0"),
        ("1::smallint << 16", "0"),
        ("1::smallint << 32", "1"),
        ("1 << -1", "-2147483648"),
        ("1 << 33", "2"),
        ("1::bigint << 64", "1"),
        ("-2::bigint >> -1", "-1"),
    ] {
        assert_eq!(
            expression(sql)?.evaluate(&crate::transaction::Row::new())?.as_deref(),
            Some(expected),
            "{sql}"
        );
    }
    Ok(())
}
