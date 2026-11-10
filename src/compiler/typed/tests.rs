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
