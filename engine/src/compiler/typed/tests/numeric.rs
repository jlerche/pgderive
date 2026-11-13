use super::expression;
use crate::transaction::Row;
use anyhow::{Context, Result};
#[test]
fn exact_numeric_scalar_functions_casts_and_division_use_shared_kernels() -> Result<()> {
    let row = Row::from([("a".into(), Some("7".into()))]);
    for (sql, expected) in [
        ("a/2", "3"),
        ("a::numeric/2", "3.5000000000000000"),
        ("round(2.5)", "3"),
        ("trunc(-2.5)", "-2"),
        ("ceil(-2.5)", "-2"),
        ("floor(-2.5)", "-3"),
        ("round(a::smallint,-1)", "10"),
        ("trunc(a::smallint,1)", "7.0"),
        ("2.5::smallint", "3"),
        ("-2.5::integer", "-3"),
        ("a::numeric(5,2)", "7.00"),
        ("1499::numeric(2,-3)", "1000"),
        ("0.00994::numeric(2,4)", "0.0099"),
        ("abs('-Infinity'::numeric)", "Infinity"),
        ("'NaN'::numeric/0", "NaN"),
        ("CASE WHEN a>0 THEN 1::numeric ELSE a::numeric/0 END", "1"),
        ("COALESCE(a::numeric,a::numeric/0)", "7"),
    ] {
        assert_eq!(
            expression(sql)
                .with_context(|| sql.to_owned())?
                .evaluate(&row)
                .with_context(|| sql.to_owned())?
                .as_deref(),
            Some(expected),
            "{sql}"
        );
    }
    for sql in ["NULL::numeric/0", "round(NULL::numeric)", "round(1.5,NULL)", "NULL::numeric(2,1)"]
    {
        assert_eq!(
            expression(sql)
                .with_context(|| sql.to_owned())?
                .evaluate(&row)
                .with_context(|| sql.to_owned())?,
            None,
            "{sql}"
        );
    }
    for sql in [
        "round(a)",
        "round(NULL)",
        "ceil(a)",
        "trunc(a)",
        "1::numeric/0",
        "a::numeric/0",
        "32767.5::smallint",
        "999.995::numeric(5,2)",
        "'NaN'::numeric::integer",
        "1::numeric(0)",
    ] {
        assert!(expression(sql).and_then(|value| value.evaluate(&row)).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn integral_division_truncates_toward_zero_and_checks_every_width() -> Result<()> {
    let row = Row::from([("a".into(), Some("7".into()))]);
    for (sql, expected) in [("(-a)/2", "-3"), ("a/(-2)", "-3"), ("(-a)/(-2)", "3")] {
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), Some(expected));
    }
    for (kind, min, max) in [
        ("smallint", "-32768", "32767"),
        ("integer", "-2147483648", "2147483647"),
        ("bigint", "-9223372036854775808", "9223372036854775807"),
    ] {
        for value in [min, max] {
            let sql = format!("('{value}'::numeric)::{kind}/(1::{kind})");
            assert_eq!(expression(&sql)?.evaluate(&row)?.as_deref(), Some(value));
        }
        for (divisor, error) in [("0", "division by zero"), ("-1", "overflow")] {
            let sql = format!("('{min}'::numeric)::{kind}/('{divisor}'::numeric)::{kind}");
            let result = expression(&sql)
                .and_then(|value| value.evaluate(&row))
                .err()
                .context("invalid division succeeded")?;
            assert!(format!("{result:#}").contains(error), "{sql}: {result:#}");
        }
    }
    Ok(())
}
