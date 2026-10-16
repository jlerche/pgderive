use super::{Typed, bind};
use crate::compiler::{
    ColumnRef,
    parser::expressions::{node, target},
};
use anyhow::{Context, Result};
use pg_query::NodeEnum;
fn expression(sql: &str) -> Result<Typed> {
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
    )
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
    for sql in ["COALESCE(NULL,NULL)", "random()", "a / 2", "NULL + NULL"] {
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
        let predicate = crate::compiler::expression::bind(predicate, &|name| {
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
    for sql in ["1.2 / 2", "1.2::integer", "1.2::numeric(10,2)"] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    Ok(())
}
