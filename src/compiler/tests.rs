use super::{
    ColumnRef,
    expression::{self, Expr},
    syntax,
};
use crate::transaction::Row;
use anyhow::{Context, Result};
use pg_query::NodeEnum;
fn expression(sql: &str) -> Result<Expr> {
    let parsed = pg_query::parse(&format!("SELECT 1 WHERE {sql}"))?;
    let statement = parsed.protobuf.stmts.first().context("statement absent")?;
    let Some(NodeEnum::SelectStmt(select)) =
        statement.stmt.as_ref().and_then(|node| node.node.as_ref())
    else {
        anyhow::bail!("SELECT absent");
    };
    expression::bind(
        syntax::predicate(select.where_clause.as_deref())?.context("WHERE absent")?,
        &|name| {
            let name = name.0.last().context("column absent")?;
            let oid = match name.as_str() {
                "a" | "b" => 16,
                "n" => 23,
                "big" => 20,
                "u" => 2950,
                "text" => 25,
                _ => anyhow::bail!("unknown column"),
            };
            Ok(ColumnRef { right: false, name: name.clone(), oid, nullable: true })
        },
    )
}
fn rows(a: Option<&str>, b: Option<&str>) -> Row {
    [("a".into(), a.map(str::to_owned)), ("b".into(), b.map(str::to_owned))].into()
}
#[test]
fn complete_three_valued_truth_tables() -> Result<()> {
    let values = [Some("f"), Some("t"), None];
    let and = [
        [Some(false), Some(false), Some(false)],
        [Some(false), Some(true), None],
        [Some(false), None, None],
    ];
    let or = [
        [Some(false), Some(true), None],
        [Some(true), Some(true), Some(true)],
        [None, Some(true), None],
    ];
    let conjunction = expression("a AND b")?;
    let disjunction = expression("a OR b")?;
    let negate = expression("NOT a")?;
    for (i, a) in values.iter().enumerate() {
        for (j, b) in values.iter().enumerate() {
            let row = rows(*a, *b);
            assert_eq!(conjunction.evaluate((&row, &row))?, and[i][j]);
            assert_eq!(disjunction.evaluate((&row, &row))?, or[i][j]);
            assert_eq!(negate.evaluate((&row, &row))?, a.map(|value| value != "t"));
        }
    }
    Ok(())
}
#[test]
fn native_comparisons_literals_and_nulls() -> Result<()> {
    let mut row = rows(Some("t"), None);
    row.extend([
        ("n".into(), Some("-3".into())),
        ("big".into(), Some("9223372036854775807".into())),
        ("u".into(), Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())),
        ("text".into(), Some("arbitrary".into())),
    ]);
    for sql in [
        "a = true",
        "n < 0",
        "n <= -3",
        "n <> 0",
        "n >= -3",
        "big > n",
        "big = 9223372036854775807",
        "u = '{A0EEBC999C0B4EF8BB6D6BB9BD380A11}'",
        "u > '00000000-0000-0000-0000-000000000000'",
        "text IS NOT NULL",
        "b IS NULL",
        "NULL OR true",
        "NOT false",
    ] {
        assert_eq!(expression(sql)?.evaluate((&row, &row))?, Some(true), "{sql}");
    }
    for sql in ["b = true", "n = NULL", "NOT NULL", "NULL AND true"] {
        assert_eq!(expression(sql)?.evaluate((&row, &row))?, None, "{sql}");
    }
    assert_eq!(expression("a = false")?.evaluate((&row, &row))?, Some(false));
    assert!(expression("n < 0")?.evaluate((&Row::new(), &row)).is_err());
    row.insert("a".into(), Some("bad".into()));
    assert!(expression("a")?.evaluate((&row, &row)).is_err());
    row.insert("n".into(), Some("bad".into()));
    assert!(expression("n < 0")?.evaluate((&row, &row)).is_err());
    Ok(())
}
#[test]
fn normalization_and_fail_closed_types() -> Result<()> {
    assert_eq!(expression("a AND (b AND a)")?, expression("b AND a")?);
    assert_eq!(expression("a OR (b OR a)")?, expression("b OR a")?);
    assert_eq!(expression("a AND a")?, expression("a")?);
    for sql in [
        "n",
        "text = 'x'",
        "a = 1",
        "n = true",
        "n = u",
        "n = '1'",
        "1 = 1",
        "u = 'bad'",
        "u = '0000--0000000000000000000000000000'",
        "u = '00000000000000000000000000000000-'",
        "u = '0-0000000000000000000000000000000'",
        "n > 1.5",
        "big > 9223372036854775808",
        "n+1>0",
        "n BETWEEN 1 AND 2",
        "n IN (1,2)",
        "n IS DISTINCT FROM 1",
        "true IS NULL",
        "ROW(a) IS NULL",
        "n = CAST(1 AS integer)",
        "abs(n)>0",
        "a IS TRUE",
        "n OPERATOR(pg_catalog.=) 1",
    ] {
        assert!(expression(sql).is_err(), "accepted {sql}");
    }
    assert!(expression(&format!("{}a", "NOT ".repeat(66))).is_err());
    Ok(())
}

#[test]
fn projection_dispatch_preserves_grouped_v3_identity_bytes() -> Result<()> {
    #[derive(serde::Serialize)]
    struct Previous<'a> {
        selectors: &'a crate::worker::Query,
        predicates: &'a Option<Expr>,
        revision: &'a Option<String>,
    }
    let selectors = crate::worker::Query {
        left_schema: "source".into(),
        left_table: "left".into(),
        left_key: "id".into(),
        group: "group".into(),
        right_schema: "source".into(),
        right_table: "right".into(),
        right_key: "id".into(),
        sum: "value".into(),
    };
    let compiled = super::Compiled {
        program: super::Program::Grouped { selectors: selectors.clone() },
        predicates: Some(expression("a OR (n < 0 AND b)")?),
        revision: Some(super::REVISION.into()),
    };
    let previous = Previous {
        selectors: &selectors,
        predicates: &compiled.predicates,
        revision: &compiled.revision,
    };
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&previous)?);
    Ok(())
}
