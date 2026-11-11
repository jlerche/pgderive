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
        syntax::predicate(select.where_clause.as_deref())?.context("WHERE absent")?.as_ref(),
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
        "a IS TRUE",
        "NULLIF(a, false)",
        "b IS UNKNOWN",
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
    assert_eq!(expression("a AND (b AND a)")?, expression("a AND b")?);
    assert_eq!(expression("a OR (b OR a)")?, expression("a OR b")?);
    assert_ne!(expression("a AND b")?, expression("b AND a")?);
    assert_eq!(expression("a AND a")?, expression("a")?);
    assert_eq!(expression("NOT (n IS NOT DISTINCT FROM NULL)")?, expression("n IS NOT NULL")?);
    assert_eq!(expression("NOT (n IS DISTINCT FROM 1)")?, expression("n IS NOT DISTINCT FROM 1")?);
    for sql in [
        "n",
        "text < 'x'",
        "text = text",
        "a = 1",
        "n = true",
        "n = u",
        "u = 'bad'",
        "u = '0000--0000000000000000000000000000'",
        "u = '00000000000000000000000000000000-'",
        "u = '0-0000000000000000000000000000000'",
        "n BETWEEN SYMMETRIC 1 AND 2",
        "n IS DISTINCT FROM true",
        "true IS NULL",
        "ROW(a) IS NULL",
        "round(n)>0",
        "n IS TRUE",
        "n OPERATOR(pg_catalog.=) 1",
    ] {
        assert!(expression(sql).is_err(), "accepted {sql}");
    }
    let one = [("n".into(), Some("1".into()))].into();
    assert_eq!(expression("n='1'")?.evaluate((&one, &one))?, Some(true));
    assert_eq!(expression("1=1")?.evaluate((&one, &one))?, Some(true));
    assert!(expression(&format!("{}a", "NOT ".repeat(66))).is_err());
    Ok(())
}

#[test]
fn postgres_remainder_sign_null_and_minimum_integer() -> Result<()> {
    let cases = [(-7_i64, 3_i64, -1_i64), (7, -3, 1), (i64::MIN, -1, 0), (i64::MIN, 3, -2)];
    for (value, divisor, expected) in cases {
        let row = [("big".into(), Some(value.to_string()))].into();
        let function = expression(&format!("pg_catalog.mod(big,{divisor})={expected}"))?;
        let operator = expression(&format!("big % {divisor}={expected}"))?;
        assert_eq!(function, operator);
        assert_eq!(function.evaluate((&row, &row))?, Some(true));
        let row = [("big".into(), None)].into();
        assert_eq!(function.evaluate((&row, &row))?, None);
    }
    for sql in [
        "mod(n,0)=0",
        "mod(n,NULL)=0",
        "mod(n,a)=0",
        "mod(text,3)=0",
        "public.mod(n,3)=0",
        "mod(DISTINCT n,3)=0",
        "mod(n,3) OVER ()=0",
        "mod(mod(n,3),2)=0",
        "mod(n,3)",
    ] {
        assert!(expression(sql).is_err(), "accepted {sql}");
    }
    Ok(())
}

#[test]
fn deterministic_text_equality_preserves_bytes_and_unknown() -> Result<()> {
    let empty = Row::new();
    for text in [Some("OR"), Some("or"), Some("OR "), Some("é"), Some("e\u{301}"), Some(""), None]
    {
        let row = [("text".into(), text.map(str::to_owned))].into();
        for literal in ["OR", "é", ""] {
            let equal = expression(&format!("text = '{literal}'"))?;
            let reverse = expression(&format!("'{literal}' = text"))?;
            let different = expression(&format!("text <> '{literal}'"))?;
            assert_eq!(equal.evaluate((&row, &empty))?, text.map(|value| value == literal));
            assert_eq!(equal.evaluate((&row, &empty))?, reverse.evaluate((&row, &empty))?);
            assert_eq!(different.evaluate((&row, &empty))?, text.map(|value| value != literal));
        }
        assert_eq!(expression("text = NULL")?.evaluate((&row, &empty))?, None);
    }
    Ok(())
}

#[path = "tests/scalar.rs"]
mod scalar;

#[path = "tests/using.rs"]
mod using;

mod join_predicate;

#[test]
fn simple_case_prunes_unreachable_aggregate_results_and_guards_subqueries() -> Result<()> {
    let contract = crate::source::tests::contract();
    super::compile("SELECT SUM(CASE 1 WHEN 0 THEN 1 / 0 ELSE 2 END) FROM public.items", &contract)?;
    for value in [
        "CASE (SELECT 1) WHEN NULL THEN 1 ELSE 2 END",
        "CASE NULL::integer WHEN (SELECT 1) THEN 1 ELSE 2 END",
    ] {
        let sql = format!("SELECT {value} FROM public.items");
        let Err(error) = super::compile(&sql, &contract) else {
            anyhow::bail!("unexpectedly admitted {sql}");
        };
        assert!(
            format!("{error:#}").contains("subqueries in lazy CASE/COALESCE"),
            "{sql}: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn nullif_guards_only_the_subquery_that_folding_can_discard() -> Result<()> {
    let contract = crate::source::tests::contract();
    let compiled = super::compile(
        "SELECT NULLIF((SELECT MAX(s.id) FROM public.items s), NULL) FROM public.items i",
        &contract,
    )?;
    assert_eq!(compiled.output().columns[0].label, "nullif");
    let Err(error) =
        super::compile("SELECT NULLIF(NULLIF(1, 1), (SELECT 1)) FROM public.items", &contract)
    else {
        anyhow::bail!("unexpectedly lifted a guarded NULLIF operand");
    };
    assert!(format!("{error:#}").contains("subqueries in lazy CASE/COALESCE/NULLIF"));
    Ok(())
}

#[test]
fn between_probe_expansion_is_bounded() {
    let sql = (0..15).fold("a".to_owned(), |sql, _| format!("({sql}) BETWEEN false AND true"));
    assert!(expression(&sql).is_err_and(|error| error.to_string().contains("expansion budget")));
}

#[test]
fn between_folds_qualifiers_in_written_order_before_sorting() -> Result<()> {
    let contract = crate::source::tests::contract();
    let sql = "SELECT id FROM public.items WHERE 1 BETWEEN (2147483647 + 1) + id AND 1 / 0";
    let Err(error) = super::compile(sql, &contract) else {
        anyhow::bail!("unexpectedly admitted {sql}");
    };
    assert!(format!("{error:#}").contains("integer overflow"), "{error:#}");
    Ok(())
}
