//! Shared-IR regression boundaries, independent of any durable registration.
use super::{ColumnRef, Expr, Row, expression};
use crate::compiler::{
    parser::expressions::{node, target},
    scalar, syntax, typed,
};
use anyhow::{Context, Result};
use pg_query::NodeEnum;
use std::cell::Cell;

#[test]
fn projection_and_predicate_bind_the_same_scalar_once() -> Result<()> {
    let query = pg_query::parse("SELECT abs(n) WHERE abs(n)>abs(big)")?;
    let statement = query
        .protobuf
        .stmts
        .first()
        .and_then(|statement| statement.stmt.as_ref())
        .context("statement absent")?;
    let NodeEnum::SelectStmt(select) = node(statement)? else { anyhow::bail!("SELECT absent") };
    let visits = Cell::new(0);
    let resolve = |name: &crate::compiler::parser::Name| -> Result<ColumnRef> {
        visits.set(visits.get() + 1);
        let name = name.0.last().context("name absent")?.clone();
        Ok(ColumnRef {
            oid: if name == "big" { 20 } else { 23 },
            name,
            right: false,
            nullable: true,
        })
    };
    let bound = crate::compiler::expression::bind(
        syntax::predicate(select.where_clause.as_deref())?.context("WHERE absent")?.as_ref(),
        &resolve,
    )?;
    assert_eq!(visits.get(), 2, "each predicate operand must bind once");
    let Expr::Compare(_, left, right) = bound else { anyhow::bail!("comparison absent") };
    let mut computed = Vec::new();
    let ast = target(select.target_list.first().context("target absent")?)?;
    let projection = typed::bind(ast, &resolve, 0)?;
    assert_eq!(projection, left);
    let first = scalar::register(projection.clone(), &mut computed, "@test_")?;
    let second = scalar::register(left, &mut computed, "@test_")?;
    assert_eq!(first, second);
    assert_eq!(computed.len(), 1, "one scope must intern identical maps");
    let row = Row::from([("n".into(), Some("-7".into())), ("big".into(), Some("-2".into()))]);
    assert_eq!(computed[0].expression.evaluate(&row)?.as_deref(), Some("7"));
    assert_eq!(
        projection.native_pair((&row, &row))?,
        Some(crate::compiler::value::Value::Integer(7))
    );
    assert_eq!(right.oid(), 20);
    Ok(())
}
#[test]
fn native_literals_and_function_aliases_keep_normalized_identities() -> Result<()> {
    assert_eq!(
        expression("u='A0EEBC999C0B4EF8BB6D6BB9BD380A11'")?,
        expression("u='{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'")?
    );
    assert_eq!(expression("length(text)>1")?, expression("character_length(text)>1")?);
    let row = Row::from([("n".into(), Some(i32::MAX.to_string()))]);
    assert_eq!(expression("n<2147483648")?.evaluate((&row, &row))?, Some(true));
    assert_eq!(expression("n>-2147483649")?.evaluate((&row, &row))?, Some(true));
    Ok(())
}
#[test]
fn unsupported_overloads_explain_types_and_available_signatures() -> Result<()> {
    let error = expression("abs(n::numeric)>1").err().context("unsupported overload accepted")?;
    let error = format!("{error:#}");
    assert!(error.contains("abs(numeric [OID 1700])"), "{error}");
    assert!(error.contains("supported signatures: abs(smallint [OID 21])"), "{error}");
    assert!(error.contains("abs(integer [OID 23])"), "{error}");
    Ok(())
}
