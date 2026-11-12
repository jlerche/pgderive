//! The scalar RHS is maintained independently, so its expressions must be safe without demand.
use super::super::expressions::{distinct_kind, node, optional};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn validate(select: &pg::SelectStmt, depth: usize) -> Result<()> {
    ensure!(depth < 16, "scalar RHS nesting exceeds limit");
    ensure!(
        select.with_clause.is_none()
            && select.group_clause.is_empty()
            && select.having_clause.is_none()
            && select.window_clause.is_empty()
            && select.from_clause.len() <= 1
            && select
                .from_clause
                .iter()
                .all(|source| matches!(source.node, Some(NodeEnum::RangeVar(_)))),
        "nonaggregate scalar RHS requires a native table or constant/set input"
    );
    for target in &select.target_list {
        let value = super::super::target(target)?;
        ensure!(plain(value)?, "nonaggregate scalar RHS requires a column or literal");
    }
    for value in &select.values_lists {
        let NodeEnum::List(values) = node(value)? else {
            anyhow::bail!("invalid scalar VALUES row");
        };
        for value in &values.items {
            ensure!(literal(value)?, "nonaggregate scalar VALUES requires literals");
        }
    }
    if let Some(filter) = select.where_clause.as_deref() {
        ensure!(
            predicate(filter, 0)?,
            "nonaggregate scalar RHS filter requires comparisons of columns and literals"
        );
    }
    for sort in &select.sort_clause {
        let (value, _, _) = super::super::partition::sort_expression(sort)?;
        ensure!(plain(value)?, "nonaggregate scalar RHS ordering requires a column or literal");
    }
    for key in select.distinct_clause.iter().filter(|value| value.node.is_some()) {
        ensure!(plain(key)?, "nonaggregate scalar RHS distinct key requires a column or literal");
    }
    for child in [&select.larg, &select.rarg].into_iter().flatten() {
        validate(child, depth + 1)?;
    }
    Ok(())
}

pub(super) const fn column(value: &Node) -> bool {
    matches!(value.node, Some(NodeEnum::ColumnRef(_)))
}

fn literal(value: &Node) -> Result<bool> {
    Ok(match node(value)? {
        NodeEnum::AConst(_) => true,
        NodeEnum::TypeCast(cast) => {
            matches!(node(optional(cast.arg.as_deref())?)?, NodeEnum::AConst(_))
        }
        _ => false,
    })
}

pub(super) fn plain(value: &Node) -> Result<bool> {
    Ok(column(value) || literal(value)?)
}

fn predicate(value: &Node, depth: usize) -> Result<bool> {
    ensure!(depth < 32, "scalar RHS predicate depth exceeded");
    match node(value)? {
        NodeEnum::BoolExpr(value) => {
            for argument in &value.args {
                if !predicate(argument, depth + 1)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        NodeEnum::NullTest(test) => plain(optional(test.arg.as_deref())?),
        NodeEnum::BooleanTest(test) => plain(optional(test.arg.as_deref())?),
        NodeEnum::AExpr(expr)
            if crate::compiler::expression::is_comparison(expr)?
                || distinct_kind(expr.kind).is_some() =>
        {
            Ok(plain(optional(expr.lexpr.as_deref())?)? && plain(optional(expr.rexpr.as_deref())?)?)
        }
        _ => plain(value),
    }
}
