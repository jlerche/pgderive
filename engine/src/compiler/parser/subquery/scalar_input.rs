//! The scalar RHS is maintained independently, so its expressions must be safe without demand.
use super::super::expressions::{distinct_kind, node, optional};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

#[derive(Clone, Copy)]
pub(super) enum Input {
    Scalar,
    Membership,
    Quantified,
}

pub(super) fn validate(select: &pg::SelectStmt, input: Input) -> Result<()> {
    validate_scope(select, 0, input)
}

fn validate_scope(select: &pg::SelectStmt, depth: usize, input: Input) -> Result<()> {
    ensure!(depth < 16, "scalar RHS nesting exceeds limit");
    ensure!(
        select.with_clause.is_none()
            && select.group_clause.is_empty()
            && select.having_clause.is_none()
            && select.window_clause.is_empty()
            && select.from_clause.len() <= 1,
        "nonaggregate scalar RHS requires native tables, a total native join, or constant/set input"
    );
    if let [source] = select.from_clause.as_slice() {
        ensure!(
            if matches!(input, Input::Scalar) {
                matches!(source.node, Some(NodeEnum::RangeVar(_)))
            } else {
                total_source(source, 0)?
            },
            "scalar RHS requires native tables and simple local join predicates"
        );
    }
    for target in &select.target_list {
        let value = super::super::target(target)?;
        ensure!(
            if matches!(input, Input::Quantified) {
                conversion(value, 0)?
            } else {
                collated_plain(value)?
            },
            "nonaggregate scalar RHS requires a column or literal"
        );
    }
    for value in &select.values_lists {
        let NodeEnum::List(values) = node(value)? else {
            anyhow::bail!("invalid scalar VALUES row");
        };
        for value in &values.items {
            ensure!(
                literal(without_collation(value)?)?,
                "nonaggregate scalar VALUES requires literals"
            );
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
        validate_scope(child, depth + 1, input)?;
    }
    Ok(())
}

// A lifted join runs independently of its outer query. Only ordinary native
// joins with simple local predicates enter this path.
fn total_source(value: &Node, depth: usize) -> Result<bool> {
    ensure!(depth < 16, "scalar RHS join depth exceeded");
    match node(value)? {
        NodeEnum::RangeVar(_) => Ok(true),
        NodeEnum::JoinExpr(join) => {
            let left = optional(join.larg.as_deref())?;
            let right = optional(join.rarg.as_deref())?;
            let total_condition = match join.quals.as_deref() {
                Some(condition) => predicate(condition, 0)?,
                None => true,
            };
            Ok(total_condition && total_source(left, depth + 1)? && total_source(right, depth + 1)?)
        }
        _ => Ok(false),
    }
}

// The bound expressions must additionally pass the runtime totality proof.
// Preserve row count here and exclude calls that could introduce aggregates,
// windows or set-returning operators into the independently maintained side.
fn conversion(value: &Node, depth: usize) -> Result<bool> {
    ensure!(depth < 32, "quantified RHS conversion depth exceeded");
    match node(value)? {
        NodeEnum::TypeCast(cast) => conversion(optional(cast.arg.as_deref())?, depth + 1),
        NodeEnum::CollateClause(clause) => conversion(optional(clause.arg.as_deref())?, depth + 1),
        _ => plain(value),
    }
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

pub(super) fn collated_plain(value: &Node) -> Result<bool> {
    plain(without_collation(value)?)
}

fn without_collation(value: &Node) -> Result<&Node> {
    if let NodeEnum::CollateClause(clause) = node(value)? {
        optional(clause.arg.as_deref())
    } else {
        Ok(value)
    }
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
