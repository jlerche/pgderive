//! Bounded correlation extraction; inner unqualified names retain local shadowing.
use super::ast;
use crate::compiler::parser::{
    Table,
    expressions::{column, names, node},
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
#[derive(Default)]
pub(super) struct Correlation {
    pub pairs: Vec<(Node, Node)>,
    pub residual: Option<Inequality>,
}
pub(super) struct Inequality {
    pub outer: Node,
    pub inner: Node,
    pub operator: String,
    pub outer_first: bool,
}
pub(super) fn extract(query: &mut pg::SelectStmt, inequality: bool) -> Result<Correlation> {
    let [source] = query.from_clause.as_slice() else {
        return Ok(Correlation::default());
    };
    let Ok(table) = crate::compiler::parser::table(source) else {
        return Ok(Correlation::default());
    };
    let mut local = Vec::new();
    let mut correlation = Correlation::default();
    if let Some(condition) = query.where_clause.take() {
        conjunct(*condition, &table, &mut local, (&mut correlation, inequality), 0)?;
    }
    query.where_clause = ast::logical(local, true);
    for value in query
        .target_list
        .iter()
        .chain(&query.group_clause)
        .chain(query.having_clause.iter().map(Box::as_ref))
    {
        ensure!(
            references(value, &table, 0)? & 2 == 0,
            "outer references require top-level AND correlation equalities"
        );
    }
    ensure!(
        correlation.residual.is_none() || !correlation.pairs.is_empty(),
        "inequality correlation requires an equality key"
    );
    Ok(correlation)
}
fn conjunct(
    value: Node,
    table: &Table,
    local: &mut Vec<Node>,
    state: (&mut Correlation, bool),
    depth: usize,
) -> Result<()> {
    ensure!(depth < 32, "subquery correlation budget exceeded");
    let (correlation, inequality) = state;
    if let NodeEnum::BoolExpr(value) = node(&value)?
        && value.boolop == i32::from(pg::BoolExprType::AndExpr)
    {
        for value in &value.args {
            conjunct(value.clone(), table, local, (correlation, inequality), depth + 1)?;
        }
        return Ok(());
    }
    let sides = references(&value, table, 0)?;
    if sides & 2 == 0 {
        local.push(value);
        return Ok(());
    }
    comparison(&value, table, correlation, inequality)
}
fn comparison(
    value: &Node,
    table: &Table,
    correlation: &mut Correlation,
    inequality: bool,
) -> Result<()> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("correlation requires top-level AND equalities");
    };
    ensure!(expression.kind == i32::from(pg::AExprKind::AexprOp), "invalid correlation comparison");
    let operator = names(&expression.name)?.0;
    let [operator] = operator.as_slice() else {
        anyhow::bail!("correlation requires an unqualified comparison");
    };
    let left = expression
        .lexpr
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing correlation left operand"))?;
    let right = expression
        .rexpr
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing correlation right operand"))?;
    let sides = (references(left, table, 0)?, references(right, table, 0)?);
    ensure!(
        matches!(sides, (1, 2) | (2, 1)),
        "correlation operands must be local to opposite scopes"
    );
    let outer_first = sides.0 == 2;
    let (outer, inner) = if outer_first { (left, right) } else { (right, left) };
    if operator == "=" {
        ensure!(correlation.pairs.len() < 8, "subquery correlation key count exceeded");
        correlation.pairs.push((outer.clone(), inner.clone()));
    } else {
        ensure!(
            inequality && matches!(operator.as_str(), "<" | "<=" | ">" | ">=" | "<>"),
            "correlation requires ordinary equality or one supported inequality"
        );
        ensure!(correlation.residual.is_none(), "only one correlation inequality is supported");
        ensure!(
            super::scalar_input::column(outer) && super::scalar_input::column(inner),
            "correlation inequality requires plain columns"
        );
        correlation.residual = Some(Inequality {
            outer: outer.clone(),
            inner: inner.clone(),
            operator: operator.clone(),
            outer_first,
        });
    }
    Ok(())
}
fn references(value: &Node, table: &Table, depth: usize) -> Result<u8> {
    ensure!(depth < 64, "subquery reference depth exceeded");
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        let name = column(value)?;
        return Ok(match name.0.as_slice() {
            [_] => 1,
            [qualifier, _]
                if Some(qualifier) == table.alias.as_ref().or_else(|| table.name.0.last()) =>
            {
                1
            }
            [schema, relation, _]
                if table.alias.is_none()
                    && table.name.0.as_slice() == [schema.clone(), relation.clone()] =>
            {
                1
            }
            _ => 2,
        });
    }
    let mut result = 0;
    for child in children(value) {
        result |= references(child, table, depth + 1)?;
    }
    Ok(result)
}
pub(super) fn children(value: &Node) -> Vec<&Node> {
    match &value.node {
        Some(NodeEnum::ResTarget(value)) => value.val.as_deref().into_iter().collect(),
        Some(NodeEnum::AExpr(value)) => {
            [&value.lexpr, &value.rexpr].into_iter().flatten().map(Box::as_ref).collect()
        }
        Some(NodeEnum::BoolExpr(value)) => value.args.iter().collect(),
        Some(NodeEnum::FuncCall(value)) => {
            value.args.iter().chain(value.agg_filter.iter().map(Box::as_ref)).collect()
        }
        Some(NodeEnum::CoalesceExpr(value)) => value.args.iter().collect(),
        Some(NodeEnum::MinMaxExpr(value)) => value.args.iter().collect(),
        Some(NodeEnum::TypeCast(value)) => value.arg.as_deref().into_iter().collect(),
        Some(NodeEnum::CollateClause(value)) => value.arg.as_deref().into_iter().collect(),
        Some(NodeEnum::NullTest(value)) => value.arg.as_deref().into_iter().collect(),
        Some(NodeEnum::BooleanTest(value)) => value.arg.as_deref().into_iter().collect(),
        Some(NodeEnum::CaseExpr(value)) => value
            .args
            .iter()
            .chain([&value.arg, &value.defresult].into_iter().flatten().map(Box::as_ref))
            .collect(),
        Some(NodeEnum::CaseWhen(value)) => {
            [&value.expr, &value.result].into_iter().flatten().map(Box::as_ref).collect()
        }
        Some(NodeEnum::List(value)) => value.items.iter().collect(),
        _ => Vec::new(),
    }
}
pub(super) fn aggregate(value: &Node) -> bool {
    super::super::partition::group_aggregate(value) || children(value).into_iter().any(aggregate)
}
