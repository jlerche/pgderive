//! Bounded correlation extraction; inner unqualified names retain local shadowing.
use super::ast;
use crate::compiler::parser::{
    Table,
    expressions::{column, names, node},
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
#[derive(Clone, Default)]
pub(super) struct Correlation {
    pub pairs: Vec<(Node, Node)>,
    pub residuals: Vec<Residual>,
    residual_count: usize,
}
#[derive(Clone)]
pub(super) enum Residual {
    Comparison(Box<ResidualComparison>),
    Logical { terms: Vec<Self>, kind: pg::BoolExprType },
}
#[derive(Clone)]
pub(super) struct ResidualComparison {
    pub outer: Node,
    pub inner: Node,
    pub operator: String,
    pub kind: pg::AExprKind,
    pub outer_first: bool,
}
pub(super) fn extract(query: &mut pg::SelectStmt, inequality: bool) -> Result<Correlation> {
    if !inequality
        && !matches!(query.from_clause.as_slice(), [source]
        if matches!(source.node, Some(NodeEnum::RangeVar(_))))
    {
        return Ok(Correlation::default());
    }
    let mut tables = Vec::new();
    for source in &query.from_clause {
        if !collect_tables(source, &mut tables, 0)? {
            return Ok(Correlation::default());
        }
    }
    if tables.is_empty() {
        return Ok(Correlation::default());
    }
    let mut local = Vec::new();
    let mut correlation = Correlation::default();
    if let Some(condition) = query.where_clause.take() {
        conjunct(*condition, &tables, &mut local, (&mut correlation, inequality), 0)?;
    }
    query.where_clause = ast::logical(local, true);
    for value in query
        .target_list
        .iter()
        .chain(&query.group_clause)
        .chain(query.having_clause.iter().map(Box::as_ref))
    {
        ensure!(
            references(value, &tables, 0)? & 2 == 0,
            "outer references require top-level AND correlation equalities"
        );
    }
    Ok(correlation)
}
fn collect_tables(value: &Node, tables: &mut Vec<Table>, depth: usize) -> Result<bool> {
    ensure!(depth < 16 && tables.len() < 16, "subquery source scope budget exceeded");
    match node(value)? {
        NodeEnum::RangeVar(_) => {
            let Ok(table) = crate::compiler::parser::table(value) else { return Ok(false) };
            tables.push(table);
            Ok(true)
        }
        NodeEnum::JoinExpr(join) if join.alias.is_none() && join.join_using_alias.is_none() => {
            let left = super::super::optional(join.larg.as_deref())?;
            let right = super::super::optional(join.rarg.as_deref())?;
            Ok(collect_tables(left, tables, depth + 1)?
                && collect_tables(right, tables, depth + 1)?)
        }
        _ => Ok(false),
    }
}

pub(in crate::compiler::parser) fn only_cross_scope_equalities(
    query: &pg::SelectStmt,
) -> Result<bool> {
    fn check(value: &Node, tables: &[Table], depth: usize) -> Result<bool> {
        ensure!(depth < 16, "EXISTS correlation nesting exceeded");
        if let NodeEnum::BoolExpr(logical) = node(value)?
            && logical.boolop == i32::from(pg::BoolExprType::AndExpr)
        {
            for term in &logical.args {
                if !check(term, tables, depth + 1)? {
                    return Ok(false);
                }
            }
            return Ok(!logical.args.is_empty());
        }
        if comparison(value, tables)
            .is_ok_and(|(operator, operands)| operands.ordinary_equality(&operator))
        {
            return Ok(true);
        }
        let NodeEnum::AExpr(expression) = node(value)? else { return Ok(false) };
        if expression.kind != i32::from(pg::AExprKind::AexprOp)
            || names(&expression.name)?.0 != ["="]
        {
            return Ok(false);
        }
        let (Some(left), Some(right)) = (&expression.lexpr, &expression.rexpr) else {
            return Ok(false);
        };
        // Leave unqualified equality operands to the existing native scope binder.
        Ok(references(left, tables, 0)? != 0
            && references(right, tables, 0)? != 0
            && (has_unqualified(left)? || has_unqualified(right)?))
    }
    let [source] = query.from_clause.as_slice() else { return Ok(false) };
    let Ok(table) = crate::compiler::parser::table(source) else { return Ok(false) };
    let Some(filter) = query.where_clause.as_deref() else { return Ok(false) };
    check(filter, &[table], 0)
}

fn has_unqualified(value: &Node) -> Result<bool> {
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        return Ok(column(value)?.0.len() == 1);
    }
    for child in children(value) {
        if has_unqualified(child)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn conjunct(
    value: Node,
    tables: &[Table],
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
            conjunct(value.clone(), tables, local, (correlation, inequality), depth + 1)?;
        }
        return Ok(());
    }
    let sides = references(&value, tables, 0)?;
    if sides & 2 == 0 {
        local.push(value);
        return Ok(());
    }
    if inequality && matches!(node(&value)?, NodeEnum::BoolExpr(_)) {
        let term = residual(&value, tables, &mut correlation.residual_count, depth)?;
        correlation.residuals.push(term);
        return Ok(());
    }
    let (operator, operands) = comparison(&value, tables)?;
    if operands.ordinary_equality(&operator) {
        ensure!(correlation.pairs.len() < 8, "subquery correlation key count exceeded");
        correlation.pairs.push((operands.outer, operands.inner));
    } else {
        ensure!(inequality, "correlation requires ordinary equality");
        correlation.residuals.push(residual_term(
            &operator,
            operands,
            &mut correlation.residual_count,
        )?);
    }
    Ok(())
}
fn residual(value: &Node, tables: &[Table], count: &mut usize, depth: usize) -> Result<Residual> {
    ensure!(depth < 16, "correlation residual nesting limit exceeded");
    if let NodeEnum::BoolExpr(logical) = node(value)? {
        let kind = pg::BoolExprType::try_from(logical.boolop)?;
        ensure!(
            match kind {
                pg::BoolExprType::NotExpr => logical.args.len() == 1,
                pg::BoolExprType::AndExpr | pg::BoolExprType::OrExpr => logical.args.len() >= 2,
                pg::BoolExprType::Undefined => false,
            },
            "invalid Boolean correlation expression"
        );
        let terms = logical
            .args
            .iter()
            .map(|term| residual(term, tables, count, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        return Ok(Residual::Logical { terms, kind });
    }
    let (operator, operands) = comparison(value, tables)?;
    residual_term(&operator, operands, count)
}

struct Operands {
    outer: Node,
    inner: Node,
    outer_first: bool,
    kind: pg::AExprKind,
}

impl Operands {
    fn ordinary_equality(&self, operator: &str) -> bool {
        self.kind == pg::AExprKind::AexprOp && operator == "="
    }
}

fn comparison(value: &Node, tables: &[Table]) -> Result<(String, Operands)> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("correlation requires top-level AND equalities");
    };
    let kind = pg::AExprKind::try_from(expression.kind)?;
    ensure!(
        matches!(
            kind,
            pg::AExprKind::AexprOp | pg::AExprKind::AexprDistinct | pg::AExprKind::AexprNotDistinct
        ),
        "invalid correlation comparison"
    );
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
    let sides = (references(left, tables, 0)?, references(right, tables, 0)?);
    ensure!(
        matches!(sides, (1, 2) | (2, 1)),
        "correlation operands must be local to opposite scopes"
    );
    let outer_first = sides.0 == 2;
    let (outer, inner) = if outer_first { (left, right) } else { (right, left) };
    Ok((
        operator.clone(),
        Operands { outer: outer.clone(), inner: inner.clone(), outer_first, kind },
    ))
}

fn residual_term(operator: &str, operands: Operands, count: &mut usize) -> Result<Residual> {
    ensure!(
        if operands.kind == pg::AExprKind::AexprOp {
            matches!(operator, "=" | "<" | "<=" | ">" | ">=" | "<>")
        } else {
            operator == "="
        },
        "unsupported correlation residual comparison"
    );
    ensure!(*count < 8, "subquery correlation residual comparison count exceeded");
    *count += 1;
    Ok(Residual::Comparison(Box::new(ResidualComparison {
        outer: operands.outer,
        inner: operands.inner,
        operator: operator.into(),
        kind: operands.kind,
        outer_first: operands.outer_first,
    })))
}
fn references(value: &Node, tables: &[Table], depth: usize) -> Result<u8> {
    ensure!(depth < 64, "subquery reference depth exceeded");
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        let name = column(value)?;
        return Ok(match name.0.as_slice() {
            [_] => 1,
            [qualifier, _]
                if tables.iter().any(|table| {
                    Some(qualifier) == table.alias.as_ref().or_else(|| table.name.0.last())
                }) =>
            {
                1
            }
            [schema, relation, _]
                if tables.iter().any(|table| {
                    table.alias.is_none()
                        && table.name.0.as_slice() == [schema.clone(), relation.clone()]
                }) =>
            {
                1
            }
            _ => 2,
        });
    }
    let mut result = 0;
    for child in children(value) {
        result |= references(child, tables, depth + 1)?;
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
        Some(NodeEnum::GroupingFunc(value)) => value.args.iter().collect(),
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
