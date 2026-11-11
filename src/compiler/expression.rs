mod fold;
mod quals;
mod visit;
use super::{ColumnRef, parser::Name, syntax::Compare};
use super::{
    typed::{self, Operand, Typed as Scalar},
    value::Value,
};
fn operand(value: &pg_query::Node, resolve: &Resolve<'_>) -> Result<Operand> {
    typed::operand(value, &|name| resolve(name))
}
use crate::transaction::Row;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Expr {
    Value(Scalar),
    Null(Scalar, bool),
    Compare(Compare, Scalar, Scalar),
    And(Vec<Self>),
    Or(Vec<Self>),
    Array { and: bool, values: Vec<Self> },
    Quals(Vec<Self>),
    Not(Box<Self>),
    BooleanTest { input: Box<Self>, expected: Option<bool>, negated: bool },
    Distinct { left: Scalar, right: Scalar, negated: bool },
}
type Resolve<'a> = dyn Fn(&Name) -> Result<ColumnRef> + 'a;
pub fn native_key(column: &ColumnRef, row: &Row) -> Result<Option<String>> {
    Scalar::column(column.clone())
        .native_pair((row, row))?
        .map(|value| match value {
            Value::Numeric(value) => serde_json::to_string(&value.key()).map_err(Into::into),
            _ => serde_json::to_string(&value).map_err(Into::into),
        })
        .transpose()
}
pub(in crate::compiler) fn is_comparison(value: &pg_query::protobuf::AExpr) -> Result<bool> {
    Ok(value.kind == i32::from(pg_query::protobuf::AExprKind::AexprOp)
        && matches!(super::parser::expressions::names(&value.name)?.0.as_slice(), [value] if matches!(value.as_str(), "="|"<>"|"<"|"<="|">"|">=")))
}
pub(super) fn bind(value: &pg_query::Node, resolve: &Resolve<'_>) -> Result<Expr> {
    bind_ast(value, resolve, &mut 0, 0, None)
}
pub(super) fn bind_quals(value: &pg_query::Node, resolve: &Resolve<'_>) -> Result<Expr> {
    bind(value, resolve).map(|value| value.until_false().into_quals())
}
pub(in crate::compiler) fn bind_join(
    value: &pg_query::Node,
    resolve: &Resolve<'_>,
    collation: &impl Fn(&ColumnRef) -> Result<u32>,
) -> Result<Expr> {
    let text = |value: &Scalar| value.collation(collation);
    bind_ast(value, resolve, &mut 0, 0, Some(&text))
}
type TextCollation<'a> = Option<&'a dyn Fn(&Scalar) -> Result<u32>>;
fn bind_ast(
    value: &pg_query::Node,
    resolve: &Resolve<'_>,
    visited: &mut usize,
    depth: usize,
    text: TextCollation<'_>,
) -> Result<Expr> {
    use super::parser::expressions::{between_kind, node, optional};
    use pg_query::{NodeEnum, protobuf as pg};
    *visited += 1;
    ensure!(*visited <= 512 && depth <= 64, "WHERE exceeds AST/depth budget");
    match node(value)? {
        NodeEnum::BoolExpr(value) => boolean(value, resolve, visited, depth, text),
        NodeEnum::BooleanTest(value) => {
            use pg::BoolTestType::{
                IsFalse, IsNotFalse, IsNotTrue, IsNotUnknown, IsTrue, IsUnknown,
            };
            ensure!(value.xpr.is_none(), "invalid Boolean test");
            let (expected, negated) = match pg::BoolTestType::try_from(value.booltesttype)? {
                IsTrue => (Some(true), false),
                IsNotTrue => (Some(true), true),
                IsFalse => (Some(false), false),
                IsNotFalse => (Some(false), true),
                IsUnknown => (None, false),
                IsNotUnknown => (None, true),
                pg::BoolTestType::Undefined => anyhow::bail!("invalid Boolean test type"),
            };
            let input =
                bind_ast(optional(value.arg.as_deref())?, resolve, visited, depth + 1, text)?;
            Ok(Expr::BooleanTest { input: Box::new(input), expected, negated })
        }
        NodeEnum::NullTest(value) => {
            ensure!(!value.argisrow && value.xpr.is_none(), "row NULL tests unsupported");
            let not = match pg::NullTestType::try_from(value.nulltesttype)? {
                pg::NullTestType::IsNull => false,
                pg::NullTestType::IsNotNull => true,
                pg::NullTestType::Undefined => anyhow::bail!("invalid NULL test"),
            };
            let value = optional(value.arg.as_deref())?;
            let value = operand(value, resolve)?.null_test()?;
            Ok(Expr::Null(value, not))
        }
        NodeEnum::AExpr(value) if value.kind == i32::from(pg::AExprKind::AexprIn) => {
            membership(value, resolve, text)
        }
        NodeEnum::AExpr(value) if value.kind != i32::from(pg::AExprKind::AexprNullif) => {
            between_kind(value.kind).map_or_else(
                || comparison(value, resolve, text),
                |negated| between(value, negated, resolve, text),
            )
        }
        _ => Ok(Expr::Value(operand(value, resolve)?.coerce(16)?)),
    }
}
fn boolean(
    value: &pg_query::protobuf::BoolExpr,
    resolve: &Resolve<'_>,
    visited: &mut usize,
    depth: usize,
    text: TextCollation<'_>,
) -> Result<Expr> {
    use pg_query::protobuf as pg;
    ensure!(value.xpr.is_none(), "invalid Boolean expression");
    let mut args = value
        .args
        .iter()
        .map(|value| bind_ast(value, resolve, visited, depth + 1, text))
        .collect::<Result<Vec<_>>>()?;
    let operator = pg::BoolExprType::try_from(value.boolop)?;
    match operator {
        pg::BoolExprType::AndExpr | pg::BoolExprType::OrExpr => {
            ensure!(args.len() >= 2, "invalid Boolean arity");
            normalized(args, operator == pg::BoolExprType::AndExpr)
        }
        pg::BoolExprType::NotExpr => {
            ensure!(args.len() == 1, "invalid NOT arity");
            Ok(args.remove(0).negated())
        }
        pg::BoolExprType::Undefined => anyhow::bail!("invalid Boolean operator"),
    }
}
fn comparison(
    value: &pg_query::protobuf::AExpr,
    resolve: &Resolve<'_>,
    text: TextCollation<'_>,
) -> Result<Expr> {
    use super::parser::expressions::{distinct_kind, names, optional};
    use pg_query::{NodeEnum, protobuf::AExprKind};

    let kind = distinct_kind(value.kind);
    ensure!(
        kind.is_some() || value.kind == i32::from(AExprKind::AexprOp),
        "unsupported comparison kind"
    );
    let name = names(&value.name)?;
    let [name] = name.0.as_slice() else {
        anyhow::bail!("qualified operators unsupported");
    };
    let left = optional(value.lexpr.as_deref())?;
    let right = optional(value.rexpr.as_deref())?;
    if let Some(negated) = kind {
        ensure!(name == "=", "invalid distinctness operator");
        // PostgreSQL treats an undecorated NULL as a scalar NULL test.
        let bare_null = |node: &pg_query::Node| matches!(&node.node, Some(NodeEnum::AConst(value)) if value.isnull);
        if let Some(input) =
            if bare_null(right) { Some(left) } else { bare_null(left).then_some(right) }
        {
            return Ok(Expr::Null(operand(input, resolve)?.into_value(), !negated));
        }
    }
    let op = match name.as_str() {
        "=" => Compare::Eq,
        "<>" => Compare::Ne,
        "<" => Compare::Lt,
        "<=" => Compare::Le,
        ">" => Compare::Gt,
        ">=" => Compare::Ge,
        _ => anyhow::bail!("unsupported comparison operator"),
    };
    let (left, right) = comparison_operands(operand(left, resolve)?, operand(right, resolve)?)?;
    check_comparison(&op, &left, &right, text)?;
    Ok(match kind {
        Some(negated) => Expr::Distinct { left, right, negated },
        None => Expr::Compare(op, left, right),
    })
}

fn between(
    value: &pg_query::protobuf::AExpr,
    negated: bool,
    resolve: &Resolve<'_>,
    text: TextCollation<'_>,
) -> Result<Expr> {
    use super::parser::expressions::{node, optional};
    use pg_query::NodeEnum;

    let NodeEnum::List(bounds) = node(optional(value.rexpr.as_deref())?)? else {
        anyhow::bail!("BETWEEN requires scalar bounds");
    };
    let [lower, upper] = bounds.items.as_slice() else {
        anyhow::bail!("BETWEEN requires two bounds");
    };
    let probe = operand(optional(value.lexpr.as_deref())?, resolve)?;
    // PostgreSQL resolves each comparison separately. Retain both comparisons
    // so planning and runtime can reach their operands in PostgreSQL's order.
    let lower_op = if negated { Compare::Lt } else { Compare::Ge };
    let upper_op = if negated { Compare::Gt } else { Compare::Le };
    let (left, lower) =
        comparison_operands(probe.duplicate_for_between()?, operand(lower, resolve)?)?;
    check_comparison(&lower_op, &left, &lower, text)?;
    let (right, upper) = comparison_operands(probe, operand(upper, resolve)?)?;
    check_comparison(&upper_op, &right, &upper, text)?;
    normalized(
        vec![Expr::Compare(lower_op, left, lower), Expr::Compare(upper_op, right, upper)],
        !negated,
    )
}

pub(super) fn comparison_operands(left: Operand, right: Operand) -> Result<(Scalar, Scalar)> {
    let oid = comparison_oid(left.oid(), right.oid(), [left.literal_oid(), right.literal_oid()])?;
    Ok((left.coerce(oid)?, right.coerce(oid)?))
}
pub(super) fn case_match(test: &Scalar, candidate: Operand) -> Result<Scalar> {
    let oid = comparison_oid(Some(test.oid()), candidate.oid(), [0, candidate.literal_oid()])?;
    let candidate = candidate.coerce(oid)?;
    check_comparison(&Compare::Eq, test, &candidate, None)?;
    Ok(candidate)
}
fn comparison_oid(left: Option<u32>, right: Option<u32>, literals: [u32; 2]) -> Result<u32> {
    let oid = if [left, right].iter().flatten().any(|oid| matches!(oid, 1082 | 1114 | 1184)) {
        [1184, 1114, 1082]
            .into_iter()
            .find(|oid| left == Some(*oid) || right == Some(*oid))
            .context("temporal comparison type absent")?
    } else {
        left.or(right).unwrap_or_else(|| literals.into_iter().find(|oid| *oid != 0).unwrap_or(25))
    };
    ensure!(
        matches!(oid, 16 | 20 | 21 | 23 | 1700 | 25 | 1043 | 2950 | 1082 | 1114 | 1184 | 1186),
        "SQL type: unsupported comparison type"
    );
    Ok(oid)
}
pub(super) fn check_comparison(
    op: &Compare,
    left: &Scalar,
    right: &Scalar,
    text: TextCollation<'_>,
) -> Result<()> {
    let oid = left.oid();
    if matches!(oid, 25 | 1043) && text.is_none() {
        ensure!(
            matches!(op, Compare::Eq | Compare::Ne) && (left.is_constant() || right.is_constant()),
            "text equality requires a constant operand or resolved join collation"
        );
    }
    if matches!(oid, 25 | 1043)
        && let Some(text) = text
    {
        let (left_collation, right_collation) = (text(left)?, text(right)?);
        ensure!(left_collation != 0 && right_collation != 0, "unresolved ON text collation");
        let collation = typed::collation::common(left_collation, right_collation)?;
        ensure!(
            matches!(op, Compare::Eq | Compare::Ne) || matches!(collation, 950 | 951),
            "text ON ordering requires C/POSIX collation"
        );
    }
    Ok(())
}
fn membership(
    value: &pg_query::protobuf::AExpr,
    resolve: &Resolve<'_>,
    text: TextCollation<'_>,
) -> Result<Expr> {
    use super::parser::expressions::{names, node, optional};
    use pg_query::NodeEnum;
    let NodeEnum::List(list) = node(optional(value.rexpr.as_deref())?)? else {
        anyhow::bail!("IN requires a scalar list");
    };
    ensure!((1..=64).contains(&list.items.len()), "IN list budget exceeded");
    let operator = names(&value.name)?;
    let [operator] = operator.0.as_slice() else {
        anyhow::bail!("qualified IN operator unsupported");
    };
    ensure!(matches!(operator.as_str(), "=" | "<>"), "unsupported IN operator");
    let values = std::iter::once(optional(value.lexpr.as_deref())?)
        .chain(&list.items)
        .map(|value| operand(value, resolve))
        .collect::<Result<Vec<_>>>()?;
    let common = typed::comparison_type(
        &values
            .iter()
            .map(|value| value.oid().unwrap_or_else(|| value.literal_oid()))
            .collect::<Vec<_>>(),
    )?;
    let mut values = values.into_iter().map(|value| value.coerce(common));
    let probe = values.next().context("IN probe absent")??;
    let and = operator == "<>";
    let op = if and { Compare::Ne } else { Compare::Eq };
    let written = values
        .map(|value| {
            let value = value?;
            check_comparison(&op, &probe, &value, text)?;
            Ok(Expr::Compare(op.clone(), probe.clone(), value))
        })
        .collect::<Result<Vec<_>>>()?;
    if written.iter().filter(|value| value.compares_constant()).count() < 2 {
        return normalized(written, and);
    }
    let (constants, computed): (Vec<_>, Vec<_>) =
        written.into_iter().partition(Expr::compares_constant);
    let array = Expr::Array { and, values: constants };
    normalized(std::iter::once(array).chain(computed).collect(), and)
}
fn normalized(args: Vec<Expr>, and: bool) -> Result<Expr> {
    let mut values: Vec<Expr> = Vec::new();
    let mut neutral = None;
    let flattened = args.into_iter().flat_map(|arg| match arg {
        Expr::And(nested) if and => nested,
        Expr::Or(nested) if !and => nested,
        value => vec![value],
    });
    for value in flattened {
        match value.constant_truth() {
            // Folding stops here, so the failure raises unless an earlier operand decided.
            Err(_) => {
                values.push(value);
                break;
            }
            // A substitution can still put a failing constant in what came before.
            Ok(Some(decided)) if decided != and => {
                values.push(value);
                break;
            }
            Ok(Some(_)) => neutral = Some(value),
            _ if values.contains(&value) => {}
            _ => values.push(value),
        }
    }
    if values.is_empty() {
        return neutral.context("empty normalized expression");
    }
    if values.len() == 1 {
        return values.pop().context("empty normalized expression");
    }
    Ok(if and { Expr::And(values) } else { Expr::Or(values) })
}
impl Expr {
    fn negated(self) -> Self {
        match self {
            Self::Null(input, not) => Self::Null(input, !not),
            Self::Distinct { left, right, negated } => {
                Self::Distinct { left, right, negated: !negated }
            }
            Self::BooleanTest { input, expected, negated } => {
                Self::BooleanTest { input, expected, negated: !negated }
            }
            value => Self::Not(Box::new(value)),
        }
    }

    pub(super) fn evaluate(&self, rows: (&Row, &Row)) -> Result<Option<bool>> {
        match self {
            Self::Value(value) => value
                .native_pair(rows)?
                .map(|value| {
                    let Value::Boolean(value) = value else {
                        return Err(
                            crate::engine::execution::PlanFault("nonboolean predicate").into()
                        );
                    };
                    Ok(value)
                })
                .transpose(),
            Self::Null(value, not) => Ok(Some(value.is_null(rows)? != *not)),
            Self::Compare(op, left, right) => {
                let (Some(left), Some(right)) = (left.native_pair(rows)?, right.native_pair(rows)?)
                else {
                    return Ok(None);
                };
                Ok(Some(compare(op, compare_order(&left, &right)?)))
            }
            Self::Distinct { left, right, negated } => {
                let distinct = match (left.native_pair(rows)?, right.native_pair(rows)?) {
                    (Some(left), Some(right)) => compare_order(&left, &right)? != Ordering::Equal,
                    (None, None) => false,
                    _ => true,
                };
                Ok(Some(distinct != *negated))
            }
            Self::Not(value) => Ok(value.evaluate(rows)?.map(|value| !value)),
            Self::BooleanTest { input, expected, negated } => {
                Ok(Some((input.evaluate(rows)? == *expected) != *negated))
            }
            Self::And(values) => logical(values, rows, true),
            Self::Or(values) => logical(values, rows, false),
            Self::Array { and, values } => logical(values, rows, *and),
            Self::Quals(values) => {
                for value in values {
                    match value.evaluate(rows)? {
                        Some(true) => {}
                        decided => return Ok(decided),
                    }
                }
                Ok(Some(true))
            }
        }
    }
}
fn logical(values: &[Expr], rows: (&Row, &Row), and: bool) -> Result<Option<bool>> {
    let mut unknown = false;
    for value in values {
        match value.evaluate(rows)? {
            Some(value) if value != and => return Ok(Some(!and)),
            None => unknown = true,
            Some(_) => {}
        }
    }
    Ok((!unknown).then_some(and))
}
pub(super) fn compare_order(left: &Value, right: &Value) -> Result<Ordering> {
    use crate::numeric::Numeric;
    Ok(match (left, right) {
        (Value::Date(left), Value::Timestamp(right)) => left.compare_timestamp(*right),
        (Value::Timestamp(left), Value::Date(right)) => right.compare_timestamp(*left).reverse(),
        (Value::Numeric(left), Value::Integer(right)) => {
            left.cmp(&Numeric::parse(&right.to_string())?)
        }
        (Value::Integer(left), Value::Numeric(right)) => {
            Numeric::parse(&left.to_string())?.cmp(right)
        }
        _ => left.cmp(right),
    })
}

const fn compare(op: &Compare, ordering: Ordering) -> bool {
    match op {
        Compare::Eq => matches!(ordering, Ordering::Equal),
        Compare::Ne => !matches!(ordering, Ordering::Equal),
        Compare::Lt => matches!(ordering, Ordering::Less),
        Compare::Le => !matches!(ordering, Ordering::Greater),
        Compare::Gt => matches!(ordering, Ordering::Greater),
        Compare::Ge => !matches!(ordering, Ordering::Less),
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Predicate(Expr);
impl Predicate {
    pub(super) const fn new(expr: Expr) -> Self {
        Self(expr)
    }
    pub(in crate::compiler) fn is_constant(&self) -> bool {
        self.0.is_constant()
    }
    pub(crate) fn qualifies(&self, row: &Row) -> Result<bool> {
        self.qualifies_pair((row, row))
    }
    pub(crate) fn qualifies_pair(&self, rows: (&Row, &Row)) -> Result<bool> {
        Ok(self.0.evaluate(rows)? == Some(true))
    }
}

impl Predicate {
    pub(in crate::compiler) fn non_null_columns(&self) -> std::collections::BTreeSet<String> {
        fn collect(value: &Expr, out: &mut std::collections::BTreeSet<String>) {
            match value {
                Expr::Null(value, true) => {
                    if let Some(c) = value.as_column() {
                        out.insert(c.name.clone());
                    }
                }
                Expr::And(values) | Expr::Quals(values) => {
                    for value in values {
                        collect(value, out);
                    }
                }
                _ => {}
            }
        }
        let mut out = std::collections::BTreeSet::new();
        collect(&self.0, &mut out);
        out
    }
    pub(super) fn row_number_cutoff(&self, field: &str) -> Option<usize> {
        let Expr::Compare(compare, left, right) = &self.0 else {
            return None;
        };
        let (compare, column, limit) = if let (Some(column), Some(limit)) =
            (left.as_column(), right.as_integer_literal())
        {
            (compare.clone(), column, limit)
        } else if let (Some(limit), Some(column)) = (left.as_integer_literal(), right.as_column()) {
            let reversed = match compare {
                Compare::Eq => Compare::Eq,
                Compare::Lt => Compare::Gt,
                Compare::Le => Compare::Ge,
                Compare::Gt => Compare::Lt,
                Compare::Ge => Compare::Le,
                Compare::Ne => Compare::Ne,
            };
            (reversed, column, limit)
        } else {
            return None;
        };
        if column.name != field {
            return None;
        }
        let limit = match compare {
            Compare::Eq | Compare::Le => limit.max(0),
            Compare::Lt => limit.saturating_sub(1).max(0),
            _ => return None,
        };
        let limit = usize::try_from(limit).ok()?;
        (limit <= 1024).then_some(limit)
    }
}

impl Expr {
    pub(super) const fn nullable(&self) -> bool {
        !matches!(self, Self::Null(..) | Self::BooleanTest { .. } | Self::Distinct { .. })
    }

    pub(super) fn is_constant(&self) -> bool {
        match self {
            Self::Value(value) | Self::Null(value, _) => value.is_constant(),
            Self::Compare(_, left, right) | Self::Distinct { left, right, .. } => {
                left.is_constant() && right.is_constant()
            }
            Self::And(values)
            | Self::Or(values)
            | Self::Array { values, .. }
            | Self::Quals(values) => values.iter().all(Self::is_constant),
            Self::Not(value) | Self::BooleanTest { input: value, .. } => value.is_constant(),
        }
    }
    pub(in crate::compiler) fn constant_truth(&self) -> Result<Option<bool>> {
        if !self.is_constant() {
            return Ok(None);
        }
        let row = Row::new();
        self.evaluate((&row, &row))
    }
    fn compares_constant(&self) -> bool {
        matches!(self, Self::Compare(_, _, value) if value.is_constant())
    }
    pub(in crate::compiler) fn cost(&self) -> u64 {
        if self.is_constant() {
            return 0;
        }
        match self {
            Self::Value(value) | Self::Null(value, _) => value.cost(),
            Self::Compare(_, left, right) | Self::Distinct { left, right, .. } => {
                Scalar::operator_cost(left, right)
            }
            Self::And(values) | Self::Or(values) | Self::Quals(values) => {
                values.iter().map(Self::cost).sum()
            }
            Self::Array { values, .. } => {
                let probe = values.first().map_or(0, Self::cost);
                let elements = u64::try_from(values.len()).unwrap_or(u64::MAX);
                let lookup = if elements < 9 { elements } else { 2 * typed::OPERATOR_COST };
                probe.saturating_sub(typed::OPERATOR_COST).saturating_add(lookup)
            }
            Self::Not(value) | Self::BooleanTest { input: value, .. } => value.cost(),
        }
    }
}
