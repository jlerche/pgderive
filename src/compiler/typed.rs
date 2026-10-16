//! Checked native scalar expressions executed at their bound relational scope.
use super::capabilities::Arithmetic;
use super::{ColumnRef, parser::Name};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum};
use serde::Serialize;
use types::numeric_common;
mod case;
mod functions;
mod predicate;
mod text;
mod types;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Typed {
    oid: u32,
    value: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
enum Value {
    Column(ColumnRef),
    Average(ColumnRef),
    Literal(Option<i64>),
    Numeric(String),
    Text(String),
    Encoded(Option<String>),
    Interval(crate::temporal::Gap),
    Difference(ColumnRef, ColumnRef),
    Remainder(super::remainder::Remainder),
    Bin(super::scalar::DateBin),
    Offset(super::scalar::Offset),
    Call(functions::Call),
    Cast(Box<Typed>),
    Binary { operation: Arithmetic, left: Box<Typed>, right: Box<Typed> },
    Coalesce(Vec<Typed>),
    Case { branches: Vec<(super::expression::Expr, Typed)>, otherwise: Box<Typed> },
}
impl Typed {
    pub(in crate::compiler) fn is_constant(&self) -> bool {
        match &self.value {
            Value::Column(_)
            | Value::Average(_)
            | Value::Difference(..)
            | Value::Remainder(_)
            | Value::Bin(_)
            | Value::Offset(_) => false,
            Value::Literal(_)
            | Value::Numeric(_)
            | Value::Text(_)
            | Value::Encoded(_)
            | Value::Interval(_) => true,
            Value::Call(call) => call.arguments.iter().all(Self::is_constant),
            Value::Cast(value) => value.is_constant(),
            Value::Binary { left, right, .. } => left.is_constant() && right.is_constant(),
            Value::Coalesce(values) => values.iter().all(Self::is_constant),
            Value::Case { branches, otherwise } => {
                otherwise.is_constant()
                    && branches
                        .iter()
                        .all(|(condition, value)| condition.is_constant() && value.is_constant())
            }
        }
    }

    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        self.text_pair((row, row))
    }
    fn integer(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<i64>> {
        let value = match &self.value {
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("missing typed scalar input")?
                .as_deref()
                .map(str::parse)
                .transpose()?,
            Value::Literal(value) => *value,
            Value::Average(_)
            | Value::Numeric(_)
            | Value::Text(_)
            | Value::Interval(_)
            | Value::Difference(..)
            | Value::Bin(_)
            | Value::Offset(_) => anyhow::bail!("nonintegral scalar value"),
            Value::Encoded(value) => value.as_deref().map(str::parse).transpose()?,
            Value::Remainder(value) => value.integer_pair(rows)?,
            Value::Call(call) => call.evaluate(rows)?.map(|value| value.parse()).transpose()?,
            Value::Cast(input) => input.integer(rows)?,
            Value::Case { branches, otherwise } => {
                let mut selected = otherwise.as_ref();
                for (condition, result) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        selected = result;
                        break;
                    }
                }
                selected.integer(rows)?
            }
            Value::Coalesce(arguments) => {
                let mut result = None;
                for argument in arguments {
                    result = argument.integer(rows)?;
                    if result.is_some() {
                        break;
                    }
                }
                result
            }
            Value::Binary { operation, left, right } => {
                let left = left.integer(rows)?;
                let right = right.integer(rows)?;
                match (left, right) {
                    (Some(left), Some(right)) => Some(
                        operation.integer(left, right).context("integral arithmetic overflow")?,
                    ),
                    _ => None,
                }
            }
        };
        if let Some(value) = value {
            check_width(value, self.oid)?;
        }
        Ok(value)
    }
}
fn check_width(value: i64, oid: u32) -> Result<()> {
    match oid {
        21 => {
            i16::try_from(value).context("smallint overflow")?;
        }
        23 => {
            i32::try_from(value).context("integer overflow")?;
        }
        20 => {}
        _ => anyhow::bail!("unsupported integral width"),
    }
    Ok(())
}
pub(super) fn bind(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    let result = bind_inner(value, resolve, depth)?;
    if result.is_constant() {
        result
            .evaluate(&crate::transaction::Row::new())
            .context("constant expression failed in-engine")?;
    }
    Ok(result)
}
fn bind_inner(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use super::parser::expressions::{column, names, node, optional};
    ensure!(depth < 32, "typed scalar expression depth exceeded");
    match node(value)? {
        NodeEnum::CaseExpr(value) => case::bind(value, resolve, depth),
        NodeEnum::ColumnRef(_) => {
            let column = resolve(&column(value)?)?;
            ensure!(
                matches!(column.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1700),
                "column type is not available in-engine typed expressions"
            );
            Ok(Typed { oid: column.oid, value: Value::Column(column) })
        }
        NodeEnum::AConst(constant) => literal(constant),
        NodeEnum::TypeCast(cast) => cast_expression(cast, resolve, depth),
        NodeEnum::AExpr(expression) => {
            ensure!(
                expression.kind == i32::from(pg_query::protobuf::AExprKind::AexprOp),
                "unsupported scalar operator kind"
            );
            let name = names(&expression.name)?.0;
            let [operation] = name.as_slice() else {
                anyhow::bail!("qualified scalar operator unsupported");
            };
            let operation = Arithmetic::resolve(operation)?;
            let left = bind(optional(expression.lexpr.as_deref())?, resolve, depth + 1)?;
            let right = bind(optional(expression.rexpr.as_deref())?, resolve, depth + 1)?;
            ensure!(left.oid != 0 || right.oid != 0, "unresolved scalar operator types");
            ensure!(
                [left.oid, right.oid].iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 1700)),
                "arithmetic requires integral or numeric operands"
            );
            let oid = numeric_common(&[left.oid, right.oid]);
            Ok(Typed {
                oid,
                value: Value::Binary { operation, left: Box::new(left), right: Box::new(right) },
            })
        }
        NodeEnum::CoalesceExpr(expression) => {
            ensure!(
                expression.xpr.is_none() && (1..=64).contains(&expression.args.len()),
                "invalid scalar COALESCE"
            );
            let arguments = expression
                .args
                .iter()
                .map(|value| bind(value, resolve, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                arguments.iter().any(|value| value.oid != 0),
                "all-NULL circuit COALESCE requires an explicit integral cast"
            );
            let oid = types::common(&arguments.iter().map(|value| value.oid).collect::<Vec<_>>())?;
            Ok(Typed { oid, value: Value::Coalesce(arguments) })
        }
        NodeEnum::FuncCall(function) => functions::bind(function, resolve, depth),
        _ => anyhow::bail!("expression is not available in-engine"),
    }
}
pub(super) fn computed(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    computed: &mut Vec<super::scalar::Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    let mut typed = bind(value, resolve, 0)?;
    if typed.oid == 705 {
        typed.oid = 25;
    }
    ensure!(
        matches!(typed.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1700),
        "computed scalar requires supported native type"
    );
    super::scalar::register(typed, computed, &format!("@typed_{scope:?}_"))
}

#[cfg(test)]
#[path = "typed/tests.rs"]
mod tests;

fn literal(constant: &pg_query::protobuf::AConst) -> Result<Typed> {
    if constant.isnull {
        return Ok(Typed { oid: 0, value: Value::Literal(None) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Fval(value)) = &constant.val {
        if !value.fval.contains(['.', 'e', 'E'])
            && let Ok(integer) = value.fval.parse::<i64>()
        {
            return Ok(Typed {
                oid: if i32::try_from(integer).is_ok() { 23 } else { 20 },
                value: Value::Literal(Some(integer)),
            });
        }
        let numeric = crate::numeric::Numeric::parse(&value.fval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(numeric.text()) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Sval(value)) = &constant.val {
        return Ok(Typed { oid: 705, value: Value::Text(value.sval.clone()) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Boolval(value)) = &constant.val {
        return Ok(Typed {
            oid: 16,
            value: Value::Encoded(Some(if value.boolval { "t" } else { "f" }.into())),
        });
    }
    let Some(pg_query::protobuf::a_const::Val::Ival(value)) = &constant.val else {
        anyhow::bail!("typed scalar requires integer literal");
    };
    Ok(Typed { oid: 23, value: Value::Literal(Some(i64::from(value.ival))) })
}

impl Typed {
    pub(super) fn nullable(&self) -> bool {
        match &self.value {
            Value::Column(column) | Value::Average(column) => column.nullable,
            Value::Literal(value) => value.is_none(),
            Value::Numeric(_) | Value::Text(_) | Value::Interval(_) => false,
            Value::Encoded(value) => value.is_none(),
            Value::Difference(left, right) => left.nullable || right.nullable,
            Value::Remainder(value) => value.input.nullable,
            Value::Bin(value) => value.input.nullable,
            Value::Offset(value) => value.input.nullable,
            Value::Call(call) => call.arguments.iter().any(Self::nullable),
            Value::Cast(input) => input.nullable(),
            Value::Binary { left, right, .. } => left.nullable() || right.nullable(),
            Value::Coalesce(arguments) => arguments.iter().all(Self::nullable),
            Value::Case { branches, otherwise } => {
                otherwise.nullable() || branches.iter().any(|(_, value)| value.nullable())
            }
        }
    }
    pub(in crate::compiler) const fn oid(&self) -> u32 {
        self.oid
    }
}
pub(super) fn difference(left: ColumnRef, right: ColumnRef) -> Typed {
    Typed {
        oid: numeric_common(&[left.oid, right.oid]),
        value: Value::Binary {
            operation: Arithmetic::Subtract,
            left: Box::new(Typed { oid: left.oid, value: Value::Column(left) }),
            right: Box::new(Typed { oid: right.oid, value: Value::Column(right) }),
        },
    }
}

impl Typed {
    pub(super) fn text_pair(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<String>> {
        match &self.value {
            Value::Average(column) => {
                return rows
                    .0
                    .get(&column.name)
                    .context("missing intermediate average")?
                    .as_deref()
                    .map(crate::numeric::Numeric::average)
                    .transpose();
            }
            Value::Encoded(value) => return Ok(value.clone()),
            Value::Bin(value) => return value.evaluate(rows.0),
            Value::Offset(value) => return value.evaluate(rows.0),
            _ => {}
        }
        if matches!(self.oid, 16 | 25 | 1043 | 705 | 2950 | 1114 | 1184) {
            return self.string_pair(rows);
        }
        if self.oid != 1700 {
            return self.integer(rows).map(|v| v.map(|v| v.to_string()));
        }
        let value = match &self.value {
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("missing numeric input")?
                .clone(),
            Value::Literal(value) => value.map(|v| v.to_string()),
            Value::Numeric(value) => Some(value.clone()),
            Value::Average(_)
            | Value::Text(_)
            | Value::Call(_)
            | Value::Encoded(_)
            | Value::Interval(_)
            | Value::Difference(..)
            | Value::Remainder(_)
            | Value::Bin(_)
            | Value::Offset(_) => anyhow::bail!("invalid numeric expression"),
            Value::Cast(input) => input.text_pair(rows)?,
            Value::Binary { operation, left, right } => {
                let (Some(left), Some(right)) = (left.text_pair(rows)?, right.text_pair(rows)?)
                else {
                    return Ok(None);
                };
                Some(
                    crate::numeric::Numeric::parse(&left)?
                        .binary(&crate::numeric::Numeric::parse(&right)?, operation.symbol())?
                        .text(),
                )
            }
            Value::Coalesce(arguments) => {
                let mut value = None;
                for argument in arguments {
                    value = argument.text_pair(rows)?;
                    if value.is_some() {
                        break;
                    }
                }
                value
            }
            Value::Case { branches, otherwise } => {
                let mut selected = otherwise.as_ref();
                for (condition, result) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        selected = result;
                        break;
                    }
                }
                selected.text_pair(rows)?
            }
        };
        value.map(|v| Ok(crate::numeric::Numeric::parse(&v)?.text())).transpose()
    }
}

fn numeric_cast_input(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    if let NodeEnum::AConst(constant) = super::parser::expressions::node(value)?
        && let Some(pg_query::protobuf::a_const::Val::Sval(value)) = &constant.val
    {
        crate::numeric::Numeric::parse(&value.sval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(value.sval.clone()) });
    }
    bind(value, resolve, depth)
}

fn cast_expression(
    cast: &pg_query::protobuf::TypeCast,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    types::cast(cast, resolve, depth)
}

impl Typed {
    pub(super) const fn average(column: ColumnRef) -> Self {
        Self { oid: 1700, value: Value::Average(column) }
    }
}
