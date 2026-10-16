use super::{
    ColumnRef,
    parser::Name,
    syntax::{self, Compare},
};
use super::{typed::Typed as Scalar, value::Value};
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
    Not(Box<Self>),
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
pub(super) fn bind(expr: syntax::Expr, resolve: &Resolve<'_>) -> Result<Expr> {
    match expr {
        syntax::Expr::Value(value) => Ok(Expr::Value(operand(value, resolve)?.coerce(16)?)),
        syntax::Expr::Null(value, not) => {
            ensure!(
                matches!(value, syntax::Scalar::Column(_) | syntax::Scalar::Typed(_)),
                "SQL type: NULL tests require a column or typed expression"
            );
            let Operand::Bound(value) = operand(value, resolve)? else {
                anyhow::bail!("unresolved NULL test")
            };
            Ok(Expr::Null(value, not))
        }
        syntax::Expr::Compare(op, left, right) => {
            let left = operand(left, resolve)?;
            let right = operand(right, resolve)?;
            let oid = left
                .oid()
                .or_else(|| right.oid())
                .context("SQL type: comparison requires a native column")?;
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 1700 | 25 | 1043 | 2950 | 1114 | 1184 | 1186),
                "SQL type: unsupported comparison type"
            );
            if matches!(oid, 25 | 1043) {
                ensure!(
                    matches!(op, Compare::Eq | Compare::Ne)
                        && left.oid().is_some() != right.oid().is_some(),
                    "text equality requires one native column and an unknown literal; text ordering and column collation resolution are unsupported"
                );
            }
            Ok(Expr::Compare(op, left.coerce(oid)?, right.coerce(oid)?))
        }
        syntax::Expr::Not(expr) => Ok(Expr::Not(Box::new(bind(*expr, resolve)?))),
        syntax::Expr::And(args) => normalized(args, resolve, true),
        syntax::Expr::Or(args) => normalized(args, resolve, false),
    }
}
fn normalized(args: Vec<syntax::Expr>, resolve: &Resolve<'_>, and: bool) -> Result<Expr> {
    let mut values = Vec::new();
    for arg in args {
        match bind(arg, resolve)? {
            Expr::And(nested) if and => values.extend(nested),
            Expr::Or(nested) if !and => values.extend(nested),
            value => values.push(value),
        }
    }
    values.sort();
    values.dedup();
    if values.len() == 1 {
        return values.pop().context("empty normalized expression");
    }
    Ok(if and { Expr::And(values) } else { Expr::Or(values) })
}
fn operand(value: syntax::Scalar, resolve: &Resolve<'_>) -> Result<Operand> {
    match value {
        syntax::Scalar::Column(name) => Ok(Operand::Bound(Scalar::column(resolve(&name)?))),
        syntax::Scalar::Typed(value) => {
            Ok(Operand::Bound(super::typed::bind(&value, &|name| resolve(name), 0)?))
        }
        syntax::Scalar::Remainder(name, divisor) => {
            Ok(Operand::Bound(Scalar::remainder(resolve(&name)?, divisor)?))
        }
        syntax::Scalar::Difference(left, right) => {
            Ok(Operand::Bound(Scalar::difference(resolve(&left)?, resolve(&right)?)?))
        }
        literal => Ok(Operand::Literal(literal)),
    }
}
enum Operand {
    Bound(Scalar),
    Literal(syntax::Scalar),
}
impl Operand {
    const fn oid(&self) -> Option<u32> {
        match self {
            Self::Bound(value) => Some(value.oid()),
            Self::Literal(_) => None,
        }
    }
    fn coerce(self, oid: u32) -> Result<Scalar> {
        match self {
            Self::Bound(value) => {
                ensure!(
                    value.oid() == oid || (numeric(oid) && numeric(value.oid())),
                    "SQL type: incompatible comparison operands"
                );
                Ok(value)
            }
            Self::Literal(value) => Scalar::predicate_literal(value, oid),
        }
    }
}
const fn numeric(oid: u32) -> bool {
    matches!(oid, 20 | 21 | 23 | 1700)
}
impl Expr {
    pub(super) fn evaluate(&self, rows: (&Row, &Row)) -> Result<Option<bool>> {
        match self {
            Self::Value(value) => value
                .native_pair(rows)?
                .map(|value| {
                    let Value::Boolean(value) = value else {
                        anyhow::bail!("nonboolean predicate");
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
                let ordering = match (&left, &right) {
                    (Value::Numeric(left), Value::Integer(right)) => {
                        left.cmp(&crate::numeric::Numeric::parse(&right.to_string())?)
                    }
                    (Value::Integer(left), Value::Numeric(right)) => {
                        crate::numeric::Numeric::parse(&left.to_string())?.cmp(right)
                    }
                    _ => left.cmp(&right),
                };
                Ok(Some(compare(op, ordering)))
            }
            Self::Not(value) => Ok(value.evaluate(rows)?.map(|value| !value)),
            Self::And(values) => logical(values, rows, true),
            Self::Or(values) => logical(values, rows, false),
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

#[derive(Clone, Serialize)]
#[serde(transparent)]
pub struct Predicate(Expr);
impl Predicate {
    pub(super) const fn new(expr: Expr) -> Self {
        Self(expr)
    }
    pub(crate) fn qualifies(&self, row: &Row) -> Result<bool> {
        Ok(self.0.evaluate((row, row))? == Some(true))
    }
}

impl Predicate {
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
    pub(super) fn is_constant(&self) -> bool {
        match self {
            Self::Value(value) | Self::Null(value, _) => value.is_constant(),
            Self::Compare(_, left, right) => left.is_constant() && right.is_constant(),
            Self::And(values) | Self::Or(values) => values.iter().all(Self::is_constant),
            Self::Not(value) => value.is_constant(),
        }
    }
}
