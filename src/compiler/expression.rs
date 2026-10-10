use super::{
    ColumnRef,
    parser::Name,
    syntax::{self, Compare},
};
use crate::transaction::Row;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Value {
    Boolean(bool),
    Integer(i64),
    Uuid([u8; 16]),
    Timestamp(crate::temporal::Timestamp),
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Scalar {
    Column(ColumnRef),
    Remainder(ColumnRef, i64),
    Literal(Option<Value>),
}
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
pub(super) fn bind(expr: syntax::Expr, resolve: &Resolve<'_>) -> Result<Expr> {
    match expr {
        syntax::Expr::Value(value) => Ok(Expr::Value(scalar(value, 16, resolve)?)),
        syntax::Expr::Null(value, not) => {
            let value = match value {
                syntax::Scalar::Column(name) => Scalar::Column(resolve(&name)?),
                _ => anyhow::bail!("SQL type: NULL tests require a column"),
            };
            Ok(Expr::Null(value, not))
        }
        syntax::Expr::Compare(op, left, right) => {
            let oid = column_type(&left, resolve)?
                .or(column_type(&right, resolve)?)
                .context("SQL type: comparison requires a native column")?;
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
                "SQL type: unsupported comparison type"
            );
            Ok(Expr::Compare(op, scalar(left, oid, resolve)?, scalar(right, oid, resolve)?))
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
fn column_type(value: &syntax::Scalar, resolve: &Resolve<'_>) -> Result<Option<u32>> {
    match value {
        syntax::Scalar::Column(name) | syntax::Scalar::Remainder(name, _) => {
            Ok(Some(resolve(name)?.oid))
        }
        _ => Ok(None),
    }
}
fn scalar(value: syntax::Scalar, oid: u32, resolve: &Resolve<'_>) -> Result<Scalar> {
    let value = match value {
        syntax::Scalar::Column(name) => {
            let column = resolve(&name)?;
            ensure!(
                column.oid == oid || (integral(column.oid) && integral(oid)),
                "SQL type: incompatible comparison operands"
            );
            return Ok(Scalar::Column(column));
        }
        syntax::Scalar::Remainder(name, divisor) => {
            let column = resolve(&name)?;
            ensure!(
                integral(column.oid) && integral(oid),
                "SQL type: MOD requires integral operands"
            );
            return Ok(Scalar::Remainder(column, divisor));
        }
        syntax::Scalar::Null => None,
        syntax::Scalar::Boolean(value) if oid == 16 => Some(Value::Boolean(value)),
        syntax::Scalar::Integer(value) if integral(oid) => Some(Value::Integer(value)),
        syntax::Scalar::String(value) if matches!(oid, 1114 | 1184) => {
            Some(Value::Timestamp(crate::temporal::Timestamp::parse(&value, oid)?))
        }
        syntax::Scalar::String(value) if oid == 2950 => Some(Value::Uuid(uuid(&value)?)),
        _ => anyhow::bail!("SQL type: literal does not match native column"),
    };
    Ok(Scalar::Literal(value))
}
const fn integral(oid: u32) -> bool {
    matches!(oid, 20 | 21 | 23)
}
impl Scalar {
    fn evaluate(&self, rows: (&Row, &Row)) -> Result<Option<Value>> {
        let column = match self {
            Self::Column(column) => column,
            Self::Literal(value) => return Ok(value.clone()),
            Self::Remainder(column, divisor) => {
                let value = Self::Column(column.clone()).evaluate(rows)?;
                return value
                    .map(|value| {
                        let Value::Integer(value) = value else {
                            anyhow::bail!("nonintegral MOD input");
                        };
                        // PostgreSQL defines MIN % -1 as zero, without division overflow.
                        Ok(Value::Integer(if *divisor == -1 {
                            0
                        } else {
                            value.checked_rem(*divisor).context("invalid MOD divisor")?
                        }))
                    })
                    .transpose();
            }
        };
        let row = if column.right { rows.1 } else { rows.0 };
        row.get(&column.name)
            .context("compiled predicate column absent")?
            .as_deref()
            .map(|text| match column.oid {
                16 => match text {
                    "t" => Ok(Value::Boolean(true)),
                    "f" => Ok(Value::Boolean(false)),
                    _ => anyhow::bail!("invalid native boolean"),
                },
                20 | 21 | 23 => Ok(Value::Integer(text.parse()?)),
                1114 | 1184 => {
                    Ok(Value::Timestamp(crate::temporal::Timestamp::parse(text, column.oid)?))
                }
                2950 => Ok(Value::Uuid(uuid(text)?)),
                _ => anyhow::bail!("unsupported scalar codec"),
            })
            .transpose()
    }
    fn is_null(&self, rows: (&Row, &Row)) -> Result<bool> {
        match self {
            Self::Column(column) => Ok((if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("compiled predicate column absent")?
                .is_none()),
            Self::Literal(value) => Ok(value.is_none()),
            Self::Remainder(_, _) => Ok(self.evaluate(rows)?.is_none()),
        }
    }
}
impl Expr {
    pub(super) fn evaluate(&self, rows: (&Row, &Row)) -> Result<Option<bool>> {
        match self {
            Self::Value(value) => value
                .evaluate(rows)?
                .map(|value| {
                    let Value::Boolean(value) = value else {
                        anyhow::bail!("nonboolean predicate");
                    };
                    Ok(value)
                })
                .transpose(),
            Self::Null(value, not) => Ok(Some(value.is_null(rows)? != *not)),
            Self::Compare(op, left, right) => {
                let (Some(left), Some(right)) = (left.evaluate(rows)?, right.evaluate(rows)?)
                else {
                    return Ok(None);
                };
                let ordering = left.cmp(&right);
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
fn uuid(text: &str) -> Result<[u8; 16]> {
    let text = text.strip_prefix('{').and_then(|text| text.strip_suffix('}')).unwrap_or(text);
    let mut digits = String::new();
    for byte in text.bytes() {
        if byte == b'-' {
            ensure!(!digits.is_empty() && digits.len().is_multiple_of(4), "invalid UUID separator");
        } else {
            ensure!(byte.is_ascii_hexdigit(), "invalid UUID digit");
            digits.push(char::from(byte));
        }
    }
    ensure!(
        digits.len() == 32 && !text.ends_with('-') && !text.contains("--"),
        "invalid UUID length"
    );
    let mut value = [0_u8; 16];
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16)?;
    }
    Ok(value)
}

impl Expr {
    pub(super) fn has_remainder(&self) -> bool {
        let scalar = |value: &Scalar| matches!(value, Scalar::Remainder(_, _));
        match self {
            Self::Value(value) | Self::Null(value, _) => scalar(value),
            Self::Compare(_, left, right) => scalar(left) || scalar(right),
            Self::And(values) | Self::Or(values) => values.iter().any(Self::has_remainder),
            Self::Not(value) => value.has_remainder(),
        }
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
