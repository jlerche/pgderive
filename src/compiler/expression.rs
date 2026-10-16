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
    Text(String),
    Integer(i64),
    Numeric(crate::numeric::Numeric),
    Uuid([u8; 16]),
    Timestamp(crate::temporal::Timestamp),
    Interval(crate::temporal::Gap),
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Scalar {
    Typed(Box<super::typed::Typed>),
    Column(ColumnRef),
    Remainder(ColumnRef, i64),
    Difference(ColumnRef, ColumnRef),
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
pub fn native_key(column: &ColumnRef, row: &Row) -> Result<Option<String>> {
    Scalar::Column(column.clone())
        .evaluate((row, row))?
        .map(|value| match value {
            Value::Numeric(value) => serde_json::to_string(&value.key()).map_err(Into::into),
            _ => serde_json::to_string(&value).map_err(Into::into),
        })
        .transpose()
}
pub(super) fn integral_remainder(
    column: &ColumnRef,
    divisor: i64,
    row: &Row,
) -> Result<Option<String>> {
    Scalar::Remainder(column.clone(), divisor)
        .evaluate((row, row))?
        .map(|value| {
            let Value::Integer(value) = value else { anyhow::bail!("nonintegral MOD result") };
            Ok(value.to_string())
        })
        .transpose()
}
pub(super) fn bind(expr: syntax::Expr, resolve: &Resolve<'_>) -> Result<Expr> {
    match expr {
        syntax::Expr::Value(value) => Ok(Expr::Value(scalar(value, 16, resolve)?)),
        syntax::Expr::Null(value, not) => {
            let value = match value {
                syntax::Scalar::Column(name) => Scalar::Column(resolve(&name)?),
                syntax::Scalar::Typed(value) => {
                    Scalar::Typed(Box::new(super::typed::bind(&value, &|name| resolve(name), 0)?))
                }
                _ => anyhow::bail!("SQL type: NULL tests require a column or typed expression"),
            };
            Ok(Expr::Null(value, not))
        }
        syntax::Expr::Compare(op, left, right) => {
            let oid = column_type(&left, resolve)?
                .or(column_type(&right, resolve)?)
                .context("SQL type: comparison requires a native column")?;
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 1700 | 25 | 1043 | 2950 | 1114 | 1184 | 1186),
                "SQL type: unsupported comparison type"
            );
            if matches!(oid, 25 | 1043) {
                ensure!(
                    matches!(op, Compare::Eq | Compare::Ne)
                        && (column_type(&left, resolve)?.is_some()
                            != column_type(&right, resolve)?.is_some()),
                    "text equality requires one native column and an unknown literal; text ordering and column collation resolution are unsupported"
                );
            }
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
        syntax::Scalar::Typed(value) => {
            Ok(Some(super::typed::bind(value, &|name| resolve(name), 0)?.oid()))
        }
        syntax::Scalar::Difference(left, right) => {
            let left = resolve(left)?;
            let right = resolve(right)?;
            Ok(Some(
                if (integral(left.oid) || left.oid == 1700)
                    && (integral(right.oid) || right.oid == 1700)
                {
                    super::typed::difference(left, right).oid()
                } else {
                    1186
                },
            ))
        }
        _ => Ok(None),
    }
}
fn scalar(value: syntax::Scalar, oid: u32, resolve: &Resolve<'_>) -> Result<Scalar> {
    let value = match value {
        syntax::Scalar::Typed(value) => {
            let value = super::typed::bind(&value, &|name| resolve(name), 0)?;
            ensure!(
                (integral(oid) || oid == 1700) && (integral(value.oid()) || value.oid() == 1700),
                "typed integral operand required"
            );
            return Ok(Scalar::Typed(Box::new(value)));
        }
        syntax::Scalar::Column(name) => {
            let column = resolve(&name)?;
            ensure!(
                column.oid == oid
                    || ((integral(column.oid) || column.oid == 1700)
                        && (integral(oid) || oid == 1700)),
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
        syntax::Scalar::Difference(left, right) => {
            let (left, right) = (resolve(&left)?, resolve(&right)?);
            if (integral(left.oid) || left.oid == 1700)
                && (integral(right.oid) || right.oid == 1700)
                && (integral(oid) || oid == 1700)
            {
                return Ok(Scalar::Typed(Box::new(super::typed::difference(left, right))));
            }
            ensure!(
                oid == 1186 && matches!(left.oid, 1114 | 1184) && left.oid == right.oid,
                "timestamp subtraction requires same-type native timestamp operands"
            );
            return Ok(Scalar::Difference(left, right));
        }
        syntax::Scalar::Interval(value) if oid == 1186 => {
            Some(Value::Interval(crate::temporal::Gap::Finite(value)))
        }
        syntax::Scalar::Integer(value) if oid == 1700 => {
            Some(Value::Numeric(crate::numeric::Numeric::parse(&value.to_string())?))
        }
        syntax::Scalar::String(value) if oid == 1700 => {
            Some(Value::Numeric(crate::numeric::Numeric::parse(&value)?))
        }
        syntax::Scalar::Null => None,
        syntax::Scalar::Boolean(value) if oid == 16 => Some(Value::Boolean(value)),
        syntax::Scalar::Integer(value) if integral(oid) => Some(Value::Integer(value)),
        syntax::Scalar::String(value) if matches!(oid, 1114 | 1184) => {
            Some(Value::Timestamp(crate::temporal::Timestamp::parse(&value, oid)?))
        }
        syntax::Scalar::String(value) if matches!(oid, 25 | 1043) => Some(Value::Text(value)),
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
            Self::Typed(value) if value.oid() == 1700 => {
                return value
                    .text_pair(rows)?
                    .map(|v| Ok(Value::Numeric(crate::numeric::Numeric::parse(&v)?)))
                    .transpose();
            }
            Self::Typed(value) => return Ok(value.evaluate_pair(rows)?.map(Value::Integer)),
            Self::Difference(left, right) => {
                let (Some(Value::Timestamp(left)), Some(Value::Timestamp(right))) = (
                    Self::Column(left.clone()).evaluate(rows)?,
                    Self::Column(right.clone()).evaluate(rows)?,
                ) else {
                    return Ok(None);
                };
                return Ok(Some(Value::Interval(left.difference(right)?)));
            }
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
                1700 => Ok(Value::Numeric(crate::numeric::Numeric::parse(text)?)),
                25 | 1043 => Ok(Value::Text(text.to_owned())),
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
            Self::Typed(_) | Self::Remainder(_, _) | Self::Difference(_, _) => {
                Ok(self.evaluate(rows)?.is_none())
            }
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
        let (compare, column, limit) = match (left, right) {
            (Scalar::Column(column), Scalar::Literal(Some(Value::Integer(limit)))) => {
                (compare.clone(), column, *limit)
            }
            (Scalar::Literal(Some(Value::Integer(limit))), Scalar::Column(column)) => {
                let reversed = match compare {
                    Compare::Eq => Compare::Eq,
                    Compare::Lt => Compare::Gt,
                    Compare::Le => Compare::Ge,
                    Compare::Gt => Compare::Lt,
                    Compare::Ge => Compare::Le,
                    Compare::Ne => Compare::Ne,
                };
                (reversed, column, *limit)
            }
            _ => return None,
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

impl Scalar {
    fn is_constant(&self) -> bool {
        match self {
            Self::Typed(value) => value.is_constant(),
            Self::Literal(_) => true,
            Self::Column(_) | Self::Remainder(_, _) | Self::Difference(_, _) => false,
        }
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
