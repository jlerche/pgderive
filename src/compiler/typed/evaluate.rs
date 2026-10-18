//! One native evaluator; SQL control flow selects values before result coercion.
use super::{Typed, Value, check_width};
use crate::{
    compiler::value::{self, Value as Native},
    numeric::Numeric,
    transaction::Row,
};
use anyhow::{Context, Result};
impl Typed {
    pub(in crate::compiler) fn native_pair(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        self.native_value(rows)?
            .map(|value| {
                if self.oid == 1700 {
                    return Ok(Native::Numeric(numeric(value)?));
                }
                if let Native::Integer(integer) = &value {
                    check_width(*integer, self.oid)?;
                }
                Ok(value)
            })
            .transpose()
    }
    fn native_value(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        Ok(match &self.value {
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("compiled scalar column absent")?
                .as_deref()
                .map(|text| value::decode(column.oid, text))
                .transpose()?,
            Value::Literal(value) => value.map(Native::Integer),
            Value::Numeric(value) => Some(Native::Numeric(Numeric::parse(value)?)),
            Value::Text(value) => Some(Native::Text(value.clone())),
            Value::Encoded(value) => {
                value.as_deref().map(|text| value::decode(self.oid, text)).transpose()?
            }
            Value::Interval(value) => Some(Native::Interval(*value)),
            Value::Difference(left, right) => {
                let (Some(Native::Timestamp(left)), Some(Native::Timestamp(right))) = (
                    Self::column(left.clone()).native_pair(rows)?,
                    Self::column(right.clone()).native_pair(rows)?,
                ) else {
                    return Ok(None);
                };
                Some(Native::Interval(left.difference(right)?))
            }
            Value::Average(column) => rows
                .0
                .get(&column.name)
                .context("missing intermediate average")?
                .as_deref()
                .map(|text| -> Result<_> {
                    Ok(Native::Numeric(Numeric::parse(&Numeric::average(text)?)?))
                })
                .transpose()?,
            Value::Remainder(value) => value.integer_pair(rows)?.map(Native::Integer),
            Value::Temporal(value) => value.evaluate(rows.0)?.map(Native::Timestamp),
            Value::Call(call) => {
                call.evaluate(rows)?.map(|text| value::decode(self.oid, &text)).transpose()?
            }
            Value::Cast(input) => input.native_pair(rows)?,
            Value::Binary { operation, left, right } => {
                let (left, right) = (left.native_pair(rows)?, right.native_pair(rows)?);
                match (left, right) {
                    (Some(left), Some(right)) if self.oid == 1700 => Some(Native::Numeric(
                        numeric(left)?.binary(&numeric(right)?, operation.symbol())?,
                    )),
                    (Some(Native::Integer(left)), Some(Native::Integer(right))) => {
                        Some(Native::Integer(
                            operation
                                .integer(left, right)
                                .context("integral arithmetic overflow")?,
                        ))
                    }
                    (None, _) | (_, None) => None,
                    _ => anyhow::bail!("invalid arithmetic operands"),
                }
            }
            Value::Coalesce(_) | Value::Case { .. } => self.selected(rows)?,
        })
    }
    fn selected(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        match &self.value {
            Value::Coalesce(arguments) => {
                for argument in arguments {
                    let value = argument.native_pair(rows)?;
                    if value.is_some() {
                        return Ok(value);
                    }
                }
                Ok(None)
            }
            Value::Case { branches, otherwise } => {
                for (condition, result) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        return result.native_pair(rows);
                    }
                }
                otherwise.native_pair(rows)
            }
            _ => anyhow::bail!("invalid scalar control flow"),
        }
    }
    pub(super) fn text_pair(&self, rows: (&Row, &Row)) -> Result<Option<String>> {
        self.native_pair(rows)?.map(|value| value.text(self.oid)).transpose()
    }
}
fn numeric(value: Native) -> Result<Numeric> {
    match value {
        Native::Numeric(value) => Ok(value),
        Native::Integer(value) => Numeric::parse(&value.to_string()),
        _ => anyhow::bail!("invalid numeric value"),
    }
}
