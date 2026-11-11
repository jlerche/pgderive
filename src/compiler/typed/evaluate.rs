//! One native evaluator; SQL control flow selects values before result coercion.
use super::{Typed, Value, check_width, types::Modifier};
use crate::{
    compiler::value::{self, Value as Native},
    engine::execution::{EvaluationFailed, PlanFault},
    numeric::Numeric,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
impl Typed {
    pub(in crate::compiler) fn native_pair(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        self.native_result(rows).map_err(|error| {
            if error.is::<EvaluationFailed>() || error.is::<PlanFault>() {
                error
            } else {
                error.context(EvaluationFailed)
            }
        })
    }
    fn native_result(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
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
            Value::Boolean(value) => value.evaluate(rows)?.map(Native::Boolean),
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context(PlanFault("compiled scalar column absent"))?
                .as_deref()
                .map(|text| value::decode(column.oid, text))
                .transpose()?,
            Value::Literal(value) => value.map(Native::Integer),
            Value::Numeric(value) => Some(Native::Numeric(Numeric::parse_literal(value)?)),
            Value::Text(value) => Some(Native::Text(value.clone())),
            Value::Encoded(value) => {
                value.as_deref().map(|text| value::decode(self.oid, text)).transpose()?
            }
            Value::Interval(value) => Some(Native::Interval(*value)),
            Value::Average(column) => rows
                .0
                .get(&column.name)
                .context(PlanFault("missing intermediate average"))?
                .as_deref()
                .map(|text| -> Result<_> { Ok(Native::Numeric(Numeric::average(text)?)) })
                .transpose()?,
            Value::Temporal(value) => value.evaluate(rows.0)?.map(Native::Timestamp),
            Value::Call(call) => {
                call.evaluate(rows)?.map(|text| value::decode(self.oid, &text)).transpose()?
            }
            Value::Collate { input, .. } => input.native_pair(rows)?,
            Value::Cast { input, typmod } => input
                .native_pair(rows)?
                .map(|value| {
                    let typmod =
                        typmod.filter(|modifier| modifier.requires_coercion(input, self.oid));
                    cast(value, input.oid, self.oid, typmod)
                })
                .transpose()?,
            Value::Binary { operation, left, right } => {
                let (left, right) = (left.native_pair(rows)?, right.native_pair(rows)?);
                match (left, right) {
                    (Some(left), Some(right)) if self.oid == 1700 => Some(Native::Numeric(
                        numeric(left)?.binary(&numeric(right)?, operation.symbol())?,
                    )),
                    (Some(Native::Integer(left)), Some(Native::Integer(right))) => {
                        Some(Native::Integer(integer(*operation, left, right, self.oid)?))
                    }
                    (None, _) | (_, None) => None,
                    (Some(left), Some(right))
                        if matches!(
                            left,
                            Native::Date(_) | Native::Timestamp(_) | Native::Interval(_)
                        ) || matches!(
                            right,
                            Native::Date(_) | Native::Timestamp(_) | Native::Interval(_)
                        ) =>
                    {
                        Some(super::calendar::evaluate(left, right, *operation)?)
                    }
                    _ => return Err(PlanFault("invalid arithmetic operands").into()),
                }
            }
            Value::Concat { left, right } => {
                concatenate(left.native_pair(rows)?, right.native_pair(rows)?)?
            }
            Value::NullIf { left, right } => super::nullif::evaluate(left, right, rows)?,
            Value::Coalesce(_)
            | Value::Case { .. }
            | Value::SimpleCase(_)
            | Value::MinMax { .. } => self.selected(rows)?,
        })
    }
    fn selected(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        match &self.value {
            Value::SimpleCase(case) => case.evaluate(rows),
            Value::MinMax { operation, arguments } => {
                super::minmax::evaluate(*operation, arguments, rows)
            }
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
            _ => Err(PlanFault("invalid scalar control flow").into()),
        }
    }
    pub(super) fn text_pair(&self, rows: (&Row, &Row)) -> Result<Option<String>> {
        self.native_pair(rows)?.map(|value| value.text(self.oid)).transpose()
    }
}
fn integer(
    operation: crate::compiler::capabilities::Arithmetic,
    left: i64,
    right: i64,
    oid: u32,
) -> Result<i64> {
    use crate::compiler::capabilities::Arithmetic;
    ensure!(
        !matches!(operation, Arithmetic::Divide | Arithmetic::Remainder) || right != 0,
        "division by zero"
    );
    if operation.is_shift() {
        return super::bitwise::shift(operation, left, right, oid);
    }
    operation.integer(left, right).context("integral arithmetic overflow")
}

fn cast(value: Native, source: u32, oid: u32, typmod: Option<Modifier>) -> Result<Native> {
    if matches!(oid, 20 | 21 | 23) && matches!(value, Native::Numeric(_)) {
        return Ok(Native::Integer(numeric(value)?.integer()?));
    }
    if let Some(Modifier::Numeric(typmod)) = typmod {
        return Ok(Native::Numeric(numeric(value)?.constrained(typmod)?));
    }
    if oid == 25 {
        match value {
            // PostgreSQL's explicit Boolean cast uses text(bool), which differs from boolout.
            Native::Boolean(value) => return Ok(Native::Text(value.to_string())),
            Native::Integer(_) | Native::Uuid(_) => return Ok(Native::Text(value.text(oid)?)),
            _ => {}
        }
    }
    let value = super::calendar::cast(value, source, oid)?;
    Ok(match (value, typmod) {
        (Native::Text(mut value), Some(Modifier::Varchar(length))) => {
            if let Some((end, _)) = value.char_indices().nth(usize::try_from(length)?) {
                value.truncate(end);
            }
            Native::Text(value)
        }
        (Native::Timestamp(value), Some(Modifier::Timestamp(precision))) => {
            Native::Timestamp(value.rounded(precision)?)
        }
        (Native::Interval(value), Some(Modifier::Interval { precision, .. })) => {
            Native::Interval(value.rounded(precision)?)
        }
        (value, None) => value,
        _ => return Err(PlanFault("cast modifier does not match the native value").into()),
    })
}

fn concatenate(left: Option<Native>, right: Option<Native>) -> Result<Option<Native>> {
    match (left, right) {
        (Some(Native::Text(mut left)), Some(Native::Text(right))) => {
            ensure!(
                left.len()
                    .checked_add(right.len())
                    .is_some_and(|size| size <= super::TEXT_OUTPUT_LIMIT),
                "concatenation output exceeds 16 MiB engine budget"
            );
            left.try_reserve_exact(right.len()).context("allocate text concatenation")?;
            left.push_str(&right);
            Ok(Some(Native::Text(left)))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(PlanFault("invalid concatenation operands").into()),
    }
}

fn numeric(value: Native) -> Result<Numeric> {
    match value {
        Native::Numeric(value) => Ok(value),
        Native::Integer(value) => Numeric::parse(&value.to_string()),
        _ => Err(PlanFault("invalid numeric value").into()),
    }
}
