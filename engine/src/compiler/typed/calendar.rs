//! Temporal overload resolution and evaluation share the native calendar kernels.
use super::{Typed, Value};
use crate::compiler::{capabilities::Arithmetic, value::Value as Native};
use anyhow::{Result, ensure};
pub(super) fn oid(left: u32, right: u32, operation: Arithmetic) -> Result<Option<u32>> {
    if ![left, right].iter().any(|oid| matches!(oid, 1082 | 1114 | 1184 | 1186)) {
        return Ok(None);
    }
    let add = matches!(operation, Arithmetic::Add);
    ensure!(
        add || matches!(operation, Arithmetic::Subtract),
        "unsupported temporal arithmetic operator"
    );
    let result = match (left, right, add) {
        (1082, 21 | 23, true | false) | (21 | 23, 1082, true) => 1082,
        (1082, 1082 | 0, false) => 23,
        (1082, 1186, _) | (1186, 1082, true) => 1114,
        (1114 | 1184, 1186, _) => left,
        (1186, 1114 | 1184, true) => right,
        (1114 | 1184, 1114 | 1184, false) | (1186, 1186, _) => 1186,
        _ => anyhow::bail!("unsupported or ambiguous temporal arithmetic overload"),
    };
    Ok(Some(result))
}
pub(super) fn evaluate(left: Native, right: Native, operation: Arithmetic) -> Result<Native> {
    let subtract = matches!(operation, Arithmetic::Subtract);
    Ok(match (left, right) {
        (Native::Date(left), Native::Integer(right)) => {
            Native::Date(left.add_days(if subtract { -right } else { right })?)
        }
        (Native::Integer(left), Native::Date(right)) => Native::Date(right.add_days(left)?),
        (Native::Date(left), Native::Date(right)) => {
            Native::Integer(i64::from(left.difference(right)?))
        }
        (Native::Date(left), Native::Interval(right)) => {
            Native::Timestamp(left.timestamp()?.add_interval(if subtract {
                right.negate()?
            } else {
                right
            })?)
        }
        (Native::Interval(left), Native::Date(right)) => {
            Native::Timestamp(right.timestamp()?.add_interval(left)?)
        }
        (Native::Timestamp(left), Native::Timestamp(right)) => {
            Native::Interval(left.difference(right)?)
        }
        (Native::Timestamp(left), Native::Interval(right)) => {
            Native::Timestamp(left.add_interval(if subtract { right.negate()? } else { right })?)
        }
        (Native::Interval(left), Native::Timestamp(right)) => {
            Native::Timestamp(right.add_interval(left)?)
        }
        (Native::Interval(left), Native::Interval(right)) => {
            Native::Interval(left.binary(right, subtract)?)
        }
        _ => {
            return Err(crate::engine::execution::PlanFault(
                "invalid temporal arithmetic operands",
            )
            .into());
        }
    })
}
pub(super) fn cast(value: Native, source: u32, oid: u32) -> Result<Native> {
    Ok(match (value, oid) {
        (Native::Date(value), 1114 | 1184) => Native::Timestamp(value.timestamp()?),
        (Native::Timestamp(value), 1082) => Native::Date(value.date()?),
        (Native::Timestamp(value), 1114 | 1184) if source != oid => {
            Native::Timestamp(value.cast_zone()?)
        }
        (value, _) => value,
    })
}
pub(super) const fn zero_interval() -> Typed {
    Typed { oid: 1186, value: Value::Interval(crate::temporal::Interval::duration(0)) }
}
