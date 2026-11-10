//! NULLIF returns the first argument of the resolved equality operator.
use super::{ColumnRef, Name, Typed, Value};
use crate::{
    compiler::{
        expression::{check_comparison, compare_order, comparison_operands},
        parser::expressions::{names, optional},
        syntax::Compare,
        value::Value as Native,
    },
    transaction::Row,
};
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;

pub(super) fn bind(
    value: &pg::AExpr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    ensure!(names(&value.name)?.0 == ["="], "invalid NULLIF operator");
    let left = super::operand::bind_at(optional(value.lexpr.as_deref())?, resolve, depth + 1)?;
    let right = super::operand::bind_at(optional(value.rexpr.as_deref())?, resolve, depth + 1)?;
    let (mut left, mut right) = comparison_operands(left, right)?;
    check_comparison(&Compare::Eq, &left, &right, None)?;
    if left.oid == 1700 && matches!(right.oid, 20 | 21 | 23) {
        right = right.widened_to_numeric();
    } else if right.oid == 1700 && matches!(left.oid, 20 | 21 | 23) {
        left = left.widened_to_numeric();
    }
    // Varchar equality uses the text operator. Integral and temporal equality
    // have cross-type operators, so their first argument keeps its type.
    if left.oid == 1043 {
        left = as_text(left);
    }
    if right.oid == 1043 {
        right = as_text(right);
    }
    Ok(Typed {
        oid: left.oid,
        value: Value::NullIf { left: Box::new(left), right: Box::new(right) },
    })
}

fn as_text(input: Typed) -> Typed {
    Typed { oid: 25, value: Value::Cast { input: Box::new(input), typmod: None } }
}

pub(super) fn evaluate(left: &Typed, right: &Typed, rows: (&Row, &Row)) -> Result<Option<Native>> {
    let (left, right) = (left.native_pair(rows)?, right.native_pair(rows)?);
    if let (Some(left), Some(right)) = (&left, &right)
        && compare_order(left, right)?.is_eq()
    {
        return Ok(None);
    }
    Ok(left)
}
