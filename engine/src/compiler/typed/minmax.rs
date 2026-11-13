//! Eager numeric GREATEST and LEAST, preserving the first equal representation.
use super::{ColumnRef, Name, Typed, Value, types::numeric_common};
use crate::{
    compiler::{expression::compare_order, value::Value as Native},
    transaction::Row,
};
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Operation {
    Greatest,
    Least,
}

pub(super) fn bind(
    expression: &pg::MinMaxExpr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    ensure!(
        expression.xpr.is_none()
            && expression.minmaxtype == 0
            && expression.minmaxcollid == 0
            && expression.inputcollid == 0
            && (1..=64).contains(&expression.args.len()),
        "invalid scalar GREATEST/LEAST"
    );
    let operation = match pg::MinMaxOp::try_from(expression.op)? {
        pg::MinMaxOp::IsGreatest => Operation::Greatest,
        pg::MinMaxOp::IsLeast => Operation::Least,
        pg::MinMaxOp::Undefined => anyhow::bail!("invalid GREATEST/LEAST operator"),
    };
    let arguments = expression
        .args
        .iter()
        .map(|value| super::bind(value, resolve, depth + 1))
        .collect::<Result<Vec<_>>>()?;
    let types = arguments.iter().map(|value| value.oid).collect::<Vec<_>>();
    // With only undecorated NULL arguments, PostgreSQL resolves to text.
    if types.iter().all(|oid| *oid == 0) {
        return Ok(Typed::null_text());
    }
    ensure!(
        types.iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 705 | 1700))
            && types.iter().any(|oid| matches!(oid, 20 | 21 | 23 | 1700)),
        "GREATEST/LEAST require integral or numeric arguments"
    );
    let oid = numeric_common(&types);
    let arguments = arguments
        .into_iter()
        .map(|input| match input.oid {
            0 | 705 => Typed::contextual_literal(input, oid),
            actual if actual == oid => Ok(input),
            _ => Ok(Typed { oid, value: Value::Cast { input: Box::new(input), typmod: None } }),
        })
        .collect::<Result<_>>()?;
    Ok(Typed { oid, value: Value::MinMax { operation, arguments } })
}

pub(super) fn evaluate(
    operation: Operation,
    arguments: &[Typed],
    rows: (&Row, &Row),
) -> Result<Option<Native>> {
    let replace = match operation {
        Operation::Greatest => Ordering::Less,
        Operation::Least => Ordering::Greater,
    };
    let mut selected = None;
    for argument in arguments {
        let Some(candidate) = argument.native_pair(rows)? else { continue };
        if let Some(current) = &selected
            && compare_order(current, &candidate)? != replace
        {
            continue;
        }
        selected = Some(candidate);
    }
    Ok(selected)
}
