//! Native SQL comparison tokens and complete logical-row secondary ordering.
use super::Spec;
use crate::{
    compiler::{ColumnRef, value::Value},
    transaction::Row,
};
use anyhow::{Context, Result};
use std::fmt::Write;

fn scalar(column: &ColumnRef, text: &str) -> Result<String> {
    Ok(match crate::compiler::value::decode(column.oid, text)? {
        Value::Boolean(value) => if value { "1" } else { "0" }.into(),
        Value::Integer(value) => format!("{:016x}", super::super::order::biased(value, false)),
        Value::Timestamp(value) => {
            format!("{:016x}", super::super::order::biased(value.sort_value(), false))
        }
        Value::Numeric(value) => value.order_key(),
        Value::Text(value) => value,
        Value::Uuid(value) => {
            let mut text = String::new();
            for byte in value {
                write!(text, "{byte:02x}")?;
            }
            text
        }
        Value::Interval(_) => anyhow::bail!("interval ordering is unsupported"),
    })
}

fn append(
    token: &mut String,
    column: &ColumnRef,
    row: &Row,
    descending: bool,
    nulls_first: bool,
) -> Result<()> {
    let Some(value) = row.get(&column.name).context("missing top-k ordering field")? else {
        token.push(if nulls_first { '0' } else { '2' });
        return Ok(());
    };
    token.push('1');
    let value = scalar(column, value)?;
    for byte in value.bytes() {
        let byte = if descending { !byte } else { byte };
        write!(token, "{byte:02x}")?;
    }
    // Termination reverses too, so variable-length values and prefixes preserve
    // their native order in both directions and across following sort fields.
    token.push(if descending { 'g' } else { '/' });
    Ok(())
}

pub(super) fn encoding(spec: &Spec, row: &Row) -> Result<String> {
    let mut token = String::new();
    for order in &spec.order {
        append(&mut token, &order.column, row, order.descending, order.nulls_first)?;
    }
    for column in &spec.identity {
        if spec.order.iter().any(|order| order.column == *column) {
            continue;
        }
        append(&mut token, column, row, false, true)?;
    }
    Ok(token)
}
