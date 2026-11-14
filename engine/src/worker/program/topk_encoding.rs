//! Native SQL comparison tokens and complete logical-row secondary ordering.
use super::Spec;
use crate::{
    compiler::{ColumnRef, value::Value},
    transaction::Row,
};
use anyhow::{Context, Result};
use std::fmt::Write;

fn scalar(oid: u32, text: &str) -> Result<String> {
    Ok(match crate::compiler::value::decode(oid, text)? {
        Value::Boolean(value) => if value { "1" } else { "0" }.into(),
        Value::Integer(value) => format!("{:016x}", super::super::order::biased(value, false)),
        Value::Timestamp(value) => {
            format!("{:016x}", super::super::order::biased(value.sort_value(), false))
        }
        Value::Date(value) => {
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
        Value::Interval(value) => {
            let (category, duration) = value.duration_key();
            let key = u128::from_be_bytes(duration.to_be_bytes()) ^ (1_u128 << 127);
            format!("{}{key:032x}", category + 1)
        }
    })
}

fn append(
    token: &mut String,
    column: &ColumnRef,
    row: &Row,
    descending: bool,
    nulls_first: bool,
) -> Result<()> {
    let value = row.get(&column.name).context("missing top-k ordering field")?;
    append_scalar(token, column.oid, value.as_deref(), descending, nulls_first)
}

pub(in crate::worker::program) fn append_scalar(
    token: &mut String,
    oid: u32,
    value: Option<&str>,
    descending: bool,
    nulls_first: bool,
) -> Result<()> {
    let Some(value) = value else {
        token.push(if nulls_first { '0' } else { '2' });
        return Ok(());
    };
    token.push('1');
    let value = scalar(oid, value)?;
    for byte in value.bytes() {
        let byte = if descending { !byte } else { byte };
        write!(token, "{byte:02x}")?;
    }
    // Termination reverses too, so variable-length values and prefixes preserve
    // their native order in both directions and across following sort fields.
    token.push(if descending { 'g' } else { '/' });
    Ok(())
}

pub(super) fn peer_key(spec: &Spec, row: &Row) -> Result<String> {
    let mut token = String::new();
    for order in &spec.order {
        append(&mut token, &order.column, row, order.descending, order.nulls_first)?;
    }
    Ok(token)
}

pub(super) fn encoding(spec: &Spec, row: &Row) -> Result<String> {
    let mut token = peer_key(spec, row)?;
    for column in &spec.identity {
        if spec.order.iter().any(|order| order.column == *column) {
            continue;
        }
        append(&mut token, column, row, false, true)?;
    }
    Ok(token)
}
