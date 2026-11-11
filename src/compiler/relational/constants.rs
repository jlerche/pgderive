//! Fold a planned multirow VALUES list before lowering the bootstrap operator.
use super::Node;
use crate::transaction::Row;
use anyhow::{Context, Result};
use std::mem;

const BYTE_BUDGET: usize = 16 * 1024 * 1024;

impl Node {
    pub(in crate::compiler) fn materialize_values(&mut self) -> Result<()> {
        let Self::Values { id, input, columns, rows } = self else { return Ok(()) };
        let mut remaining = BYTE_BUDGET;
        let empty = Row::new();
        let mut encoded = Vec::with_capacity(rows.len());
        for expressions in rows {
            let mut row = Row::new();
            for (column, expression) in columns.iter().zip(expressions) {
                let text = expression.fold()?.evaluate(&empty)?;
                remaining = remaining
                    .checked_sub(text.as_ref().map_or(0, String::len))
                    .context("VALUES data exceeds 16 MiB constant byte budget")?;
                row.insert(column.reference.name.clone(), text);
            }
            encoded.push(row);
        }
        *self = Self::Constant {
            id: mem::take(id),
            input: mem::take(input),
            rows: encoded,
            columns: mem::take(columns),
        };
        Ok(())
    }
}
