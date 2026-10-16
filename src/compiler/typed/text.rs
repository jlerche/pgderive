//! Text/Boolean output evaluation preserves lazy CASE/COALESCE selection.
use super::{Typed, Value};
use crate::transaction::Row;
use anyhow::{Context, Result};
impl Typed {
    pub(super) fn string_pair(&self, rows: (&Row, &Row)) -> Result<Option<String>> {
        match &self.value {
            Value::Column(column) => Ok((if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("missing typed text input")?
                .clone()),
            Value::Text(value) => Ok(Some(value.clone())),
            Value::Literal(None) => Ok(None),
            Value::Cast(input) => input.text_pair(rows),
            Value::Call(call) => call.evaluate(rows),
            Value::Coalesce(arguments) => {
                for argument in arguments {
                    let value = argument.text_pair(rows)?;
                    if value.is_some() {
                        return Ok(value);
                    }
                }
                Ok(None)
            }
            Value::Case { branches, otherwise } => {
                for (condition, value) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        return value.text_pair(rows);
                    }
                }
                otherwise.text_pair(rows)
            }
            _ => anyhow::bail!("invalid text/boolean expression"),
        }
    }
}
