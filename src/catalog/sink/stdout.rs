//! Diagnostic SQL transport. Printed attempts are not atomic with `PostgreSQL` COMMIT.
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{self, Write};

/// Selected result transport, persisted in the destination binding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkOutput {
    /// Maintain an owned ordinary `PostgreSQL` table.
    #[default]
    Postgres,
    /// Print attempted `PostgreSQL` DML and parameters without executing it.
    Stdout,
}
impl SinkOutput {
    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde skip_serializing_if requires a borrowed predicate"
    )]
    pub(super) fn is_postgres(&self) -> bool {
        *self == Self::Postgres
    }
}

pub(super) fn emit(context: &Value, sql: &str, payload: &Value, occurrences: u64) -> Result<()> {
    let mut output = io::stdout().lock();
    write(&mut output, context, sql, payload, occurrences)?;
    output.flush()?;
    Ok(())
}
fn write(
    output: &mut impl Write,
    context: &Value,
    sql: &str,
    payload: &Value,
    occurrences: u64,
) -> Result<()> {
    serde_json::to_writer(
        &mut *output,
        &json!({
            "event":"sink_batch", "version":1, "status":"attempted",
            "context":context, "sql":sql, "parameters":[payload], "occurrences":occurrences
        }),
    )?;
    writeln!(output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_bindings_keep_their_original_json() -> Result<()> {
        let value = json!({"table":"results","columns":[]});
        let mut sink: crate::catalog::Sink = serde_json::from_value(value.clone())?;
        assert_eq!(sink.output, SinkOutput::Postgres);
        assert_eq!(serde_json::to_value(&sink)?, value);
        sink.output = SinkOutput::Stdout;
        assert_eq!(serde_json::to_value(&sink)?["output"], "stdout");
        assert!(serde_json::from_value::<SinkOutput>(json!("invalid")).is_err());
        Ok(())
    }
    #[test]
    fn preserves_parameters_and_reports_write_failures() -> Result<()> {
        let payload = json!([[[null, ["1.00", "a'\n🙂"]], 2]]);
        let mut buffer = Vec::new();
        write(&mut buffer, &json!({"time":2}), "INSERT $1", &payload, 2)?;
        let event: Value = serde_json::from_slice(&buffer)?;
        assert_eq!(event["parameters"], json!([payload]));
        assert_eq!(event["status"], "attempted");
        assert!(buffer.ends_with(b"\n"));
        assert!(write(&mut io::sink(), &Value::Null, "", &Value::Null, 0).is_ok());
        assert!(
            write(&mut io::Cursor::new(&mut [0_u8; 0][..]), &Value::Null, "", &Value::Null, 0)
                .is_err()
        );
        Ok(())
    }
}
