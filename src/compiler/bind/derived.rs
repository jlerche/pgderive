//! Resolve the labels exposed by an independently bound SQL scope.
use crate::compiler::{ColumnRef, projection::OutputColumn};
use anyhow::{Context, Result, ensure};
pub(super) fn resolve_output(
    name: &crate::compiler::parser::Name,
    alias: &str,
    columns: &[OutputColumn],
) -> Result<ColumnRef> {
    let label = match name.0.as_slice() {
        [label] => label,
        [qualifier, label] if qualifier == alias => label,
        _ => anyhow::bail!("unknown derived qualifier"),
    };
    let mut candidates = columns.iter().filter(|column| column.label == *label);
    let candidate = candidates.next().context("unknown derived column")?;
    ensure!(candidates.next().is_none(), "ambiguous derived column");
    Ok(candidate.column.clone())
}
