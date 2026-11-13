//! Materialize SQL values before a downstream relational scope consumes them.
use crate::compiler::{
    relational::{Node, Relational},
    scalar,
    typed::Typed,
};
use anyhow::{Context, Result};
pub(super) fn finalize(ir: &mut Relational, averages: &[String]) -> Result<()> {
    let mut computed = Vec::new();
    let prefix = format!("@boundary_{}_", ir.nodes.len());
    for column in &mut ir.output.columns {
        if averages.contains(&column.column.name) {
            column.column =
                scalar::register(Typed::average(column.column.clone()), &mut computed, &prefix)?;
        }
    }
    if computed.is_empty() {
        return Ok(());
    }
    let id = format!("boundary_{}", ir.nodes.len());
    let output = ir.nodes.last_mut().context("missing intermediate output")?;
    let Node::Output { input, .. } = output else {
        anyhow::bail!("intermediate scope requires final Output")
    };
    let prior = std::mem::replace(input, id.clone());
    let position = ir.nodes.len() - 1;
    ir.nodes.insert(position, Node::Map { id, input: prior, computed });
    Ok(())
}
/// Keep a materialized CTE's outputs at its boundary, whichever of them its readers use.
pub(super) fn materialize(ir: &mut Relational) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    let fields = ir
        .output
        .columns
        .iter()
        .filter(|output| seen.insert(output.column.name.clone()))
        .map(|output| crate::compiler::relational::Binding {
            input: output.column.clone(),
            // A rebind keeps what its input proves; this flag only adds doubt.
            output: crate::compiler::ColumnRef { nullable: false, ..output.column.clone() },
        })
        .collect();
    let position = ir.nodes.len().checked_sub(1).context("missing materialized output")?;
    let Node::Output { input, .. } = &mut ir.nodes[position] else {
        anyhow::bail!("materialized scope requires final Output")
    };
    let id = format!("{position}_{}", crate::compiler::relational::MATERIALIZED);
    let prior = std::mem::replace(input, id.clone());
    ir.nodes.insert(position, Node::Rebind { id, input: prior, fields });
    Ok(())
}
