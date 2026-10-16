//! Materialize SQL values before a downstream relational scope consumes them.
use crate::compiler::{
    output::OutputMap,
    relational::{Node, Relational},
    scalar,
    typed::Typed,
};
use anyhow::{Context, Result, ensure};
pub(super) fn finalize(ir: &mut Relational) -> Result<()> {
    let Some(mapping) = ir.output.mapping.take() else { return Ok(()) };
    ensure!(mapping.maps.len() == ir.output.columns.len(), "intermediate output layout mismatch");
    let mut computed = Vec::new();
    let prefix = format!("@boundary_{}_", ir.nodes.len());
    for (column, map) in ir.output.columns.iter_mut().zip(mapping.maps) {
        if map == OutputMap::Average {
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
