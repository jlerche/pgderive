//! Resolve derived output labels, then filter after the inner partition.
use super::{Compiled, bind};
use crate::{
    compiler::{
        ColumnRef, Predicate, Program, parser::derived::Derived, projection::OutputColumn,
        relational::Node,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
pub(super) fn bind_derived(parsed: Derived, contract: &Contract) -> Result<Compiled> {
    let mut compiled = bind(*parsed.inner, contract)?;
    let Program::Relational { relational } = &mut compiled.program else {
        anyhow::bail!("derived query requires relational lowering");
    };
    ensure!(
        relational.output.terminal.is_none(),
        "derived scope cannot consume sink-deferred expressions"
    );
    let resolve = |name: &crate::compiler::parser::Name| -> Result<ColumnRef> {
        let label = match name.0.as_slice() {
            [label] => label,
            [alias, label] if *alias == parsed.alias => label,
            _ => anyhow::bail!("unknown derived qualifier"),
        };
        let mut candidates =
            relational.output.columns.iter().filter(|column| column.label == *label);
        let candidate = candidates.next().context("unknown derived column")?;
        ensure!(candidates.next().is_none(), "ambiguous derived column");
        Ok(candidate.column.clone())
    };
    let predicate = parsed
        .predicate
        .map(|value| crate::compiler::expression::bind(value, &resolve).map(Predicate::new))
        .transpose()?;
    let columns = parsed
        .columns
        .into_iter()
        .map(|(name, label)| Ok(OutputColumn { column: resolve(&name)?, label }))
        .collect::<Result<_>>()?;
    let Some(Node::Output { id, input }) = relational.nodes.pop() else {
        anyhow::bail!("derived query requires terminal output node");
    };
    let (input, suffix) = if let Some(predicate) = predicate {
        let count =
            relational.nodes.iter().filter(|node| matches!(node, Node::Filter { .. })).count();
        let filter = if count == 0 { "qualified".into() } else { format!("qualified_{count}") };
        relational.nodes.push(Node::Filter { id: filter.clone(), input, predicate });
        (filter, ":derived-scope-filter-v1")
    } else {
        (input, ":derived-scope-projection-v1")
    };
    relational.nodes.push(Node::Output { id, input });
    relational.output.columns = columns;
    let revision = compiled.revision.as_mut().context("missing inner compiler revision")?;
    revision.push_str(suffix);
    Ok(compiled)
}
