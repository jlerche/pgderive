//! Global occurrence selection after SQL expressions, grouping and DISTINCT.
use crate::{
    compiler::{
        Compiled, parser::ordering::Parsed, partition::Order, relational::Node, topk::Spec,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};

pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let mut compiled = super::bind(*parsed.inner, contract)?;
    let ir = &mut compiled.relational;
    let mut order = Vec::new();
    for (index, descending, nulls_first) in parsed.order {
        let column =
            ir.output.columns.get(index).context("missing ORDER BY target")?.column.clone();
        ensure!(
            matches!(column.oid, 20 | 21 | 23 | 1114 | 1184),
            "ORDER BY requires integral, timestamp or timestamptz keys"
        );
        ensure!(
            !parsed.distinct
                || ir.output.columns[..parsed.visible].iter().any(|c| c.column == column),
            "SELECT DISTINCT ORDER BY expressions must appear in the output"
        );
        order.push(Order { column, descending, nulls_first });
    }
    ir.output.columns.truncate(parsed.visible);
    if parsed.limit == Some(0)
        || crate::compiler::properties::occurrence(ir, contract)?.maximum_rows() == Some(0)
    {
        prune_empty(ir)?;
        return Ok(compiled);
    }
    if let Some(limit) = parsed.limit {
        let names = order.iter().map(|order| order.column.name.clone()).collect::<Vec<_>>();
        ensure!(
            crate::compiler::properties::occurrence(ir, contract)?.proves_key(&names),
            "ORDER BY LIMIT must include complete logical occurrence identity"
        );
        let Some(Node::Output { id, input }) = ir.nodes.pop() else {
            anyhow::bail!("ORDER BY requires final Output")
        };
        let prefix = format!("sort_{}", ir.nodes.len());
        let global = format!("{prefix}_global");
        let indexed = format!("{prefix}_input");
        let selected = format!("{prefix}_selected");
        let spec = Spec { order, field: None, limit };
        ir.nodes.push(Node::CrossKey { id: global.clone(), input });
        ir.nodes.push(Node::TopKInput { id: indexed.clone(), input: global, spec: spec.clone() });
        ir.nodes.push(Node::TopK { id: selected.clone(), input: indexed, spec });
        ir.nodes.push(Node::Output { id, input: selected });
    }
    Ok(compiled)
}

fn prune_empty(ir: &mut crate::compiler::relational::Relational) -> Result<()> {
    let source = ir.sources.first().context("empty SQL scope requires a source")?.clone();
    // The circuit requires one source to drive transaction ticks. All other
    // operands and computation are dead; the complete publication contract still
    // governs source capture and durable progress, including unrelated ticks.
    ir.sources = vec![source];
    ir.nodes = vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::Empty { id: "empty".into(), input: "source".into() },
        Node::Output { id: "project".into(), input: "empty".into() },
    ];
    Ok(())
}
