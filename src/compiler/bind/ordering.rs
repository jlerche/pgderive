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
    let facts = crate::compiler::properties::occurrence(ir, contract)?;
    let mut order = Vec::new();
    for value in parsed.order {
        ensure!(
            !parsed.distinct || !value.bytewise,
            "DISTINCT ORDER BY COLLATE requires a selected collated expression, which is unsupported"
        );
        let column =
            ir.output.columns.get(value.index).context("missing ORDER BY target")?.column.clone();
        ensure!(
            !value.bytewise || matches!(column.oid, 25 | 1043),
            "COLLATE requires text ordering"
        );
        ensure!(
            matches!(column.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1700 | 2950 | 1114 | 1184),
            "unsupported ORDER BY native type"
        );
        ensure!(
            !matches!(column.oid, 25 | 1043)
                || value.bytewise
                || matches!(facts.collation(&column), Some(950 | 951)),
            "text ORDER BY requires built-in C/POSIX collation or explicit COLLATE"
        );
        ensure!(
            !parsed.distinct
                || ir.output.columns[..parsed.visible].iter().any(|c| c.column == column),
            "SELECT DISTINCT ORDER BY expressions must appear in the output"
        );
        order.push(Order { column, descending: value.descending, nulls_first: value.nulls_first });
    }
    let mut seen = std::collections::BTreeSet::new();
    let identity = ir
        .output
        .columns
        .iter()
        .map(|column| column.column.clone())
        .filter(|column| seen.insert(column.name.clone()))
        .collect::<Vec<_>>();
    ir.output.columns.truncate(parsed.visible);
    if parsed.limit == Some(0)
        || crate::compiler::properties::occurrence(ir, contract)?.maximum_rows() == Some(0)
    {
        prune_empty(ir)?;
        return Ok(compiled);
    }
    if parsed.limit.is_some() || parsed.offset > 0 {
        let limit = parsed
            .limit
            .map(|limit| limit.checked_add(parsed.offset).context("LIMIT + OFFSET overflow"))
            .transpose()?
            .unwrap_or(usize::MAX);
        let Some(Node::Output { id, input }) = ir.nodes.pop() else {
            anyhow::bail!("ORDER BY requires final Output")
        };
        let prefix = format!("sort_{}", ir.nodes.len());
        let global = format!("{prefix}_global");
        let indexed = format!("{prefix}_input");
        let selected = format!("{prefix}_selected");
        let fields = identity
            .iter()
            .map(|column| crate::compiler::relational::Binding {
                input: column.clone(),
                output: column.clone(),
            })
            .collect();
        let spec = Spec { order, field: None, limit, offset: parsed.offset, identity };
        ir.nodes.push(Node::Rebind { id: global.clone(), input, fields });
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
