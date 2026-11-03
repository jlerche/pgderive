//! Global occurrence selection after SQL expressions, grouping and DISTINCT.
use crate::{
    compiler::{
        Compiled, parser::ordering::Parsed, partition::Order, relational::Node, topk::Spec,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};

pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let compiled = super::bind(*parsed.inner, contract)?;
    apply(
        compiled,
        Clause {
            order: parsed.order,
            visible: parsed.visible,
            distinct: parsed.distinct,
            limit: parsed.limit,
            offset: parsed.offset,
        },
        contract,
    )
}
pub(super) struct Clause {
    pub(super) order: Vec<crate::compiler::parser::ordering::Order>,
    pub(super) visible: usize,
    pub(super) distinct: bool,
    pub(super) limit: Option<usize>,
    pub(super) offset: usize,
}
pub(super) fn apply(
    mut compiled: Compiled,
    parsed: Clause,
    contract: &Contract,
) -> Result<Compiled> {
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
            matches!(column.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184),
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
    ensure!(
        identity.iter().all(|column| column.oid != 1186),
        "interval ORDER BY/LIMIT tuple ordering is unsupported"
    );
    ir.output.columns.truncate(parsed.visible);
    let sorted = order.iter().map(|value| value.column.clone()).collect();
    let empty = crate::compiler::properties::occurrence(ir, contract)?.maximum_rows() == Some(0);
    if parsed.limit == Some(0) || empty {
        sort_marker(ir, sorted)?;
        read_nothing(ir)?;
    } else if parsed.limit.is_none() && parsed.offset == 0 {
        sort_marker(ir, sorted)?;
    } else {
        top_k(ir, order, identity, (parsed.limit, parsed.offset))?;
    }
    Ok(compiled)
}
fn top_k(
    ir: &mut crate::compiler::relational::Relational,
    order: Vec<Order>,
    identity: Vec<crate::compiler::ColumnRef>,
    (limit, offset): (Option<usize>, usize),
) -> Result<()> {
    let limit = limit
        .map(|limit| limit.checked_add(offset).context("LIMIT + OFFSET overflow"))
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
    let spec = Spec { order, field: None, limit, offset, identity };
    ir.nodes.push(Node::Rebind { id: global.clone(), input, fields });
    ir.nodes.push(Node::TopKInput { id: indexed.clone(), input: global, spec: spec.clone() });
    ir.nodes.push(Node::TopK { id: selected.clone(), input: indexed, spec });
    ir.nodes.push(Node::Output { id, input: selected });
    Ok(())
}
/// `PostgreSQL` computes sort expressions even when nothing above reads them.
fn sort_marker(
    ir: &mut crate::compiler::relational::Relational,
    columns: Vec<crate::compiler::ColumnRef>,
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    let Some(Node::Output { id, input }) = ir.nodes.pop() else {
        anyhow::bail!("ORDER BY requires final Output")
    };
    let sorted = format!("sort_{}_marker", ir.nodes.len());
    ir.nodes.push(Node::Sorted { id: sorted.clone(), input, columns });
    ir.nodes.push(Node::Output { id, input: sorted });
    Ok(())
}
/// LIMIT 0 plans its input, so a constant still raises, and never runs it.
fn read_nothing(ir: &mut crate::compiler::relational::Relational) -> Result<()> {
    let Some(Node::Output { id, input }) = ir.nodes.pop() else {
        anyhow::bail!("LIMIT requires final Output")
    };
    let filter = format!("{}_{}", ir.nodes.len(), crate::compiler::relational::ZERO_LIMIT);
    let predicate =
        crate::compiler::Predicate::new(crate::compiler::expression::Expr::Or(Vec::new()));
    ir.nodes.push(Node::Filter { id: filter.clone(), input, predicate });
    ir.nodes.push(Node::Output { id, input: filter });
    Ok(())
}
