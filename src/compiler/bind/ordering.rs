//! Global occurrence selection after SQL expressions, grouping and DISTINCT.
use crate::{
    compiler::{
        Compiled,
        parser::ordering::{Bounds, Parsed},
        partition::Order,
        relational::Node,
        topk::Spec,
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
            distinct_on: parsed.distinct_on,
            bounds: parsed.bounds,
        },
        contract,
    )
}
pub(super) struct Clause {
    pub(super) order: Vec<crate::compiler::parser::ordering::Order>,
    pub(super) visible: usize,
    pub(super) distinct: bool,
    pub(super) distinct_on: Vec<usize>,
    pub(super) bounds: Bounds,
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
        (parsed.distinct_on.is_empty()
            && order.is_empty()
            && parsed.bounds.limit.is_none()
            && parsed.bounds.offset == 0)
            || identity.iter().all(|column| column.oid != 1186),
        "interval ORDER BY/LIMIT tuple ordering is unsupported"
    );
    if !parsed.distinct_on.is_empty() {
        distinct_on(ir, &parsed.distinct_on, &order, &identity)?;
    }
    ir.output.columns.truncate(parsed.visible);
    let sorted = order.iter().map(|value| value.column.clone()).collect();
    let empty = crate::compiler::properties::occurrence(ir, contract)?.maximum_rows() == Some(0);
    if parsed.bounds.limit == Some(0) || empty {
        sort_marker(ir, sorted)?;
        read_nothing(ir)?;
    } else if parsed.bounds.limit.is_none() && parsed.bounds.offset == 0 {
        sort_marker(ir, sorted)?;
    } else {
        let Bounds { limit, offset, with_ties } = parsed.bounds;
        let limit = limit
            .map(|limit| limit.checked_add(offset).context("LIMIT + OFFSET overflow"))
            .transpose()?
            .unwrap_or(usize::MAX);
        select(ir, Spec { order, field: None, limit, offset, with_ties, identity }, None)?;
    }
    Ok(compiled)
}
fn distinct_on(
    ir: &mut crate::compiler::relational::Relational,
    indices: &[usize],
    order: &[Order],
    identity: &[crate::compiler::ColumnRef],
) -> Result<()> {
    let mut names = std::collections::BTreeSet::new();
    let mut keys = Vec::new();
    for index in indices {
        let column = &ir.output.columns.get(*index).context("missing DISTINCT ON key")?.column;
        ensure!(
            matches!(column.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184),
            "unsupported DISTINCT ON key type"
        );
        ensure!(
            !matches!(column.oid, 25 | 1043) || column.collation != 0,
            "DISTINCT ON requires resolved text collation"
        );
        if names.insert(column.name.clone()) {
            keys.push(column.clone());
        }
    }
    for value in order {
        if names.is_empty() {
            break;
        }
        ensure!(
            keys.iter().any(|key| key == &value.column),
            "DISTINCT ON expressions must match initial ORDER BY expressions"
        );
        names.remove(&value.column.name);
    }
    let spec = Spec {
        order: order.to_vec(),
        field: None,
        limit: 1,
        offset: 0,
        with_ties: false,
        identity: identity.to_vec(),
    };
    select(ir, spec, Some(keys))
}

fn select(
    ir: &mut crate::compiler::relational::Relational,
    spec: Spec,
    keys: Option<Vec<crate::compiler::ColumnRef>>,
) -> Result<()> {
    let Some(Node::Output { id, input }) = ir.nodes.pop() else {
        anyhow::bail!("ORDER BY requires final Output")
    };
    let prefix = format!("sort_{}", ir.nodes.len());
    let global = format!("{prefix}_global");
    let indexed = format!("{prefix}_input");
    let selected = format!("{prefix}_selected");
    let fields = spec
        .identity
        .iter()
        .map(|column| crate::compiler::relational::Binding {
            input: column.clone(),
            output: column.clone(),
        })
        .collect();
    ir.nodes.push(Node::Rebind { id: global.clone(), input, fields });
    let keyed = keys.is_some();
    let input = if let Some(keys) = keys {
        let partition = format!("{prefix}_partition");
        ir.nodes.push(Node::PartitionBy { id: partition.clone(), input: global, keys });
        partition
    } else {
        global
    };
    ir.nodes.push(Node::TopKInput { id: indexed.clone(), input, spec: spec.clone() });
    ir.nodes.push(Node::TopK { id: selected.clone(), input: indexed, spec: spec.clone() });
    let input = if keyed {
        let deindexed = format!("{prefix}_winners");
        let fields = spec
            .identity
            .into_iter()
            .map(|column| crate::compiler::relational::Binding {
                input: column.clone(),
                output: column,
            })
            .collect();
        ir.nodes.push(Node::Rebind { id: deindexed.clone(), input: selected, fields });
        deindexed
    } else {
        selected
    };
    ir.nodes.push(Node::Output { id, input });
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
