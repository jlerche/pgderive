//! Expand select-list stars before target positions and labels are bound.
use super::{expressions::names, node, table, target};
use crate::source::Contract;
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn qualifier(value: &Node) -> Result<Option<Vec<String>>> {
    let NodeEnum::ColumnRef(value) = node(target(value)?)? else { return Ok(None) };
    let Some((last, prefix)) = value.fields.split_last() else { return Ok(None) };
    if matches!(node(last)?, NodeEnum::AStar(_)) {
        return Ok(Some(names(prefix)?.0));
    }
    Ok(None)
}

pub(super) fn expand_table(select: &mut pg::SelectStmt, contract: &Contract) -> Result<()> {
    let [source] = select.from_clause.as_slice() else { return Ok(()) };
    if !matches!(node(source)?, NodeEnum::RangeVar(_)) {
        return Ok(());
    }
    let qualifiers = select.target_list.iter().map(qualifier).collect::<Result<Vec<_>>>()?;
    let stars = qualifiers.iter().flatten().count();
    if stars == 0 {
        return Ok(());
    }
    let table = table(source)?;
    let relation = table.relation(contract)?;
    let width = (select.target_list.len() - stars)
        .saturating_add(stars.saturating_mul(relation.columns.len()));
    ensure!(width <= 64, "projection exceeds 64 columns");
    for name in qualifiers.iter().flatten() {
        ensure!(table.matches_qualifier(name), "unknown wildcard qualifier");
    }
    let mut expanded = Vec::with_capacity(width);
    for (value, name) in std::mem::take(&mut select.target_list).into_iter().zip(qualifiers) {
        if name.is_some() {
            expanded
                .extend(relation.columns.iter().map(|column| column_target(column.name.clone())));
        } else {
            expanded.push(value);
        }
    }
    select.target_list = expanded;
    Ok(())
}

pub(super) fn column_target(name: String) -> Node {
    let field = Node { node: Some(NodeEnum::String(pg::String { sval: name })) };
    let reference = Node {
        node: Some(NodeEnum::ColumnRef(pg::ColumnRef { fields: vec![field], location: -1 })),
    };
    Node {
        node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
            val: Some(Box::new(reference)),
            ..pg::ResTarget::default()
        }))),
    }
}
