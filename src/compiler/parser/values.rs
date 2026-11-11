//! Standalone VALUES uses ordinary constant and result ordering nodes.
use super::{Parsed, node, query, stars};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn parse(select: &pg::SelectStmt) -> Result<Parsed> {
    ensure!(
        select.target_list.is_empty()
            && select.from_clause.is_empty()
            && select.where_clause.is_none()
            && select.group_clause.is_empty()
            && select.having_clause.is_none()
            && select.window_clause.is_empty()
            && select.distinct_clause.is_empty()
            && select.into_clause.is_none()
            && select.locking_clause.is_empty()
            && select.with_clause.is_none(),
        "unsupported VALUES clause"
    );
    let mut rows = Vec::new();
    for row in &select.values_lists {
        let NodeEnum::List(row) = node(row)? else { anyhow::bail!("invalid VALUES row") };
        ensure!((1..=64).contains(&row.items.len()), "VALUES requires 1..=64 columns");
        ensure!(
            rows.first().is_none_or(|first: &Vec<Node>| first.len() == row.items.len()),
            "VALUES rows must have the same width"
        );
        rows.push(row.items.clone());
    }
    if select.sort_clause.is_empty()
        && select.limit_count.is_none()
        && select.limit_offset.is_none()
    {
        return Ok(Parsed::Values(rows));
    }
    let width = rows.first().map_or(0, Vec::len);
    let mut inner = select.clone();
    let sort_clause = std::mem::take(&mut inner.sort_clause);
    let limit_count = inner.limit_count.take();
    let limit_offset = inner.limit_offset.take();
    let limit_option =
        std::mem::replace(&mut inner.limit_option, i32::from(pg::LimitOption::Default));
    let source = Node {
        node: Some(NodeEnum::RangeSubselect(Box::new(pg::RangeSubselect {
            subquery: Some(Box::new(Node { node: Some(NodeEnum::SelectStmt(Box::new(inner))) })),
            ..pg::RangeSubselect::default()
        }))),
    };
    query(&pg::SelectStmt {
        target_list: (0..width).map(|index| stars::column_target(label(index))).collect(),
        from_clause: vec![source],
        sort_clause,
        limit_count,
        limit_offset,
        limit_option,
        op: i32::from(pg::SetOperation::SetopNone),
        ..pg::SelectStmt::default()
    })
}

/// Expand stars on a single VALUES source before ORDER BY resolves target positions.
pub(super) fn expand_stars(select: &pg::SelectStmt) -> Result<Option<pg::SelectStmt>> {
    let [source] = select.from_clause.as_slice() else { return Ok(None) };
    let NodeEnum::RangeSubselect(source) = node(source)? else { return Ok(None) };
    let Some(subquery) = source.subquery.as_deref() else { return Ok(None) };
    let NodeEnum::SelectStmt(inner) = node(subquery)? else { return Ok(None) };
    let Some(first) = inner.values_lists.first() else { return Ok(None) };
    let NodeEnum::List(first) = node(first)? else { return Ok(None) };
    let alias = source.alias.as_ref();
    let mut expanded = Vec::new();
    let mut changed = false;
    for value in &select.target_list {
        if stars::qualifier(value)?.is_some_and(|name| match name.as_slice() {
            [] => true,
            [name] => alias.is_some_and(|alias| alias.aliasname == *name),
            _ => false,
        }) {
            ensure!((1..=64).contains(&first.items.len()), "VALUES requires 1..=64 columns");
            ensure!(expanded.len() + first.items.len() <= 64, "projection exceeds 64 columns");
            let columns = alias.map_or(&[][..], |alias| alias.colnames.as_slice());
            let columns = super::expressions::names(columns)?.0;
            for index in 0..first.items.len() {
                let name = columns.get(index).cloned().unwrap_or_else(|| label(index));
                expanded.push(stars::column_target(name));
            }
            changed = true;
        } else {
            expanded.push(value.clone());
        }
    }
    Ok(changed.then(|| pg::SelectStmt { target_list: expanded, ..select.clone() }))
}

pub(in crate::compiler) fn label(index: usize) -> String {
    format!("column{}", index + 1)
}
