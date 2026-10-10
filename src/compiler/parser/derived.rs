//! Derived partition scopes with typed predicates and exposed projections.
use super::{Name, Parsed, column, node, query, target};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Derived {
    pub inner: Box<Parsed>,
    pub alias: String,
    pub columns: Vec<(Name, String)>,
    pub predicate: Option<crate::compiler::syntax::Expr>,
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Derived>> {
    let [source] = select.from_clause.as_slice() else {
        return Ok(None);
    };
    let NodeEnum::RangeSubselect(source) = node(source)? else {
        return Ok(None);
    };
    ensure!(select.group_clause.is_empty(), "derived grouping requires partition lowering");
    let (inner, alias) = source_query(source)?;
    ensure!(
        (1..=64).contains(&select.target_list.len()),
        "derived projection requires 1..=64 columns"
    );
    let columns = select
        .target_list
        .iter()
        .map(|value| {
            let NodeEnum::ResTarget(output) = node(value)? else {
                anyhow::bail!("invalid derived output");
            };
            ensure!(output.indirection.is_empty(), "derived output indirection unsupported");
            let name = column(target(value)?)?;
            let label = if output.name.is_empty() {
                name.0.last().context("missing derived column")?.clone()
            } else {
                output.name.clone()
            };
            Ok((name, label))
        })
        .collect::<Result<_>>()?;
    let predicate = crate::compiler::syntax::predicate(select.where_clause.as_deref())?;
    Ok(Some(Derived { inner: Box::new(inner), alias, columns, predicate }))
}

pub(super) fn source_query(source: &pg::RangeSubselect) -> Result<(Parsed, String)> {
    ensure!(!source.lateral, "derived LATERAL unsupported");
    let alias = source.alias.as_ref().context("derived query requires an alias")?;
    ensure!(alias.colnames.is_empty(), "derived column alias list unsupported");
    let NodeEnum::SelectStmt(inner) =
        node(source.subquery.as_deref().context("missing derived query")?)?
    else {
        anyhow::bail!("derived query requires SELECT");
    };
    let inner = query(inner)?;
    ensure!(
        matches!(inner, Parsed::Partition(_) | Parsed::Derived(_) | Parsed::Lookup(_)),
        "derived query requires a native partition query or derived scope"
    );
    Ok((inner, alias.aliasname.clone()))
}
