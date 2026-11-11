//! Derived partition scopes with typed predicates and exposed projections.
use super::{Parsed, node, query};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(super) fn source_query(source: &pg::RangeSubselect) -> Result<(Parsed, String)> {
    ensure!(!source.lateral, "derived LATERAL unsupported");
    let NodeEnum::SelectStmt(inner) =
        node(source.subquery.as_deref().context("missing derived query")?)?
    else {
        anyhow::bail!("derived query requires SELECT");
    };
    let values = !inner.values_lists.is_empty();
    let mut inner = if inner.with_clause.is_some() {
        let mut body = (**inner).clone();
        body.with_clause = None;
        super::Parsed::Materialized(Box::new(query(&body)?))
    } else {
        query(inner)?
    };
    let Some(alias) = source.alias.as_ref() else {
        ensure!(values, "derived query requires an alias");
        return Ok((inner, String::new()));
    };
    ensure!(alias.colnames.len() <= 64, "derived column alias budget exceeded");
    if !alias.colnames.is_empty() {
        inner = Parsed::Renamed {
            inner: Box::new(inner),
            labels: super::expressions::names(&alias.colnames)?.0,
        };
    }
    Ok((inner, alias.aliasname.clone()))
}
