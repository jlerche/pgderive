//! Derived partition scopes with typed predicates and exposed projections.
use super::{Parsed, node, query};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(super) fn source_query(source: &pg::RangeSubselect) -> Result<(Parsed, String)> {
    ensure!(!source.lateral, "derived LATERAL unsupported");
    let alias = source.alias.as_ref().context("derived query requires an alias")?;
    ensure!(alias.colnames.len() <= 64, "derived column alias budget exceeded");
    let NodeEnum::SelectStmt(inner) =
        node(source.subquery.as_deref().context("missing derived query")?)?
    else {
        anyhow::bail!("derived query requires SELECT");
    };
    let mut inner = query(inner)?;
    if !alias.colnames.is_empty() {
        inner = Parsed::Renamed {
            inner: Box::new(inner),
            labels: super::expressions::names(&alias.colnames)?.0,
        };
    }
    Ok((inner, alias.aliasname.clone()))
}
