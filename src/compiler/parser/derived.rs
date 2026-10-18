//! Derived partition scopes with typed predicates and exposed projections.
use super::{Parsed, node, query};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
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
        matches!(inner, Parsed::Partition(_) | Parsed::Lookup(_)),
        "derived query requires a native partition query or derived scope"
    );
    Ok((inner, alias.aliasname.clone()))
}
