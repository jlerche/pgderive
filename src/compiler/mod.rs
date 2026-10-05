//! Bounded PostgreSQL-oriented frontend for the durable grouped-join runtime.
//! See `docs/sql-compiler.md` for the exact grammar and compatibility contract.
mod bind;
mod expression;
mod parser;
mod syntax;
use crate::{source::Contract, worker::Query};
use anyhow::Result;
use expression::Expr;
use serde::Serialize;

/// Semantic/compiler and runtime codec revision; changes require fresh bootstrap.
pub const REVISION: &str = "sql-grouped-v3:where-3vl-v1:pg-query-6.2.1:pg-17.7:row-text-v1:json-v2:group-string-v1:i64-sum-v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ColumnRef {
    pub(crate) right: bool,
    pub(crate) name: String,
    pub(crate) oid: u32,
    pub(crate) nullable: bool,
}
/// Resolved typed grouped relational IR, lowered only through the worker bridge.
/// Names/aliases and formatting are absent; full native layout binds at planning.
#[derive(Clone, Serialize)]
pub struct Compiled {
    pub(crate) selectors: Query,
    predicates: Option<Expr>,
    pub(crate) revision: Option<String>,
}
impl Compiled {
    pub(crate) fn legacy(selectors: Query, contract: &Contract) -> Result<Self> {
        selectors.validate(contract)?;
        Ok(Self { selectors, predicates: None, revision: None })
    }
    pub(crate) fn qualifies(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<bool> {
        self.predicates.as_ref().map_or(Ok(true), |expr| Ok(expr.evaluate(rows)? == Some(true)))
    }
}
/// Parse, resolve, type-check and lower the supported SQL shape without side effects.
///
/// # Errors
/// Rejects invalid/unsupported SQL, unresolved/ambiguous names and incompatible types.
pub fn compile(sql: &str, contract: &Contract) -> Result<Compiled> {
    contract.validate()?;
    bind::bind(parser::parse(sql)?, contract)
}

#[cfg(test)]
mod tests;
