//! Bounded `PostgreSQL` frontend for durable grouped joins and source projections.
//! See `docs/sql-compiler.md` for the exact grammar and compatibility contract.
mod bind;
mod expression;
pub(crate) mod left_join;
pub(crate) mod lookup;
pub(crate) mod navigation;
mod parser;
pub(crate) mod partition;
mod placement;
mod projection;
pub(crate) mod relational;
#[cfg(test)]
pub(crate) use projection::Cell;
pub(crate) use projection::Projected;
pub(crate) mod expansion;
mod remainder;
pub(crate) mod scalar;
mod syntax;
pub(crate) mod topk;
mod typed;
use crate::{source::Contract, worker::Query};
use anyhow::Result;
pub(crate) use expression::Predicate;
pub(crate) use expression::native_key;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ColumnRef {
    pub(crate) right: bool,
    pub(crate) name: String,
    pub(crate) oid: u32,
    pub(crate) nullable: bool,
}
/// Resolved relational graph and output layout shared by every frontend.
#[derive(Clone, Serialize)]
pub struct Compiled {
    pub(crate) relational: relational::Relational,
    #[serde(skip)]
    pub(crate) result_columns: Vec<crate::catalog::ResultColumn>,
}
impl Compiled {
    pub(crate) fn placement(&self) -> Result<placement::Report> {
        placement::analyze(self)
    }

    pub(crate) const fn relational(&self) -> &relational::Relational {
        &self.relational
    }
    pub(crate) const fn output(&self) -> &Projected {
        &self.relational.output
    }
    pub(crate) async fn bind_terminal(
        &mut self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
    ) -> Result<()> {
        if let Some(terminal) = &mut self.relational.output.terminal {
            terminal.bind(sql).await?;
        }
        Ok(())
    }
    pub(crate) fn validate_bound(&self) -> Result<()> {
        if let Some(terminal) = &self.output().terminal {
            terminal.validate()?;
        }
        Ok(())
    }
    pub(crate) fn sink(&self, table: &str) -> crate::catalog::Sink {
        crate::catalog::Sink::Rows {
            table: table.into(),
            columns: self.result_columns.clone(),
            map: self.output().terminal.clone(),
        }
    }

    pub(crate) fn legacy(selectors: Query, contract: &Contract) -> Result<Self> {
        selectors.validate(contract)?;
        bind::legacy(selectors, contract)
    }
}
/// Parse, resolve, type-check and lower the supported SQL shape without side effects.
///
/// # Errors
/// Rejects invalid/unsupported SQL, unresolved/ambiguous names and incompatible types.
pub fn compile(sql: &str, contract: &Contract) -> Result<Compiled> {
    contract.validate()?;
    let (parsed, fallback) = parser::parse(sql)?;
    let compiled = bind::bind(parsed, contract).or_else(|prior| match fallback {
        Some(parsed) => bind::bind(parsed, contract).map_err(|_| prior),
        None => Err(prior),
    })?;
    Ok(compiled)
}

#[cfg(test)]
mod tests;
