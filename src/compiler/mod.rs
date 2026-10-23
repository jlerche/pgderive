//! Bounded `PostgreSQL` frontend for durable grouped joins and source projections.
//! See `docs/sql-compiler.md` for the exact grammar and compatibility contract.
mod bind;
pub(crate) mod capabilities;
mod expression;
pub(crate) mod left_join;
pub(crate) mod lookup;
pub(crate) mod navigation;
mod parser;
pub(crate) mod partition;
mod sink_key;

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
mod value;
use crate::source::Contract;
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
    pub(crate) const fn relational(&self) -> &relational::Relational {
        &self.relational
    }
    pub(crate) const fn output(&self) -> &Projected {
        &self.relational.output
    }
    pub(crate) fn validate_execution(&self) -> Result<()> {
        anyhow::ensure!(!self.output().columns.is_empty(), "empty compiled output");
        Ok(())
    }
    pub(crate) fn sink(
        &self,
        table: &str,
        output: crate::catalog::SinkOutput,
        primary_key: &[String],
    ) -> Result<crate::catalog::Sink> {
        sink_key::prove(self, primary_key)?;
        Ok(crate::catalog::Sink {
            output,
            table: table.into(),
            primary_key: primary_key.to_vec(),
            columns: self.result_columns.clone(),
        })
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
