//! Bounded `PostgreSQL` frontend for durable grouped joins and source projections.
//! See `docs/sql-compiler.md` for the exact grammar and compatibility contract.
mod bind;
mod expression;
mod parser;
pub(crate) mod partition;
mod projection;
pub(crate) mod relational;
pub(crate) use projection::{Cell, Projected};
pub(crate) mod scalar;
mod syntax;
use crate::{source::Contract, worker::Query};
use anyhow::Result;
use expression::Expr;
pub(crate) use expression::Predicate;
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
/// Resolved typed bounded relational IR, lowered only through the worker bridge.
/// Names/aliases and formatting are absent; full native layout binds at planning.
#[derive(Clone, Serialize)]
pub struct Compiled {
    #[serde(flatten)]
    pub(crate) program: Program,
    predicates: Option<Expr>,
    pub(crate) revision: Option<String>,
}
#[derive(Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum Program {
    Grouped { selectors: Query },
    Relational { relational: relational::Relational },
    Projection { projection: Projected },
}
impl Compiled {
    pub(crate) fn selectors(&self) -> Result<&Query> {
        let Program::Grouped { selectors } = &self.program else {
            anyhow::bail!("expected grouped program");
        };
        Ok(selectors)
    }
    pub(crate) const fn projection(&self) -> Option<&Projected> {
        if let Program::Projection { projection } = &self.program { Some(projection) } else { None }
    }
    pub(crate) const fn relational(&self) -> Option<&relational::Relational> {
        if let Program::Relational { relational } = &self.program { Some(relational) } else { None }
    }
    const fn layout(&self) -> Option<&Projected> {
        match &self.program {
            Program::Projection { projection } => Some(projection),
            Program::Relational { relational } => Some(&relational.output),
            Program::Grouped { .. } => None,
        }
    }
    pub(crate) async fn bind_terminal(
        &mut self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
    ) -> Result<()> {
        let layout = match &mut self.program {
            Program::Projection { projection } => Some(projection),
            Program::Relational { relational } => Some(&mut relational.output),
            Program::Grouped { .. } => None,
        };
        if let Some(layout) = layout
            && let Some(terminal) = &mut layout.terminal
        {
            terminal.bind(sql).await?;
        }
        Ok(())
    }
    pub(crate) fn validate_bound(&self) -> Result<()> {
        if let Some(projection) = self.layout()
            && let Some(terminal) = &projection.terminal
        {
            terminal.validate()?;
        }
        Ok(())
    }
    pub(crate) fn sink(&self, table: &str) -> crate::catalog::Sink {
        if let Some(projection) = self.layout()
            && let Some(map) = &projection.terminal
        {
            return crate::catalog::Sink::MappedBag { table: table.into(), map: map.clone() };
        }
        if self.layout().is_some() {
            crate::catalog::Sink::Bag(table.into())
        } else {
            crate::catalog::Sink::Grouped(table.into())
        }
    }
    pub(crate) fn legacy(selectors: Query, contract: &Contract) -> Result<Self> {
        selectors.validate(contract)?;
        let mut compiled =
            Self { program: Program::Grouped { selectors }, predicates: None, revision: None };
        compiled.bind_native_codecs(contract);
        Ok(compiled)
    }
    fn bind_native_codecs(&mut self, contract: &Contract) {
        if contract
            .relations
            .iter()
            .flat_map(|relation| &relation.columns)
            .any(|column| matches!(column.oid, 1114 | 1184 | 1700))
        {
            let base = self.revision.as_deref().unwrap_or(REVISION);
            self.revision = Some(format!(
                "{base}:native-iso-utc-source-v1:pg-microsecond-timestamp-v1:exact-numeric-json-v1"
            ));
        }
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
    let mut compiled = bind::bind(parser::parse(sql)?, contract)?;
    compiled.bind_native_codecs(contract);
    Ok(compiled)
}

#[cfg(test)]
mod tests;
