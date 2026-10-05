use crate::{
    configuration::identifier,
    source::{Column, Contract, Relation},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// Hand-authored grouped inner-join registration; SQL lowering follows the MVP.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Left source namespace.
    pub left_schema: String,
    /// Left source table.
    pub left_table: String,
    /// Left equijoin column (NULL never matches).
    pub left_key: String,
    /// Left grouping column, preserving NULL.
    pub group: String,
    /// Right source namespace.
    pub right_schema: String,
    /// Right source table.
    pub right_table: String,
    /// Right equijoin column.
    pub right_key: String,
    /// Nullable integral right-side SUM column.
    pub sum: String,
}
/// SQL frontend input, retaining the legacy selector form for existing registrations.
#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub enum QueryDefinition {
    /// Compile SQL into the supported durable composition.
    Sql(SqlQuery),
    /// Existing explicit selector registration.
    Legacy(Query),
}
/// SQL text supplied in `[worker.query]` instead of explicit selectors.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQuery {
    /// One supported SELECT statement.
    pub sql: String,
}
impl QueryDefinition {
    pub(crate) fn compile(&self, contract: &Contract) -> Result<crate::compiler::Compiled> {
        match self {
            Self::Sql(query) => crate::compiler::compile(&query.sql, contract),
            Self::Legacy(query) => crate::compiler::Compiled::legacy(query.clone(), contract),
        }
    }
}
/// Continuous worker configuration and bounded maintenance/retry policy.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// Existing owned catalog/destination schema.
    pub catalog_schema: String,
    /// Stable registered query name.
    pub query_id: String,
    /// Grouped destination table inside the owned catalog schema.
    pub sink_table: String,
    /// Exclusively owned durable object-store prefix, reused across resumes.
    pub object_prefix: String,
    /// Maximum rows in one immutable data block.
    pub block_rows: usize,
    /// Compact and collect after this many logical transactions (1..=64).
    pub maintenance_ticks: u64,
    /// Maximum consecutive failed attempts before stopping without ACK.
    pub retry_attempts: u32,
    /// Initial exponential retry delay; capped at 5 seconds.
    pub retry_delay_ms: u64,
    /// Zero runs continuously; nonzero stops after this many new CDC transactions.
    pub max_transactions: u64,
    /// Maximum startup/recovery duration before cancellation and authoritative retry.
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout_secs: u64,
    /// Explicit supported operator composition.
    pub query: QueryDefinition,
}
impl Settings {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!((1..=3600).contains(&self.startup_timeout_secs), "invalid worker startup timeout");
        ensure!(identifier(&self.catalog_schema), "invalid worker catalog schema");
        ensure!(
            !self.query_id.is_empty() && self.query_id.len() <= 200,
            "invalid worker query name"
        );
        crate::catalog::Sink::Grouped(self.sink_table.clone()).validate()?;
        ensure!(
            !self.object_prefix.is_empty()
                && self.object_prefix.len() <= 200
                && self.object_prefix.split('/').all(|part| !part.is_empty()
                    && part.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))),
            "invalid owned worker object prefix"
        );
        ensure!(
            (1..=65_536).contains(&self.block_rows) && (1..=64).contains(&self.maintenance_ticks),
            "invalid worker maintenance/block limits"
        );
        ensure!(
            (1..=32).contains(&self.retry_attempts) && (1..=5000).contains(&self.retry_delay_ms),
            "invalid worker retry limits"
        );
        Ok(())
    }
}
impl Query {
    pub(crate) fn validate(&self, contract: &Contract) -> Result<()> {
        contract.validate()?;
        let left = self.relation(contract, true)?;
        let right = self.relation(contract, false)?;
        ensure!(left.oid != right.oid, "worker join requires distinct source relations");
        let lhs = column(left, &self.left_key)?;
        let rhs = column(right, &self.right_key)?;
        ensure!(
            lhs.oid == rhs.oid && lhs.modifier == rhs.modifier && lhs.collation == rhs.collation,
            "worker join key native types differ"
        );
        column(left, &self.group)?;
        ensure!(
            matches!(column(right, &self.sum)?.oid, 20 | 21 | 23),
            "worker SUM requires an integral native column"
        );
        Ok(())
    }
    fn relation<'a>(&self, contract: &'a Contract, left: bool) -> Result<&'a Relation> {
        let (schema, table) = if left {
            (&self.left_schema, &self.left_table)
        } else {
            (&self.right_schema, &self.right_table)
        };
        contract
            .relations
            .iter()
            .find(|relation| &relation.schema == schema && &relation.table == table)
            .context("worker source missing from native publication")
    }
}
fn column<'a>(relation: &'a Relation, name: &str) -> Result<&'a Column> {
    relation
        .columns
        .iter()
        .find(|column| column.name == name)
        .context("worker column missing from native contract")
}

const fn default_startup_timeout() -> u64 {
    180
}
