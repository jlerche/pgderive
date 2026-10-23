use crate::{configuration::identifier, source::Contract};
use anyhow::{Result, ensure};
use serde::Deserialize;

/// SQL text supplied in `[worker.query]`.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryDefinition {
    /// One supported SELECT statement.
    pub sql: String,
}
impl QueryDefinition {
    pub(crate) async fn compile_bound(
        &self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
        contract: &Contract,
    ) -> Result<crate::compiler::Compiled> {
        self.describe(sql, contract).await.map(|(compiled, _)| compiled)
    }
    pub(crate) async fn describe(
        &self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
        contract: &Contract,
    ) -> Result<(crate::compiler::Compiled, Vec<serde_json::Value>)> {
        let mut compiled = self.compile(contract)?;
        let (columns, description) = super::results::columns(sql, self).await?;
        compiled.result_columns = columns;
        Ok((compiled, description))
    }
    pub(crate) fn compile(&self, contract: &Contract) -> Result<crate::compiler::Compiled> {
        crate::compiler::compile(&self.sql, contract)
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
    /// Owned ordinary typed result table inside the catalog schema.
    pub sink_table: String,
    /// `PostgreSQL` result table (default) or diagnostic stdout SQL batches.
    #[serde(default)]
    pub sink_output: crate::catalog::SinkOutput,
    /// Proven unique, non-NULL result column names; empty preserves arbitrary bags.
    #[serde(default)]
    pub sink_key: Vec<String>,
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
    /// Opt-in execution-boundary timing; disabled by default.
    #[serde(default)]
    pub phase_metrics: bool,
    /// Maximum charged in-memory arrangement bytes between object checkpoints.
    #[serde(default = "default_memtable_bytes")]
    pub memtable_bytes: usize,
    /// Interval for scheduling checkpoint uploads; slow I/O is bounded by backpressure.
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_ms: u64,
    /// Adaptive committed-prefix publication policy.
    #[serde(default)]
    pub epoch: super::EpochPolicy,
    /// Diagnostic observation and physical WAL alerts.
    #[serde(default)]
    pub health: super::HealthPolicy,
    /// Explicit supported operator composition.
    pub query: QueryDefinition,
}
impl Settings {
    pub(crate) fn validate(&self) -> Result<()> {
        self.health.validate()?;
        self.epoch.validate()?;
        ensure!(self.memtable_bytes > 0, "invalid memtable byte limit");
        ensure!((1..=60_000).contains(&self.checkpoint_interval_ms), "invalid checkpoint interval");
        ensure!((1..=3600).contains(&self.startup_timeout_secs), "invalid worker startup timeout");
        ensure!(identifier(&self.catalog_schema), "invalid worker catalog schema");
        ensure!(
            !self.query_id.is_empty() && self.query_id.len() <= 200,
            "invalid worker query name"
        );
        ensure!(
            identifier(&self.sink_table) && !self.sink_table.starts_with("pgderive_"),
            "invalid or reserved sink table"
        );
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
const fn default_startup_timeout() -> u64 {
    180
}

const fn default_memtable_bytes() -> usize {
    64 * 1024 * 1024
}
const fn default_checkpoint_interval() -> u64 {
    5000
}
