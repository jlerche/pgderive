//! Explicit, independent metadata endpoint for tenant physical storage.
use anyhow::{Result, ensure};
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub(crate) postgres: crate::configuration::Postgres,
    pub(crate) schema: String,
    pub(crate) object_prefix: String,
    #[serde(default)]
    pub(crate) batching: Batching,
}
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Batching {
    pub(crate) target_bytes: usize,
    pub(crate) buffer_bytes: usize,
    pub(crate) delay_ms: u64,
}
impl Default for Batching {
    fn default() -> Self {
        Self { target_bytes: 4 * 1024 * 1024, buffer_bytes: 32 * 1024 * 1024, delay_ms: 5 }
    }
}
impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        self.postgres.validate()?;
        ensure!(crate::configuration::identifier(&self.schema), "invalid tenant storage schema");
        ensure!(crate::storage::prefix(&self.object_prefix), "invalid tenant physical prefix");
        let batch = &self.batching;
        ensure!((1..=8 * 1024 * 1024).contains(&batch.target_bytes), "invalid tenant pack target");
        ensure!(
            (9 * 1024 * 1024..=256 * 1024 * 1024).contains(&batch.buffer_bytes),
            "invalid upload buffer budget"
        );
        ensure!(batch.delay_ms <= 1000, "tenant batching delay exceeds one second");
        Ok(())
    }
    pub(crate) const fn delay(&self) -> Duration {
        Duration::from_millis(self.batching.delay_ms)
    }
}
