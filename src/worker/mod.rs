//! Continuous operation of the explicit, native-bound grouped inner-join MVP.
mod drive;
mod open;
mod program;
mod registration;
mod runtime;
mod spec;
mod sql;
use crate::Config;
use anyhow::{Context, Result};
pub use spec::{Query, QueryDefinition, Settings, SqlQuery};

/// Start or resume a durable grouped worker using the configuration's worker section.
///
/// The catalog schema must already exist and the object prefix must be exclusively
/// owned by it. Ctrl-C drains the current publication before stopping; no received
/// transaction is acknowledged before authoritative sink/membership/progress COMMIT.
///
/// # Errors
/// Returns configuration, source contract, bootstrap, retry exhaustion or output errors.
pub async fn run(config: Config) -> Result<()> {
    config.validate()?;
    let settings = config.worker.clone().context("configuration requires a worker section")?;
    drive::run(config, settings).await
}

#[cfg(test)]
mod tests;
