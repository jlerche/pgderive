//! Continuous operation of the explicit, native-bound grouped inner-join MVP.
mod admin;
mod drive;
mod health;
mod lifecycle;
pub use health::Policy as HealthPolicy;
mod open;
mod program;
mod registration;
mod results;
mod retire;
mod runtime;
mod spec;
mod sql;
use crate::Config;
pub use admin::{environment, start, status, validate};
use anyhow::{Context, Result};
pub use lifecycle::{drop_query, rebuild};
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
