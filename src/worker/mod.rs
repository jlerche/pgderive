//! Continuous operation of the explicit, native-bound grouped inner-join MVP.
mod admin;
mod checkpoint;
mod collection;
mod drive;
mod epoch;
pub use epoch::Policy as EpochPolicy;
mod health;
mod lifecycle;
mod replay;
pub use health::Policy as HealthPolicy;
mod measurements;
mod multiple;
pub use multiple::run_many;
mod open;
mod program;
mod registration;
mod results;
mod retire;
mod spec;
mod sql;
use crate::Config;
pub use admin::{environment, start, status, validate};
use anyhow::Result;
pub use lifecycle::{drop_query, rebuild};
pub use spec::{QueryDefinition, Settings};

/// Start or resume a durable grouped worker using the configuration's worker section.
///
/// The catalog schema must already exist and the object prefix must be exclusively
/// owned by it. Ctrl-C drains the current publication before stopping; no received
/// transaction is acknowledged before authoritative sink/membership/progress COMMIT.
///
/// # Errors
/// Returns configuration, source contract, bootstrap, retry exhaustion or output errors.
pub async fn run(config: Config) -> Result<()> {
    run_many(vec![config]).await
}

#[cfg(test)]
mod tests;
