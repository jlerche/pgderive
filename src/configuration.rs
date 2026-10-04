use anyhow::{Context, Result, ensure};
use config::{Environment, File};
use pgwire_replication::{ReplicationConfig, TlsConfig};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Typed configuration for the diagnostic replication listener.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub(super) object_store: Option<crate::storage::Storage>,
    pub(super) postgres: Postgres,
    pub(super) replication: Replication,
    #[serde(default)]
    pub(super) listener: Listener,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Postgres {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) database: String,
    pub(super) user: String,
    #[serde(default)]
    pub(super) password: String,
    pub(super) tls: String,
    pub(super) ca_file: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Replication {
    pub(super) slot: String,
    pub(super) publication: String,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Listener {
    pub(super) max_transactions: usize,
    pub(super) max_transaction_changes: usize,
}

impl Default for Listener {
    fn default() -> Self {
        Self { max_transactions: 0, max_transaction_changes: 100_000 }
    }
}

impl Config {
    /// Load a TOML file, then overlay `PGDERIVE__SECTION__KEY` environment variables.
    ///
    /// # Errors
    /// Returns an error for unreadable files, unknown fields, or invalid settings.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let config: Self = config::Config::builder()
            .add_source(File::from(path.as_ref()))
            .add_source(
                Environment::with_prefix("PGDERIVE")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .context("loading listener configuration")?
            .try_deserialize()
            .context("decoding listener configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(!self.postgres.host.is_empty(), "postgres.host cannot be empty");
        ensure!(self.postgres.port > 0, "postgres.port cannot be zero");
        ensure!(!self.postgres.database.is_empty(), "postgres.database cannot be empty");
        ensure!(!self.postgres.user.is_empty(), "postgres.user cannot be empty");
        ensure!(
            matches!(self.postgres.tls.as_str(), "disable" | "verify-full"),
            "postgres.tls must be disable or verify-full"
        );
        ensure!(identifier(&self.replication.slot), "invalid replication.slot identifier");
        ensure!(
            identifier(&self.replication.publication),
            "invalid replication.publication identifier"
        );
        ensure!(
            self.listener.max_transaction_changes > 0,
            "max_transaction_changes must be positive"
        );
        if let Some(storage) = &self.object_store {
            storage.validate()?;
        }
        Ok(())
    }

    pub(super) fn replication_config(&self) -> ReplicationConfig {
        let pg = &self.postgres;
        let tls = if pg.tls == "verify-full" {
            TlsConfig::verify_full(pg.ca_file.clone())
        } else {
            TlsConfig::disabled()
        };
        ReplicationConfig::new(
            &pg.host,
            &pg.user,
            &pg.password,
            &pg.database,
            &self.replication.slot,
            self.replication.publication.as_str(),
        )
        .with_port(pg.port)
        .with_tls(tls)
    }
}

pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && value.as_bytes().first().is_some_and(|byte| byte.is_ascii_lowercase() || *byte == b'_')
}

#[cfg(test)]
mod tests;
