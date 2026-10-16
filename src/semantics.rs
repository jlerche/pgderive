//! Shared `PostgreSQL` codec and generated-SQL dependency attestation.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;

/// Bound server and catalog semantics, checked during initialization.
/// Catalog inspection detects drift; it does not lock administrative mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    server: String,
    encoding: String,
    catalog: String,
}
impl Environment {
    /// Configure a worker session during initialization.
    pub(crate) async fn configure(sql: &(impl GenericClient + Sync)) -> Result<()> {
        sql.batch_execute("SET search_path=pg_catalog; SET TimeZone='UTC'; SET DateStyle='ISO,MDY'; SET IntervalStyle='iso_8601'; SET extra_float_digits=3").await?;
        Ok(())
    }

    pub(crate) async fn inspect(
        sql: &(impl GenericClient + Sync),
        collations: &[u32],
    ) -> Result<Self> {
        let mut collations = collations.to_vec();
        collations.extend([100, 950]); // PostgreSQL's default and built-in C collation.
        collations.sort_unstable();
        collations.dedup();
        collations.retain(|oid| *oid != 0);
        let row = sql.query_one(include_str!("semantics/catalog.sql"), &[&collations]).await?;
        let environment =
            Self { server: row.try_get(0)?, encoding: row.try_get(1)?, catalog: row.try_get(2)? };
        environment.validate()?;
        Ok(environment)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.server.parse::<u32>()? >= 170_000, "PostgreSQL 17 or newer required");
        ensure!(self.encoding == "UTF8", "semantic codecs require UTF8");
        ensure!(
            self.catalog.len() == 64 && self.catalog.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid semantic catalog fingerprint"
        );
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn fixture() -> Self {
        Self { server: "170007".into(), encoding: "UTF8".into(), catalog: "0".repeat(64) }
    }
}

#[cfg(test)]
mod tests {
    use super::Environment;
    use anyhow::Result;
    #[test]
    fn environment_rejects_incomplete_or_unsupported_bindings() -> Result<()> {
        Environment::fixture().validate()?;
        for (field, value) in
            [("server", "bad"), ("server", "160000"), ("encoding", "LATIN1"), ("catalog", "abc")]
        {
            let mut encoded = serde_json::to_value(Environment::fixture())?;
            encoded[field] = value.into();
            let environment: Environment = serde_json::from_value(encoded)?;
            assert!(environment.validate().is_err());
        }
        assert!(serde_json::from_str::<Environment>("{}").is_err());
        Ok(())
    }
}
