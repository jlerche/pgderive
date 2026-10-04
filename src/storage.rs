use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, RetryConfig, aws::AmazonS3Builder, prefix::PrefixStore};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    endpoint: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
}
impl Storage {
    pub fn validate(&self) -> Result<()> {
        let port = self
            .endpoint
            .strip_prefix("http://127.0.0.1:")
            .or_else(|| self.endpoint.strip_prefix("http://localhost:"))
            .context("local object endpoint must be loopback HTTP")?;
        ensure!(
            port.parse::<u16>().is_ok_and(|port| port > 0),
            "invalid local object endpoint port"
        );
        ensure!(
            self.bucket.starts_with("pgderive-")
                && self
                    .bucket
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
            "local harness bucket must start with pgderive-"
        );
        ensure!(
            !self.access_key_id.is_empty() && !self.secret_access_key.is_empty(),
            "missing local S3 credentials"
        );
        Ok(())
    }
    pub fn build(&self, prefix: &str) -> Result<Arc<dyn ObjectStore>> {
        self.validate()?;
        let store = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .with_retry(RetryConfig { max_retries: 0, ..RetryConfig::default() })
            .build()?;
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    }
}
#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
