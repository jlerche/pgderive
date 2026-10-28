use anyhow::{Context, Result, ensure};
use object_store::{
    Certificate, ClientOptions, ObjectStore, ObjectStoreExt as _, RetryConfig,
    aws::AmazonS3Builder, prefix::PrefixStore,
};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use url::Url;
mod credentials;
mod scoped;
pub use scoped::PageStore;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    endpoint: String,
    bucket: String,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    ca_file: Option<PathBuf>,
    credentials_file: PathBuf,
}
impl Storage {
    pub fn validate(&self) -> Result<()> {
        let endpoint = self.endpoint()?;
        ensure!(
            (3..=63).contains(&self.bucket.len())
                && self.bucket.bytes().all(|c| c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || matches!(c, b'-' | b'.'))
                && self.bucket.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
                && self.bucket.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric),
            "invalid S3 bucket name"
        );
        if endpoint.scheme() == "http" {
            ensure!(
                self.bucket.starts_with("pgderive-"),
                "local harness bucket must start with pgderive-"
            );
            ensure!(self.ca_file.is_none(), "HTTP endpoint cannot use a TLS CA");
        } else {
            ensure!(self.region.is_some(), "HTTPS object endpoint requires an explicit region");
        }
        if let Some(region) = &self.region {
            ensure!(
                !region.is_empty()
                    && region.len() <= 128
                    && region
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
                "invalid S3 region"
            );
        }
        ensure!(!self.credentials_file.as_os_str().is_empty(), "missing S3 credential file");
        Ok(())
    }
    fn endpoint(&self) -> Result<Url> {
        let endpoint = Url::parse(&self.endpoint).context("invalid object endpoint URL")?;
        ensure!(
            endpoint.host_str().is_some()
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "object endpoint must be an origin without userinfo, path, query or fragment"
        );
        ensure!(
            endpoint.scheme() == "https"
                || (endpoint.scheme() == "http"
                    && matches!(endpoint.host_str(), Some("127.0.0.1" | "localhost"))
                    && endpoint.port().is_some_and(|port| port > 0)),
            "object endpoint requires HTTPS; only explicit-port loopback HTTP is supported for local fixtures"
        );
        Ok(endpoint)
    }
    pub async fn preflight(&self, store: &dyn ObjectStore) -> Result<()> {
        if self.endpoint()?.scheme() == "http" {
            return Ok(());
        }
        match store.head(&object_store::path::Path::from("__pgderive_connectivity_probe__")).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error).context("HTTPS object endpoint preflight failed"),
        }
    }
    pub fn build(&self, prefix: &str, requests: usize) -> Result<Arc<dyn PageStore>> {
        self.validate()?;
        ensure!((1..=256).contains(&requests), "invalid shared object request limit");
        let credentials = credentials::load(&self.credentials_file)?;
        let mut options = ClientOptions::new()
            .with_allow_http(self.endpoint()?.scheme() == "http")
            .with_timeout(std::time::Duration::from_secs(30))
            .with_connect_timeout(std::time::Duration::from_secs(10));
        if let Some(path) = &self.ca_file {
            options = options.with_root_certificate(Certificate::from_pem(&std::fs::read(path)?)?);
        }
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region(self.region.as_deref().unwrap_or("us-east-1"))
            .with_client_options(options)
            .with_access_key_id(credentials.access_key_id)
            .with_secret_access_key(credentials.secret_access_key)
            .with_retry(RetryConfig { max_retries: 0, ..RetryConfig::default() });
        if let Some(token) = credentials.session_token {
            builder = builder.with_token(token);
        }
        let raw = Arc::new(builder.build()?);
        let inner = Arc::new(PrefixStore::new(raw.clone(), prefix));
        Ok(Arc::new(scoped::Scoped::new(
            inner,
            raw,
            object_store::path::Path::from(prefix),
            requests,
        )?))
    }
}
#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
