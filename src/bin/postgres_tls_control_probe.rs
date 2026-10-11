//! Read-only replication-control TLS qualification probe.
use anyhow::{Context, Result};
use pgderive::{Config, source::Identity};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).context("missing configuration path")?;
    let identity = Identity::inspect(&Config::load(path)?).await?;
    serde_json::to_writer(std::io::stdout().lock(), &identity)?;
    Ok(())
}
