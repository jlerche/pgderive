//! Configurable diagnostic `PostgreSQL` logical replication listener.

use anyhow::Result;
use pgderive::{Config, listen};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "config.local.toml".to_owned());
    listen(Config::load(path)?).await
}
