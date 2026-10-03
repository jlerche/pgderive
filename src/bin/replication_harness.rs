//! Drive an isolated Nexmark-shaped fixture through the diagnostic listener.

use anyhow::Result;
use pgderive::{Config, run_harness};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "config.local.toml".to_owned());
    run_harness(Config::load(path)?).await
}
