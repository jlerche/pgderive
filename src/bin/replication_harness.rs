//! Drive an isolated Nexmark-shaped fixture through the diagnostic listener.

use anyhow::{Context, Result};
use pgderive::{Config, run_harness, run_recovery};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "--recover") {
        let arg = |ordinal| args.get(ordinal).context("missing recovery argument");
        let report = run_recovery(Config::load(arg(2)?)?, arg(3)?, arg(4)?, arg(5)?).await?;
        serde_json::to_writer(std::io::stdout().lock(), &report)?;
        return Ok(());
    }
    let path = args.get(1).map_or("config.local.toml", String::as_str);
    run_harness(Config::load(path)?).await
}
