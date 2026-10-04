//! Run a native-bound, durable grouped inner-join worker.
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "config.local.toml".into());
    pgderive::worker::run(pgderive::Config::load(path)?).await
}
