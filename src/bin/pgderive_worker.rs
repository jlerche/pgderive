//! Run a native-bound, durable grouped inner-join worker.
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    let mut paths = std::env::args().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        paths.push("config.local.toml".into());
    }
    let configs = paths.into_iter().map(pgderive::Config::load).collect::<Result<Vec<_>>>()?;
    Box::pin(pgderive::worker::run_many(configs)).await
}
