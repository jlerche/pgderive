//! Run a native-bound, durable grouped inner-join worker.
use anyhow::{Context, Result, ensure};
#[tokio::main]
async fn main() -> Result<()> {
    let mut paths = std::env::args().skip(1).collect::<Vec<_>>();
    let limits = if paths.first().is_some_and(|value| value == "--resources") {
        ensure!(
            paths.len() >= 3,
            "usage: pgderive_worker [--resources PROCESS.toml] QUERY.toml ..."
        );
        let limits = pgderive::worker::ProcessLimits::load(&paths[1])
            .context("loading process resource limits")?;
        paths.drain(..2);
        limits
    } else {
        pgderive::worker::ProcessLimits::default()
    };
    ensure!(paths.iter().all(|path| !path.starts_with('-')), "unknown worker option");
    if paths.is_empty() {
        paths.push("config.local.toml".into());
    }
    let configs = paths
        .into_iter()
        .map(|path| {
            pgderive::Config::load(&path)
                .with_context(|| format!("loading query configuration {path}"))
        })
        .collect::<Result<Vec<_>>>()?;
    Box::pin(pgderive::worker::run_many(configs, limits)).await
}
