//! Configurable diagnostic `PostgreSQL` logical replication listener.

use anyhow::{Context, Result, bail};
use pgderive::{Config, listen};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [] => listen(Config::load("config.local.toml")?).await,
        [path] => listen(Config::load(path)?).await,
        [scope, command, path] if scope == "query" => {
            let config = Config::load(path).context("loading query configuration")?;
            match command.as_str() {
                "validate" | "status" => {
                    let result = if command == "validate" {
                        pgderive::worker::validate(&config).await?
                    } else {
                        pgderive::worker::status(&config).await?
                    };
                    println!("{result}");
                    Ok(())
                }
                "create" | "restart" => pgderive::worker::run(config).await,
                _ => bail!("unknown query command: {command}"),
            }
        }
        _ => bail!(
            "usage: pgderive [CONFIG.toml] | query <validate|status|create|restart> CONFIG.toml"
        ),
    }
}
