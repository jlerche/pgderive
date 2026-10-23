//! Configurable diagnostic `PostgreSQL` logical replication listener.

use anyhow::{Context, Result, bail};
use pgderive::{Config, listen};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, path] if command == "listen" => listen(Config::load(path)?).await,
        [scope, command, path] if scope == "environment" => {
            let config = Config::load(path)?;
            let begin = match command.as_str() {
                "begin" => true,
                "finish" => false,
                _ => bail!("unknown environment command: {command}"),
            };
            println!("{}", pgderive::worker::environment(&config, begin).await?);
            Ok(())
        }
        [scope, command, path] if scope == "query" => {
            let config = Config::load(path).context("loading query configuration")?;
            match command.as_str() {
                "validate" | "status" | "drop" | "rebuild" => {
                    let result = if command == "validate" {
                        pgderive::worker::validate(&config).await?
                    } else if command == "status" {
                        pgderive::worker::status(&config).await?
                    } else if command == "rebuild" {
                        pgderive::worker::rebuild(&config).await?
                    } else {
                        pgderive::worker::drop_query(&config).await?
                    };
                    println!("{result}");
                    Ok(())
                }
                "create" | "restart" => {
                    Box::pin(pgderive::worker::start(config, command == "restart")).await
                }
                _ => bail!("unknown query command: {command}"),
            }
        }
        _ => bail!(
            "usage: pgderive environment <begin|finish> CONFIG.toml | listen CONFIG.toml | query <validate|status|create|restart|rebuild|drop> CONFIG.toml"
        ),
    }
}
