use crate::Config;
use anyhow::{Context, Result, ensure};
use std::future::Future;
use tokio::{sync::watch, task::JoinSet};

/// Run independent queries concurrently on the caller's runtime in one process.
///
/// Each configuration must exclusively own its catalog, sink, slot and object
/// prefix, exactly as for separate processes. Caches and state remain per query.
/// Ctrl-C or a terminal query failure asks every query to drain its durable work;
/// this function waits for all of them before returning the first error.
///
/// # Errors
/// Returns configuration, signal, or worker errors after draining the other workers.
pub async fn run_many(configs: Vec<Config>) -> Result<()> {
    ensure!(!configs.is_empty(), "at least one query configuration is required");
    let mut queries = Vec::with_capacity(configs.len());
    for config in configs {
        config.validate()?;
        let settings = config.worker.clone().context("configuration requires a worker section")?;
        queries.push((config, settings));
    }
    let (stop, receiver) = watch::channel(false);
    let mut pending = JoinSet::new();
    for (config, settings) in queries {
        let receiver = receiver.clone();
        pending.spawn(async move {
            let name = settings.query_id.clone();
            Box::pin(super::drive::run_controlled(config, settings, receiver))
                .await
                .with_context(|| format!("query {name} failed"))
        });
    }
    supervise(pending, stop, tokio::signal::ctrl_c()).await
}

async fn supervise<S>(
    mut pending: JoinSet<Result<()>>,
    stop: watch::Sender<bool>,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(shutdown);
    let mut stopping = false;
    let mut failure = None;
    while !pending.is_empty() {
        let result = tokio::select! {
            signal = &mut shutdown, if !stopping => {
                stopping = true;
                stop.send_replace(true);
                signal.context("worker Ctrl-C monitor failed")
            }
            result = pending.join_next() => result
                .context("query supervisor stream ended")?
                .context("query task failed")
                .and_then(std::convert::identity),
        };
        if let Err(error) = result {
            stop.send_replace(true);
            stopping = true;
            if failure.is_none() {
                failure = Some(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests;
