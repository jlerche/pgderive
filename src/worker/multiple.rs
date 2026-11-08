use crate::Config;
use anyhow::{Context, Result, ensure};
use std::future::Future;
use tokio::sync::watch;
mod tasks;
use tasks::Tasks;
mod preflight;

/// Run independent queries concurrently on the caller's runtime in one process.
///
/// Each configuration must exclusively own its catalog, sink, slot and object
/// prefix, exactly as for separate processes. Caches and state remain per query.
/// SIGINT/SIGTERM asks every query to drain its durable work. A terminal query
/// failure is reported without cancelling healthy peers. After all queries stop,
/// this function returns the first error. Broken process diagnostics stop all peers.
///
/// # Errors
/// Returns configuration, signal, or worker errors after draining the other workers.
pub async fn run_many(mut configs: Vec<Config>, limits: super::ProcessLimits) -> Result<()> {
    ensure!(!configs.is_empty(), "at least one query configuration is required");
    for config in &configs {
        config.validate()?;
    }
    preflight::validate(&configs)?;
    super::resources::install(&mut configs, &limits)?;
    let mut queries = Vec::with_capacity(configs.len());
    for config in configs {
        config.validate()?;
        let settings = config.worker.clone().context("configuration requires a worker section")?;
        queries.push((config, settings));
    }
    let (stop, receiver) = watch::channel(false);
    let shutdown = super::shutdown::listen().context("installing worker shutdown handlers")?;
    let mut pending = Tasks::default();
    for (config, settings) in queries {
        let receiver = receiver.clone();
        let identity = crate::diagnostics::Identity::new(&config, &settings);
        pending.spawn(identity, async move {
            let name = settings.query_id.clone();
            Box::pin(super::drive::run_controlled(config, settings, receiver))
                .await
                .with_context(|| format!("query {name} failed"))
        });
    }
    supervise(pending, stop, shutdown).await
}

async fn supervise<S>(mut pending: Tasks, stop: watch::Sender<bool>, shutdown: S) -> Result<()>
where
    S: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(shutdown);
    let mut stopping = false;
    let mut failure = None;
    let mut failed_queries = 0_u64;
    while !pending.is_empty() {
        let result = tokio::select! {
            signal = &mut shutdown, if !stopping => {
                stopping = true;
                stop.send_replace(true);
                signal.context("worker Ctrl-C monitor failed")
            }
            completion = pending.next() => {
                if completion.fatal {
                    stopping = true;
                    stop.send_replace(true);
                }
                completion.result
            },
        };
        if let Err(error) = result {
            failed_queries =
                failed_queries.checked_add(1).context("failed query count overflow")?;
            if failure.is_none() {
                failure = Some(error);
            }
            let status = serde_json::json!({"event":"process_status","status":"degraded","failed_queries":failed_queries,"active_queries":pending.len()});
            if let Err(error) = super::drive::emit(&status) {
                stop.send_replace(true);
                stopping = true;
                failure =
                    Some(failure.take().map_or(error, |primary| {
                        primary.context("process diagnostics also failed")
                    }));
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests;
