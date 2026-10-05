use super::{open::Open, program, spec::Settings};
use crate::{
    Config,
    catalog::{Deltas, GcLimits, Progress, Publication},
    transaction::Transaction,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    io::{self, Write},
    time::Instant,
};
use tokio::{
    sync::watch,
    time::{Duration, sleep, timeout},
};

pub(super) async fn run(config: Config, settings: Settings) -> Result<()> {
    let (signal, mut stop) = watch::channel(false);
    let monitor = Signal(tokio::spawn(async move {
        tokio::signal::ctrl_c().await.context("worker Ctrl-C monitor failed")?;
        let _closed = signal.send(true);
        Ok(())
    }));
    let result = retry(&config, &settings, &mut stop).await;
    drop(monitor);
    result
}
struct Signal(tokio::task::JoinHandle<Result<()>>);
impl Drop for Signal {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[derive(Default)]
struct Metrics {
    base: Option<u64>,
    retries: u64,
    maintenance: u64,
    failures: u32,
}
async fn retry(
    config: &Config,
    settings: &Settings,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    let started = Instant::now();
    let mut metrics = Metrics::default();
    loop {
        if *stop.borrow() {
            return emit(&json!({"event":"stopped","retries":metrics.retries}));
        }
        let result = attempt(config, settings, stop, &mut metrics).await;
        match result {
            Ok((time, end)) => {
                return emit(
                    &json!({"event":"stopped","time":time,"end_lsn":end,"transactions":time.saturating_sub(metrics.base.unwrap_or(time)),"retries":metrics.retries,"maintenance":metrics.maintenance,"elapsed_ms":started.elapsed().as_millis()}),
                );
            }
            Err(error) => {
                metrics.failures =
                    metrics.failures.checked_add(1).context("worker retry counter overflow")?;
                let attempts = metrics.failures;
                metrics.retries =
                    metrics.retries.checked_add(1).context("worker retry metric overflow")?;
                emit(&json!({"event":"retry","attempt":attempts,"error":format!("{error:#}")}))?;
                if attempts >= settings.retry_attempts {
                    return Err(error.context(
                        "worker retry budget exhausted; source remains at authoritative progress",
                    ));
                }
                let delay = settings
                    .retry_delay_ms
                    .saturating_mul(1_u64.checked_shl(attempts - 1).unwrap_or(u64::MAX))
                    .min(5000);
                tokio::select! {
                    ()=sleep(Duration::from_millis(delay)) => {}
                    signal=stop.changed() => {signal.context("worker stop monitor closed")?;}
                }
            }
        }
    }
}
async fn attempt(
    config: &Config,
    settings: &Settings,
    stop: &mut watch::Receiver<bool>,
    metrics: &mut Metrics,
) -> Result<(u64, String)> {
    let mut open = timeout(
        Duration::from_secs(settings.startup_timeout_secs),
        Open::connect(config, settings),
    )
    .await
    .context("worker startup/recovery timed out")??;
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    let base = *metrics.base.get_or_insert_with(|| open.query.time());
    let mut stream = crate::acknowledged::Stream::connect(&open.replication, &durable).await?;
    let result = async {
        emit(&json!({"event":"ready","time":open.query.time(),"end_lsn":durable.end.to_string(),"epoch":durable.stored.epoch,"slot":open.replication.replication.slot}))?;
        consume(&mut open, &mut stream, settings, stop, (base, metrics)).await
    }.await;
    let shutdown = timeout(Duration::from_secs(10), stream.shutdown())
        .await
        .context("worker replication shutdown timed out")
        .and_then(std::convert::identity);
    crate::outcome::combine(result, shutdown, "worker replication shutdown")?;
    Ok((open.query.time(), open.writer.end().to_string()))
}
async fn consume(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    settings: &Settings,
    stop: &mut watch::Receiver<bool>,
    count: (u64, &mut Metrics),
) -> Result<()> {
    loop {
        maintain(open, settings, count.1).await?;
        if *stop.borrow()
            || (settings.max_transactions != 0
                && open.query.time().saturating_sub(count.0) >= settings.max_transactions)
        {
            return Ok(());
        }
        let transaction = tokio::select! {
            signal=stop.changed() => {signal.context("worker stop monitor closed")?;return Ok(());}
            transaction=stream.recv() => transaction?,
        };
        publish(open, &transaction).await?;
        stream.acknowledge(&mut open.session.client, &open.writer, open.query.plan()).await?;
        count.1.failures = 0;
    }
}
async fn publish(open: &mut Open, transaction: &Transaction) -> Result<()> {
    let started = Instant::now();
    let time = open.query.time().checked_add(1).context("worker logical time overflow")?;
    let token = format!("worker:{}:transaction:{time}", open.nonce);
    let protection = open.writer.protect_upload(&mut open.session.client, &token).await?;
    let prepared = open
        .query
        .prepare_protected(
            program::inputs(&transaction.batch, &open.compiled.selectors, time)?,
            &protection,
        )
        .await?;
    let checkpoint = open.query.prepared_checkpoint(&prepared)?;
    let deltas = Deltas::grouped(&prepared.output().batch)?;
    let progress = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
    open.writer
        .publish(
            &mut open.session.client,
            open.query.plan(),
            Publication { checkpoint: &checkpoint, progress: &progress, deltas: &deltas },
        )
        .await?;
    open.query.commit(prepared)?;
    protection
        .published(&mut open.session.client, &open.writer, open.query.plan(), &checkpoint)
        .await?;
    let cache = open.query.cache_stats()?;
    emit(
        &json!({"event":"published","time":time,"end_lsn":transaction.end_lsn,"changes":transaction.changes.len(),"elapsed_ms":started.elapsed().as_millis(),"cache":{"bytes":cache.bytes,"entries":cache.entries,"hits":cache.hits,"misses":cache.misses}}),
    )
}
async fn maintain(open: &mut Open, settings: &Settings, metrics: &mut Metrics) -> Result<()> {
    let state = open.query.snapshot();
    let runs = [
        state.left.run_count(),
        state.right.run_count(),
        state.sums.run_count(),
        state.output.run_count(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    if u64::try_from(runs)? < settings.maintenance_ticks {
        return Ok(());
    }
    let protection = open
        .writer
        .protect_upload(
            &mut open.session.client,
            &format!("worker:{}:maintenance:{}", open.nonce, open.writer.epoch()),
        )
        .await?;
    let prepared = open.query.prepare_compaction_protected(&protection).await?;
    let checkpoint = open.query.prepared_compaction_checkpoint(&prepared)?;
    open.query.publish_compaction(&mut open.session.client, &mut open.writer, prepared).await?;
    protection
        .published(&mut open.session.client, &open.writer, open.query.plan(), &checkpoint)
        .await?;
    drop(state);
    let collection = open
        .catalog
        .collect(&mut open.session.client, open.store.clone(), GcLimits::default())
        .await?;
    metrics.maintenance =
        metrics.maintenance.checked_add(1).context("worker maintenance metric overflow")?;
    ensure!(open.query.time() == checkpoint.time, "worker maintenance advanced time");
    emit(
        &json!({"event":"maintenance","time":open.query.time(),"epoch":open.writer.epoch(),"previous_runs":runs,"collection":format!("{collection:?}")}),
    )
}
pub(super) fn emit(event: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, event)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
