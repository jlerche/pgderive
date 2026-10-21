use super::{open::Open, program::relational, spec::Settings, sql::Session};
use crate::{
    Config,
    catalog::{GcLimits, Progress},
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
    run_initial(config, settings, None).await
}
pub(super) async fn run_initial(
    config: Config,
    settings: Settings,
    initial: Option<Session>,
) -> Result<()> {
    let (signal, mut stop) = watch::channel(false);
    let monitor = Signal(tokio::spawn(async move {
        tokio::signal::ctrl_c().await.context("worker Ctrl-C monitor failed")?;
        let _closed = signal.send(true);
        Ok(())
    }));
    let result = retry(&config, &settings, &mut stop, initial).await;
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
    mut initial: Option<Session>,
) -> Result<()> {
    let started = Instant::now();
    let mut metrics = Metrics::default();
    loop {
        if *stop.borrow() {
            return emit(&json!({"event":"stopped","retries":metrics.retries}));
        }
        let mut ticket = super::health::attempts::Ticket::new();
        let opening = timeout(
            Duration::from_secs(settings.startup_timeout_secs),
            ticket.start(config, settings, initial.take(), metrics.failures),
        )
        .await
        .context("worker attempt startup timed out")
        .and_then(std::convert::identity);
        let result = match opening {
            Ok(session) => attempt(config, settings, stop, &mut metrics, (session, &ticket)).await,
            Err(error) => Err(error),
        };
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
                let observation = timeout(
                    Duration::from_secs(settings.health.stale_secs),
                    ticket.failure(config, settings, attempts, &error),
                )
                .await
                .context("worker failure observation timed out")
                .and_then(std::convert::identity);
                match observation {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(error.context("query attempt superseded by another owner"));
                    }
                    Err(diagnostic) => emit(
                        &json!({"event":"diagnostic_failure","attempt":attempts,"error":format!("{diagnostic:#}")}),
                    )?,
                }
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
    initial: (Session, &super::health::attempts::Ticket),
) -> Result<(u64, String)> {
    let ticket = initial.1;
    let mut startup = crate::timing::Clock::new(settings.phase_metrics);
    let opening = Open::connect_with_session(config, settings, initial.0);
    let mut open = timeout(Duration::from_secs(settings.startup_timeout_secs), Box::pin(opening))
        .await
        .context("worker startup/recovery timed out")??;
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    let base = *metrics.base.get_or_insert(durable.published_time);
    let mut stream = crate::acknowledged::Stream::connect(&open.replication, &durable).await?;
    if !super::replay::monitored(&mut open, &mut stream, &durable, stop).await? {
        return Ok((open.query.time(), durable.end.to_string()));
    }
    let mut health = super::health::Monitor::start(config, &open, &durable).await?;
    let reporter = health.reporter();
    ensure!(
        ticket.record(&open.session.client, settings, "running", (metrics.failures, None)).await?,
        "worker attempt ownership changed"
    );
    let result = async {
        let mut ready = json!({"event":"ready","time":open.query.time(),"end_lsn":durable.end.to_string(),"checkpoint_lsn":durable.checkpoint_end.to_string(),"epoch":durable.stored.epoch,"slot":open.replication.replication.slot});
        if settings.phase_metrics { ready["open_recovery_us"] = serde_json::to_value(startup.lap())?; }
        emit(&ready)?;
        tokio::select! {
            result = consume(&mut open, &mut stream, settings, stop, (base, metrics, &reporter)) => result,
            result = health.failed() => result,
        }
    }.await;
    let shutdown = timeout(Duration::from_secs(10), stream.shutdown())
        .await
        .context("worker replication shutdown timed out")
        .and_then(std::convert::identity);
    let result = crate::outcome::combine(result, shutdown, "worker replication shutdown");
    let attempts = if result.is_err() {
        metrics.failures.checked_add(1).context("worker retry counter overflow")?
    } else {
        metrics.failures
    };
    let diagnostics =
        timeout(Duration::from_secs(10), health.finish(&open, result.as_ref().err(), attempts))
            .await
            .context("worker final diagnostics timed out")
            .and_then(std::convert::identity);
    let result = crate::outcome::combine(result, diagnostics, "worker final diagnostics");
    let diagnostics = ticket
        .record(
            &open.session.client,
            settings,
            if result.is_ok() { "stopped" } else { "failed" },
            (attempts, result.as_ref().err()),
        )
        .await;
    if let Err(diagnostic) = diagnostics {
        emit(&json!({"event":"diagnostic_failure","error":format!("{diagnostic:#}")}))?;
    }
    result?;
    Ok((open.query.time(), open.writer.end().to_string()))
}
async fn consume(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    settings: &Settings,
    stop: &mut watch::Receiver<bool>,
    count: (u64, &mut Metrics, &super::health::Reporter),
) -> Result<()> {
    let interval = Duration::from_millis(settings.checkpoint_interval_ms);
    let mut deadline = tokio::time::Instant::now() + interval;
    loop {
        let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
        let pending = durable.checkpoint_end < durable.end;
        let stopping = *stop.borrow()
            || (settings.max_transactions != 0
                && open.query.time().saturating_sub(count.0) >= settings.max_transactions);
        if pending
            && (stopping
                || tokio::time::Instant::now() >= deadline
                || u64::try_from(open.query.run_count())? >= settings.maintenance_ticks)
        {
            count.2.record(open, "maintenance")?;
            timeout(
                Duration::from_secs(settings.health.operation_timeout_secs),
                maintain(open, count.1),
            )
            .await
            .context("worker checkpoint timed out; recover retained WAL")??;
            stream.acknowledge(&mut open.session.client, &open.writer, open.query.plan()).await?;
            deadline = tokio::time::Instant::now() + interval;
            count.2.record(open, "ready")?;
        }
        if stopping {
            return Ok(());
        }
        let transaction = tokio::select! {
            signal=stop.changed() => {signal.context("worker stop monitor closed")?;continue;}
            ()=tokio::time::sleep_until(deadline), if pending => {continue;}
            transaction=stream.recv() => transaction?,
        };
        let progress =
            Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        if durable.covers(&progress)? {
            continue;
        }
        if !pending {
            deadline = tokio::time::Instant::now() + interval;
        }
        count.2.record(open, "publishing")?;
        timeout(
            Duration::from_secs(settings.health.operation_timeout_secs),
            publish(open, &transaction, stream.decode_us(), count.1),
        )
        .await
        .context("worker publication timed out; recover retained WAL without ACK")??;
        count.2.record(open, "ready")?;
        count.1.failures = 0;
    }
}

async fn publish(
    open: &mut Open,
    transaction: &Transaction,
    decode_us: u128,
    metrics: &mut Metrics,
) -> Result<()> {
    let measured = open.settings.phase_metrics;
    let mut phases = super::measurements::Phases::new(measured);
    let started = Instant::now();
    let time = open.query.time().checked_add(1).context("worker logical time overflow")?;
    let input = relational::inputs(&transaction.batch, open.compiled.relational(), time)?;
    phases.mark("inputs");
    let prepared =
        match open.query.prepare_memtable(input.clone(), open.settings.memtable_bytes).await {
            Ok(work) => work,
            Err(error) if error.is::<crate::engine::plan::relational::MemtableFull>() => {
                maintain(open, metrics).await?;
                open.query.prepare_memtable(input, open.settings.memtable_bytes).await?
            }
            Err(error) => return Err(error),
        };
    phases.mark("prepare_including_object_io");
    let deltas = relational::deltas(&prepared.output().batch, open.compiled.output())?;
    let progress = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
    phases.mark("finalize");
    let publication = open
        .writer
        .publish_memtable(
            &mut open.session.client,
            open.query.plan(),
            (time, &progress, &deltas),
            measured,
        )
        .await?;
    phases.mark("publication");
    open.query.commit(prepared)?;
    phases.mark("local_completion");
    let cache = open.query.cache_stats()?;
    let mut event = json!({"event":"published","time":time,"memtable_bytes":open.query.memtable_bytes()?,"end_lsn":transaction.end_lsn,"changes":transaction.changes.len(),"elapsed_ms":started.elapsed().as_millis(),"cache":{"bytes":cache.bytes,"entries":cache.entries,"live_bytes":cache.live_bytes,"pins":cache.pins,"hits":cache.hits,"misses":cache.misses}});
    if let Some(phases) = phases.finish(decode_us)? {
        event["phases_us"] = phases;
        event["publication_us"] = serde_json::to_value(publication)?;
    }
    emit(&event)
}
async fn maintain(open: &mut Open, metrics: &mut Metrics) -> Result<()> {
    let mut clock = crate::timing::Clock::new(open.settings.phase_metrics);
    let runs = open.query.run_count();
    let protection = open
        .writer
        .protect_upload(
            &mut open.session.client,
            &format!("worker:{}:maintenance:{}", open.nonce, open.writer.epoch()),
        )
        .await?;
    let prepared = open.query.prepare_compaction_protected(&protection).await?;
    let checkpoint = open.query.prepared_compaction_checkpoint(&prepared)?;
    open.writer
        .checkpoint_memtable(&mut open.session.client, open.query.plan(), &checkpoint)
        .await?;
    open.query.commit_compaction(prepared)?;
    protection
        .published(&mut open.session.client, &open.writer, open.query.plan(), &checkpoint)
        .await?;
    let collection = open
        .catalog
        .collect(&mut open.session.client, open.store.clone(), GcLimits::default())
        .await?;
    metrics.maintenance =
        metrics.maintenance.checked_add(1).context("worker maintenance metric overflow")?;
    ensure!(open.query.time() == checkpoint.time, "worker maintenance advanced time");
    let mut event = json!({"event":"maintenance","time":open.query.time(),"epoch":open.writer.epoch(),"previous_runs":runs,"collection":format!("{collection:?}")});
    if open.settings.phase_metrics {
        event["elapsed_us"] = serde_json::to_value(clock.lap())?;
    }
    emit(&event)
}
pub(super) fn emit(event: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, event)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
