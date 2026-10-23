use super::{open::Open, program::relational, spec::Settings, sql::Session};
use crate::{Config, catalog::Progress};
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
    source: super::epoch::Count,
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
                    &json!({"event":"stopped","time":time,"end_lsn":end,"transactions":metrics.source.total(),"retries":metrics.retries,"maintenance":metrics.maintenance,"elapsed_ms":started.elapsed().as_millis()}),
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
    metrics.source.resolve(&durable)?;
    let mut stream = crate::acknowledged::Stream::connect(&open.replication, &durable).await?;
    if !super::replay::monitored(&mut open, &mut stream, &durable, stop).await? {
        return Ok((open.query.time(), durable.end.to_string()));
    }
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    let mut health = super::health::Monitor::start(config, &open, &durable).await?;
    let reporter = health.reporter();
    ensure!(
        ticket.record(&open.session.client, settings, "running", (metrics.failures, None)).await?,
        "worker attempt ownership changed"
    );
    let result = async {
        ready(&open, &durable, &mut startup)?;
        tokio::select! {
            result = consume(&mut open, &mut stream, settings, stop, (metrics, &reporter)) => result,
            result = health.failed() => result,
        }
    }.await;
    let shutdown = timeout(Duration::from_secs(10), stream.shutdown())
        .await
        .context("worker replication shutdown timed out")
        .and_then(std::convert::identity);
    drop(stream);
    let result = crate::outcome::combine(result, shutdown, "worker replication shutdown");
    let collection = open.collector.drain().await;
    let result = crate::outcome::combine(result, collection, "worker physical collection shutdown");
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
    record_finished(ticket, &open, settings, attempts, &result).await?;
    result?;
    Ok((open.query.time(), open.writer.end().to_string()))
}

async fn record_finished(
    ticket: &super::health::attempts::Ticket,
    open: &Open,
    settings: &Settings,
    attempts: u32,
    result: &Result<()>,
) -> Result<()> {
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
    Ok(())
}

fn ready(
    open: &Open,
    durable: &crate::catalog::Durable,
    startup: &mut crate::timing::Clock,
) -> Result<()> {
    let mut event = json!({"event":"ready","time":open.query.time(),"end_lsn":durable.end.to_string(),"checkpoint_lsn":durable.checkpoint_end.to_string(),"epoch":durable.stored.epoch,"slot":open.replication.replication.slot});
    if open.settings.phase_metrics {
        event["open_recovery_us"] = serde_json::to_value(startup.lap())?;
    }
    emit(&event)
}

async fn consume(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    settings: &Settings,
    stop: &mut watch::Receiver<bool>,
    count: (&mut Metrics, &super::health::Reporter),
) -> Result<()> {
    let mut epochs = Epochs {
        collector: super::epoch::Collector::default(),
        scheduler: super::epoch::Controller::new(settings.epoch.clone()),
    };
    let interval = Duration::from_millis(settings.checkpoint_interval_ms);
    let mut deadline = tokio::time::Instant::now() + interval;
    loop {
        let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
        let pending = durable.checkpoint_end < durable.end;
        let stopping = *stop.borrow()
            || (settings.max_transactions != 0
                && count.0.source.total() >= settings.max_transactions);
        if pending
            && (stopping
                || tokio::time::Instant::now() >= deadline
                || u64::try_from(open.query.run_count())? >= settings.maintenance_ticks)
        {
            count.1.record(open, "maintenance")?;
            timeout(
                Duration::from_secs(settings.health.operation_timeout_secs),
                maintain(open, count.0),
            )
            .await
            .context("worker checkpoint timed out; recover retained WAL")??;
            stream.acknowledge(&mut open.session.client, &open.writer, open.query.plan()).await?;
            deadline = tokio::time::Instant::now() + interval;
            count.1.record(open, "ready")?;
        }
        if stopping {
            return Ok(());
        }
        let transaction = tokio::select! {
            signal=stop.changed() => {signal.context("worker stop monitor closed")?;continue;}
            ()=tokio::time::sleep_until(deadline), if pending => {continue;}
            collection=open.collector.completed() => {collection?; continue;}
            transaction=epochs.collector.recv(stream) => transaction?,
        };
        let progress = Progress::new(
            transaction.transaction.xid,
            &transaction.transaction.commit_lsn,
            &transaction.transaction.end_lsn,
        )?;
        if durable.covers(&progress)? {
            continue;
        }
        if !pending {
            deadline = tokio::time::Instant::now() + interval;
        }
        count.1.record(open, "publishing")?;
        timeout(
            Duration::from_secs(settings.health.operation_timeout_secs),
            publish_group(open, stream, transaction, &mut epochs, (settings, stop, count.0)),
        )
        .await
        .context("worker publication timed out; recover retained WAL without ACK")??;
        count.1.record(open, "ready")?;
    }
}

struct Epochs {
    collector: super::epoch::Collector,
    scheduler: super::epoch::Controller,
}
async fn publish_group(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    transaction: super::epoch::collect::Input,
    epochs: &mut Epochs,
    control: (&Settings, &mut watch::Receiver<bool>, &mut Metrics),
) -> Result<()> {
    let (settings, stop, metrics) = control;
    let remaining = if settings.max_transactions == 0 {
        u64::MAX
    } else {
        settings.max_transactions.saturating_sub(metrics.source.total())
    };
    let grouped = epochs
        .collector
        .gather(
            transaction,
            stream,
            &mut epochs.scheduler,
            super::epoch::Bounds {
                limits: open.replication.execution,
                policy: &settings.epoch,
                remaining,
                source_changes: open.replication.listener.max_transaction_changes,
                stop,
            },
        )
        .await?;
    let age = grouped.epoch.age();
    let work = grouped.epoch.work();
    metrics.source.attempt(grouped.epoch.progress()?, grouped.epoch.transactions())?;
    let executing = Instant::now();
    publish(open, &grouped.epoch, metrics).await?;
    epochs.scheduler.record(work, executing.elapsed());
    metrics.source.complete()?;
    metrics.failures = 0;
    emit(
        &json!({"event":"epoch","time":open.query.time(),"transactions":grouped.epoch.transactions(),"source_bytes":grouped.epoch.bytes(),"oldest_source_age_ms":age.as_millis(),"publication_source_age_ms":grouped.epoch.age().as_millis(),"received_to_sink_ms":grouped.epoch.received_age().as_millis(),"freshness_target_missed":grouped.epoch.age()>Duration::from_millis(settings.epoch.freshness_target_ms),"predicted_service_us":grouped.predicted.as_micros(),"reason":grouped.reason}),
    )?;
    Ok(())
}

async fn publish(
    open: &mut Open,
    epoch: &super::epoch::Epoch,
    metrics: &mut Metrics,
) -> Result<()> {
    let transaction = epoch.transaction();
    let measured = open.settings.phase_metrics;
    let mut phases = super::measurements::Phases::new(measured);
    let started = Instant::now();
    let time = open.query.time().checked_add(1).context("worker logical time overflow")?;
    let input = relational::inputs(&transaction.batch, open.compiled.relational(), time)?;
    phases.mark("inputs");
    let mut preparation = crate::timing::Profile::new(measured);
    let prepared = match open
        .query
        .prepare_memtable_profiled(input.clone(), open.settings.memtable_bytes, &mut preparation)
        .await
    {
        Ok(work) => work,
        Err(error) if error.is::<crate::engine::plan::relational::MemtableFull>() => {
            preparation.mark("failed_preparation_tail");
            maintain(open, metrics).await?;
            preparation.mark("forced_maintenance");
            open.query
                .prepare_memtable_profiled(input, open.settings.memtable_bytes, &mut preparation)
                .await?
        }
        Err(error) => return Err(error),
    };
    preparation.mark("candidate_validation");
    phases.mark("prepare_including_object_io");
    let deltas = relational::deltas(&prepared.output().batch, open.compiled.output())?;
    let progress = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
    phases.mark("finalize");
    let publication = open
        .writer
        .publish_epoch(
            &mut open.session.client,
            open.query.plan(),
            (time, &progress, &deltas, epoch.transactions()),
            measured,
        )
        .await?;
    phases.mark("publication");
    open.query.commit(prepared)?;
    phases.mark("local_completion");
    let cache = open.query.cache_stats()?;
    let mut event = json!({"event":"published","time":time,"memtable_bytes":open.query.memtable_bytes()?,"end_lsn":transaction.end_lsn,"changes":transaction.changes.len(),"source_transactions":epoch.transactions(),"elapsed_ms":started.elapsed().as_millis(),"cache":{"bytes":cache.bytes,"entries":cache.entries,"live_bytes":cache.live_bytes,"pins":cache.pins,"hits":cache.hits,"misses":cache.misses}});
    if let Some(phases) = phases.finish(epoch.decode_us())? {
        event["phases_us"] = phases;
        event["publication_us"] = serde_json::to_value(publication)?;
        event["preparation_us"] = serde_json::to_value(preparation.finish())?;
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
    let prepared = open.query.prepare_flush_protected(&protection).await?;
    let checkpoint = open.query.prepared_compaction_checkpoint(&prepared)?;
    open.writer
        .checkpoint_memtable(&mut open.session.client, open.query.plan(), &checkpoint)
        .await?;
    open.query.commit_compaction(prepared)?;
    protection
        .published(&mut open.session.client, &open.writer, open.query.plan(), &checkpoint)
        .await?;
    open.collector.start(
        open.catalog.clone(),
        open.store.clone(),
        open.settings.health.operation_timeout_secs,
    )?;
    metrics.maintenance =
        metrics.maintenance.checked_add(1).context("worker maintenance metric overflow")?;
    ensure!(open.query.time() == checkpoint.time, "worker maintenance advanced time");
    let mut event = json!({"event":"maintenance","time":open.query.time(),"epoch":open.writer.epoch(),"previous_runs":runs,"collection":"background"});
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
