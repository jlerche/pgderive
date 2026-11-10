mod admission;
mod retry_wait;
mod startup;
use super::epoch::model::Observation;
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

pub(super) async fn run_controlled(
    config: Config,
    settings: Settings,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let identity = crate::diagnostics::Identity::new(&config, &settings);
    Box::pin(crate::diagnostics::scope(identity, retry(&config, &settings, &mut stop, None))).await
}
pub(super) async fn run_initial<S: std::future::Future<Output = std::io::Result<()>>>(
    config: Config,
    settings: Settings,
    initial: Option<Session>,
    shutdown: S,
) -> Result<()> {
    let (signal, mut stop) = watch::channel(false);
    let identity = crate::diagnostics::Identity::new(&config, &settings);
    let work = crate::diagnostics::scope(identity, retry(&config, &settings, &mut stop, initial));
    tokio::pin!(work);
    tokio::pin!(shutdown);
    tokio::select! {
        result = &mut work => result,
        result = &mut shutdown => {
            signal.send_replace(true);
            crate::outcome::combine(work.await, result.context("worker shutdown monitor failed"), "worker shutdown monitor")
        }
    }
}
#[derive(Default)]
struct Metrics {
    source: super::epoch::Count,
    retries: u64,
    maintenance: u64,
    failures: u32,
    narrow: Option<crate::catalog::Lsn>,
}
impl Metrics {
    const fn stopping(&self, settings: &Settings, requested: bool) -> bool {
        requested
            || (settings.max_transactions != 0 && self.source.total() >= settings.max_transactions)
    }
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
            Ok(session) => {
                Box::pin(attempt(config, settings, stop, &mut metrics, (session, &ticket))).await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok((time, end)) => {
                return emit(
                    &json!({"event":"stopped","time":time,"end_lsn":end,"transactions":metrics.source.total(),"retries":metrics.retries,"maintenance":metrics.maintenance,"elapsed_ms":started.elapsed().as_millis()}),
                );
            }
            Err(error)
                if error.is::<super::epoch::collect::NarrowEpochs>()
                    || (error.is::<crate::engine::plan::relational::PooledMemtableFull>()
                        && !permanent(&error)) =>
            {
                tokio::select! {
                    ()=sleep(Duration::from_millis(settings.retry_delay_ms)) => {}
                    signal=stop.changed() => {signal.context("worker stop monitor closed")?;}
                }
                drop(require_running(error, *stop.borrow())?);
            }
            Err(error) => {
                metrics.failures =
                    metrics.failures.checked_add(1).context("worker retry counter overflow")?;
                let attempts = metrics.failures;
                observe_failure(config, settings, &ticket, attempts, &error).await?;
                let error = require_running(error, *stop.borrow())?;
                if permanent(&error) {
                    emit(
                        &json!({"event":"capacity_exceeded","attempt":attempts,"error":format!("{error:#}")}),
                    )?;
                    return Err(error.context(
                        "permanent capacity failure; source remains at authoritative progress",
                    ));
                }
                metrics.retries =
                    metrics.retries.checked_add(1).context("worker retry metric overflow")?;
                emit(&json!({"event":"retry","attempt":attempts,"error":format!("{error:#}")}))?;
                if attempts >= settings.retry_attempts {
                    return Err(error.context(
                        "worker retry budget exhausted; source remains at authoritative progress",
                    ));
                }
                retry_wait::backoff(settings.retry_delay_ms, attempts, stop).await?;
                drop(require_running(error, *stop.borrow())?);
            }
        }
    }
}
async fn observe_failure(
    config: &Config,
    settings: &Settings,
    ticket: &super::health::attempts::Ticket,
    attempts: u32,
    error: &anyhow::Error,
) -> Result<()> {
    let observation = timeout(
        Duration::from_secs(settings.health.stale_secs),
        ticket.failure(config, settings, attempts, error),
    )
    .await
    .context("worker failure observation timed out")
    .and_then(std::convert::identity);
    match observation {
        Ok(true) => Ok(()),
        Ok(false) => anyhow::bail!("query attempt superseded by another owner: {error:#}"),
        Err(diagnostic) => emit(
            &json!({"event":"diagnostic_failure","attempt":attempts,"error":format!("{diagnostic:#}")}),
        ),
    }
}
fn require_running(error: anyhow::Error, stopping: bool) -> Result<anyhow::Error> {
    if stopping {
        return Err(error.context("worker stopped with an incomplete durable drain"));
    }
    Ok(error)
}
fn permanent(error: &anyhow::Error) -> bool {
    error.is::<crate::engine::capacity::Invariant>()
        || crate::storage::operation_budget_exhausted(error)
        || error.is::<crate::engine::execution::CapacityExceeded>()
        || error.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::StorageFull | std::io::ErrorKind::OutOfMemory
            )
        })
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
    let startup::Opened { mut open, mut stream, durable, running } =
        startup::open(config, settings, initial.0, (stop, &mut metrics.source)).await?;
    if !running {
        return Ok((open.query.time(), durable.end.to_string()));
    }
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
    let cancelled = super::checkpoint::cancel(&mut open).await;
    let result = crate::outcome::combine(result, cancelled, "checkpoint upload cancellation");
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
        collector: super::epoch::Collector::new(open.source_keys.clone()),
        scheduler: super::epoch::Controller::new(settings.epoch.clone()),
    };
    let interval = Duration::from_millis(settings.checkpoint_interval_ms);
    let mut deadline = tokio::time::Instant::now() + interval;
    let mut pressure = Pressure::new(open);
    loop {
        start_collector(open, settings)?;
        let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
        let pending = durable.checkpoint_end < durable.end;
        let stopping = count.0.stopping(settings, *stop.borrow());
        let pooled_pressure = pressure.requested()?;
        if stopping || (pooled_pressure && pending) || super::checkpoint::must_drain(open, &durable)
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
            count.1.record(open, epochs.idle_phase())?;
            if stopping {
                return Ok(());
            }
            continue;
        }
        if admit(open, stream, (&mut *count.0, count.1), durable.stored.checkpoint.time).await? {
            count.1.record(open, epochs.idle_phase())?;
            deadline = tokio::time::Instant::now() + interval;
            continue;
        }
        if super::checkpoint::due(open, pending, deadline)? {
            super::checkpoint::start(open, durable.stored.checkpoint.time).await?;
        }
        let input = match next_event(
            open,
            stream,
            &mut epochs,
            (stop, count.0, &mut pressure),
            (deadline, pending),
        )
        .await?
        {
            Event::Wake => continue,
            Event::Checkpoint => {
                deadline = tokio::time::Instant::now() + interval;
                continue;
            }
            Event::Retry => None,
            Event::Transaction(transaction) => Some(transaction),
        };
        let Pending::Attempt(input) =
            pending_input(open, &mut epochs, input, (settings, stop, &*count.0), &durable)?
        else {
            continue;
        };
        if !pending {
            deadline = tokio::time::Instant::now() + interval;
        }
        count.1.record(
            open,
            if epochs.collector.blocked() { "evaluation_blocked" } else { "publishing" },
        )?;
        timeout(
            Duration::from_secs(settings.health.operation_timeout_secs),
            publish_group(open, stream, input, &mut epochs, (settings, stop, count.0)),
        )
        .await
        .context("worker publication timed out; recover retained WAL without ACK")??;
        count.1.record(open, epochs.idle_phase())?;
    }
}

fn start_collector(open: &mut Open, settings: &Settings) -> Result<()> {
    open.collector.start(
        open.catalog.clone(),
        open.store.clone(),
        (settings.health.operation_timeout_secs, settings.gc_interval_ms),
        open.query.workspaces(),
    )
}

enum Pending {
    Consumed,
    Attempt(Option<super::epoch::collect::Input>),
}
/// Skip durable input and merge input that arrives during a blocked epoch's backoff.
fn pending_input(
    open: &Open,
    epochs: &mut Epochs,
    input: Option<super::epoch::collect::Input>,
    control: (&Settings, &mut watch::Receiver<bool>, &Metrics),
    durable: &crate::catalog::Durable,
) -> Result<Pending> {
    let Some(input) = input else {
        return Ok(Pending::Attempt(None));
    };
    let progress = Progress::new(
        input.transaction.xid,
        &input.transaction.commit_lsn,
        &input.transaction.end_lsn,
    )?;
    if durable.covers(&progress)? {
        return Ok(Pending::Consumed);
    }
    let (settings, stop, metrics) = control;
    let bounds = bounds(open, settings, stop, metrics);
    Ok(epochs
        .collector
        .absorb(input, &mut epochs.scheduler, &bounds)?
        .map_or(Pending::Consumed, |input| Pending::Attempt(Some(input))))
}
const fn bounds<'a>(
    open: &Open,
    settings: &'a Settings,
    stop: &'a mut watch::Receiver<bool>,
    metrics: &Metrics,
) -> super::epoch::Bounds<'a> {
    let remaining = if settings.max_transactions == 0 {
        u64::MAX
    } else {
        settings.max_transactions.saturating_sub(metrics.source.total())
    };
    super::epoch::Bounds {
        limits: open.replication.execution,
        policy: &settings.epoch,
        remaining,
        source_changes: open.replication.listener.max_transaction_changes,
        single: metrics.narrow.is_some(),
        stop,
    }
}

/// Reserve headroom for the checkpoint already in service before receiving more input.
async fn admit(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    count: (&mut Metrics, &super::health::Reporter),
    checkpoint_time: u64,
) -> Result<bool> {
    if !super::checkpoint::debt_pressure(open, checkpoint_time)? {
        return Ok(false);
    }
    emit(&json!({"event":"checkpoint_backpressure","reason":"maintenance_debt",
        "time":open.query.time(),"memtable_bytes":open.query.memtable_bytes()?,
        "runs":open.query.run_count(),"replay_epochs":open.query.time().saturating_sub(checkpoint_time),
        "checkpoint_in_flight":true}))?;
    count.1.record(open, "maintenance")?;
    timeout(
        Duration::from_secs(open.settings.health.operation_timeout_secs),
        super::checkpoint::finish(open),
    )
    .await
    .context("debt checkpoint timed out; retain source WAL")??;
    count.0.maintenance =
        count.0.maintenance.checked_add(1).context("maintenance counter overflow")?;
    stream.acknowledge(&mut open.session.client, &open.writer, open.query.plan()).await?;
    Ok(true)
}

enum Event {
    Wake,
    Checkpoint,
    Retry,
    Transaction(super::epoch::collect::Input),
}
type Control<'a> = (&'a mut watch::Receiver<bool>, &'a mut Metrics, &'a mut Pressure);
async fn next_event(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    epochs: &mut Epochs,
    control: Control<'_>,
    timer: (tokio::time::Instant, bool),
) -> Result<Event> {
    let (stop, metrics, pressure) = control;
    let seconds = open.settings.health.operation_timeout_secs;
    let retry = epochs.collector.retry_at();
    tokio::select! {
        changed=pressure.changed() => { changed.context("capacity monitor closed")?; Ok(Event::Wake) },
        ()=tokio::time::sleep_until(retry.unwrap_or(timer.0)), if retry.is_some() => Ok(Event::Retry),
        signal=stop.changed() => {signal.context("worker stop monitor closed")?; Ok(Event::Wake)}
        ()=tokio::time::sleep_until(timer.0), if timer.1 && open.checkpoint.is_none() => Ok(Event::Wake),
        uploaded=super::checkpoint::ready(&mut open.checkpoint) => {
            timeout(Duration::from_secs(seconds), super::checkpoint::complete(open, uploaded?))
                .await.context("checkpoint publication timed out")??;
            metrics.maintenance = metrics.maintenance.checked_add(1).context("maintenance counter overflow")?;
            stream.acknowledge(&mut open.session.client, &open.writer, open.query.plan()).await?;
            Ok(Event::Checkpoint)
        }
        ()=tokio::time::sleep_until(open.collector.deadline()), if open.collector.waiting() => Ok(Event::Wake),
        collection=open.collector.completed() => {collection?; Ok(Event::Wake)}
        transaction=epochs.collector.recv(stream), if !epochs.collector.capacity_blocked() => Ok(Event::Transaction(transaction?)),
    }
}

struct Epochs {
    collector: super::epoch::Collector,
    scheduler: super::epoch::Controller,
}
impl Epochs {
    fn idle_phase(&self) -> &'static str {
        if self.collector.capacity_blocked() {
            "capacity_blocked"
        } else if self.collector.blocked() {
            "evaluation_blocked"
        } else {
            "ready"
        }
    }
}
async fn publish_group(
    open: &mut Open,
    stream: &mut crate::acknowledged::Stream,
    transaction: Option<super::epoch::collect::Input>,
    epochs: &mut Epochs,
    control: (&Settings, &mut watch::Receiver<bool>, &mut Metrics),
) -> Result<()> {
    let (settings, stop, metrics) = control;
    let collection = bounds(open, settings, stop, metrics);
    let grouped =
        epochs.collector.gather(transaction, stream, &mut epochs.scheduler, collection).await?;
    let age = grouped.epoch.age();
    let work = grouped.epoch.work();
    metrics.source.attempt(grouped.epoch.progress()?, grouped.epoch.transactions())?;
    let observation = match publish(open, &grouped.epoch, metrics).await {
        Ok(observation) => observation,
        Err(error)
            if error.is::<crate::engine::plan::relational::PooledMemtableFull>()
                && !permanent(&error) =>
        {
            metrics.source.abandon();
            epochs.collector.block_capacity(grouped.epoch, error);
            emit(
                &json!({"event":"capacity_blocked","time":open.query.time(),"reason":"shared_memtable"}),
            )?;
            return Ok(());
        }
        Err(error) if error.is::<crate::engine::execution::EvaluationFailed>() => {
            metrics.source.abandon();
            let narrowing = metrics.narrow.is_none() && grouped.epoch.transactions() > 1;
            metrics.narrow.get_or_insert(grouped.epoch.progress()?.end);
            if narrowing {
                emit(
                    &json!({"event":"epoch_narrowing","transactions":grouped.epoch.transactions()}),
                )?;
                return Err(error.context(super::epoch::collect::NarrowEpochs));
            }
            let limits = open.replication.execution;
            let event = json!({
                "event":"evaluation_blocked","time":open.query.time(),
                "end_lsn":grouped.epoch.transaction().end_lsn,
                "transactions":grouped.epoch.transactions(),
                "source_bytes":grouped.epoch.bytes(),
                "byte_ceiling":settings.epoch.max_bytes.min(limits.output_bytes),
                "source_changes":grouped.epoch.transaction().changes.len(),
                "change_ceiling":open.replication.listener.max_transaction_changes,
                "source_updates":grouped.epoch.transaction().batch.updates.len(),
                "update_ceiling":limits.output_entries.min(limits.contributions),
                "oldest_source_age_ms":grouped.epoch.age().as_millis(),
                "error":format!("{error:#}"),
            });
            let origin = epochs.collector.block(grouped.epoch, error);
            let mut event = event;
            event["origin"] = serde_json::to_value(origin)?;
            emit(&event)?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    epochs.collector.published();
    let published = grouped.epoch.progress()?.end;
    metrics.narrow = metrics.narrow.filter(|through| published < *through);
    epochs.scheduler.record(work, observation);
    metrics.source.complete()?;
    metrics.failures = 0;
    emit(
        &json!({"event":"epoch","time":open.query.time(),"transactions":grouped.epoch.transactions(),"source_bytes":grouped.epoch.bytes(),"oldest_source_age_ms":age.as_millis(),"publication_source_age_ms":grouped.epoch.age().as_millis(),"received_to_sink_ms":grouped.epoch.received_age().as_millis(),"freshness_target_missed":grouped.epoch.age()>Duration::from_millis(settings.epoch.freshness_target_ms),"predicted_service_us":grouped.predicted.as_micros(),"input_work":work,"service_observed":observation,"reason":grouped.reason}),
    )?;
    Ok(())
}

async fn publish(
    open: &mut Open,
    epoch: &super::epoch::Epoch,
    metrics: &mut Metrics,
) -> Result<Observation> {
    let _foreground = open.query.workspaces().demand();
    let transaction = epoch.transaction();
    let measured = open.settings.phase_metrics;
    let mut phases = super::measurements::Phases::new(measured);
    let started = Instant::now();
    let time = open.query.time().checked_add(1).context("worker logical time overflow")?;
    let mut preparation = crate::timing::Profile::new(measured);
    let input = open.input(&transaction.batch, time, &mut preparation).await?;
    phases.mark("inputs");
    preparation.reset_clock();
    let admission::Prepared { work: prepared, disruption } =
        admission::prepare(open, input, metrics, &mut preparation).await?;
    preparation.mark("candidate_validation");
    phases.mark("prepare_including_object_io");
    let deltas =
        relational::deltas(&open.query, &prepared.output().batch, open.compiled.output()).await?;
    let progress = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
    phases.mark("finalize");
    let preparation_time =
        started.elapsed().saturating_sub(preparation.admission()).saturating_sub(disruption);
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
    let mut event = json!({"event":"published","time":time,"workspaces":open.query.workspaces().stats(),"memtable_bytes":open.query.memtable_bytes()?,"end_lsn":transaction.end_lsn,"changes":transaction.changes.len(),"source_transactions":epoch.transactions(),"elapsed_ms":started.elapsed().as_millis(),"cache":crate::worker::health::cache_diagnostics(&cache)});
    let admission = preparation.admission();
    if let Some(phases) = phases.finish(epoch.decode_us())? {
        event["phases_us"] = phases;
        event["publication_us"] = serde_json::to_value(&publication.timings)?;
        event["preparation_us"] = serde_json::to_value(preparation.finish())?;
    }
    emit(&event)?;
    Ok(Observation::published(started.elapsed(), preparation_time, admission, &publication))
}
async fn maintain(open: &mut Open, metrics: &mut Metrics) -> Result<()> {
    let completed = super::checkpoint::flush(open).await?;
    metrics.maintenance =
        metrics.maintenance.checked_add(completed).context("worker maintenance metric overflow")?;
    Ok(())
}
pub(super) fn emit(event: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    crate::diagnostics::write(&mut output, event)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

async fn pooled_pressure(
    receiver: Option<&mut watch::Receiver<u64>>,
) -> Result<(), watch::error::RecvError> {
    match receiver {
        Some(receiver) => receiver.changed().await,
        None => std::future::pending().await,
    }
}

struct Pressure(Option<watch::Receiver<u64>>, bool);
impl Pressure {
    async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        pooled_pressure(self.0.as_mut()).await?;
        self.1 = true;
        Ok(())
    }
    fn new(open: &Open) -> Self {
        Self(
            open.replication.capacity.as_ref().map(|resources| resources.pressure.subscribe()),
            false,
        )
    }
    fn requested(&mut self) -> Result<bool> {
        let changed = std::mem::take(&mut self.1)
            || self.0.as_ref().map(watch::Receiver::has_changed).transpose()?.unwrap_or(false);
        if let Some(receiver) = &mut self.0 {
            drop(receiver.borrow_and_update());
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod retry_tests {
    #[tokio::test]
    async fn observed_pressure_still_requests_a_checkpoint() -> anyhow::Result<()> {
        let (sender, receiver) = tokio::sync::watch::channel(0);
        let mut pressure = super::Pressure(Some(receiver), false);
        sender.send_replace(1);
        pressure.changed().await?;
        assert!(pressure.requested()?);
        assert!(!pressure.requested()?);
        Ok(())
    }
    #[test]
    fn intrinsically_oversized_pooled_input_is_permanent() {
        let error = anyhow::Error::new(crate::engine::capacity::MemoryPressure {
            required: 2,
            limit: 1,
            tenant_only: true,
        })
        .context(crate::engine::execution::CapacityExceeded("single input too large"));
        assert!(error.is::<crate::engine::plan::relational::PooledMemtableFull>());
        assert!(super::permanent(&error));
    }
    #[test]
    fn failed_drain_cannot_be_reported_as_clean_shutdown() {
        assert!(super::require_running(anyhow::anyhow!("checkpoint failed"), true).is_err());
    }
    #[test]
    fn resource_policy_and_exhausted_storage_are_permanent_but_transient_io_is_not() {
        use std::io::{Error, ErrorKind};
        let capacity =
            anyhow::Error::new(crate::engine::execution::CapacityExceeded("test budget"))
                .context("source");
        assert!(super::permanent(&capacity));
        assert!(super::permanent(&anyhow::Error::new(Error::from(ErrorKind::StorageFull))));
        assert!(!super::permanent(&anyhow::Error::new(Error::from(ErrorKind::ConnectionReset))));
    }
}
