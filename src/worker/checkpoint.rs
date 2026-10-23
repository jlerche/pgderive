//! One protected immutable checkpoint upload, with bounded foreground completion.
use super::{open::Open, program::relational::Key};
use crate::{catalog::Protection, engine::plan::relational::UploadedFlush, transaction::Row};
use anyhow::{Context, Result, ensure};
use std::time::Instant;
use tokio::task::JoinHandle;

type Uploaded = UploadedFlush<Key, Row>;
pub(super) struct Pending {
    task: JoinHandle<Result<Uploaded>>,
    protection: Option<Protection>,
    started: Instant,
    runs: usize,
    time: u64,
    deadline: tokio::time::Instant,
    resolved: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        // Aborted uploads retain durable non-quiescent protection. Never detach
        // upload work and then declare its namespace safe for physical GC.
        self.task.abort();
    }
}
pub(super) fn must_drain(open: &Open, pending: bool) -> bool {
    pending
        && (open.query.run_count() >= crate::engine::trace::MAX_RUNS - 8
            || open
                .checkpoint
                .as_ref()
                .is_some_and(|upload| open.query.time().saturating_sub(upload.time) >= 120))
}
pub(super) fn due(open: &Open, pending: bool, deadline: tokio::time::Instant) -> Result<bool> {
    Ok(pending
        && open.checkpoint.is_none()
        && (tokio::time::Instant::now() >= deadline
            || u64::try_from(open.query.run_count())? >= open.settings.maintenance_ticks))
}
pub(super) async fn start(open: &mut Open) -> Result<()> {
    ensure!(open.checkpoint.is_none(), "checkpoint already in flight");
    let protection = open
        .writer
        .protect_upload(
            &mut open.session.client,
            &format!("worker:{}:maintenance:{}", open.nonce, open.writer.epoch()),
        )
        .await?;
    let frozen = open.query.freeze_flush(&protection)?;
    open.checkpoint = Some(Pending {
        task: tokio::spawn(frozen.upload()),
        resolved: false,
        protection: Some(protection),
        started: Instant::now(),
        runs: open.query.run_count(),
        time: open.query.time(),
        deadline: tokio::time::Instant::now()
            + std::time::Duration::from_secs(open.settings.health.operation_timeout_secs),
    });
    Ok(())
}
pub(super) async fn ready(pending: &mut Option<Pending>) -> Result<Uploaded> {
    let Some(pending) = pending.as_mut() else {
        return std::future::pending().await;
    };
    let result = tokio::time::timeout_at(pending.deadline, &mut pending.task)
        .await
        .context("checkpoint upload timed out; retain protected namespace and source WAL")?;
    pending.resolved = true;
    result.context("checkpoint upload task failed")?
}
pub(super) async fn complete(open: &mut Open, uploaded: Uploaded) -> Result<()> {
    let mut pending = open.checkpoint.take().context("checkpoint completion without upload")?;
    let checkpoint = uploaded.checkpoint();
    let prepared = open.query.prepare_uploaded_flush(&uploaded).await?;
    open.writer
        .checkpoint_memtable(&mut open.session.client, open.query.plan(), checkpoint)
        .await?;
    open.query.commit_compaction(prepared)?;
    pending
        .protection
        .take()
        .context("checkpoint upload protection missing")?
        .published(&mut open.session.client, &open.writer, open.query.plan(), checkpoint)
        .await?;
    open.collector.start(
        open.catalog.clone(),
        open.store.clone(),
        open.settings.health.operation_timeout_secs,
    )?;
    let mut event = serde_json::json!({"event":"maintenance","time":checkpoint.time,
        "published_time":open.query.time(),"epoch":open.writer.epoch(),
        "previous_runs":pending.runs,"collection":"background","upload":"background"});
    if open.settings.phase_metrics {
        event["elapsed_us"] = serde_json::to_value(pending.started.elapsed().as_micros())?;
    }
    super::drive::emit(&event)
}
pub(super) async fn finish(open: &mut Open) -> Result<()> {
    let uploaded = ready(&mut open.checkpoint).await?;
    complete(open, uploaded).await
}
/// Drain an existing older checkpoint, then checkpoint any newer published suffix.
pub(super) async fn flush(open: &mut Open) -> Result<u64> {
    let mut completed = 0;
    if open.checkpoint.is_some() {
        finish(open).await?;
        completed += 1;
    }
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    if durable.checkpoint_end < durable.end {
        start(open).await?;
        finish(open).await?;
        completed += 1;
    }
    Ok(completed)
}

/// Await cancellation before another worker attempt can acquire a fresh fence.
/// Failure never declares the upload namespace quiescent.
pub(super) async fn cancel(open: &mut Open) -> Result<()> {
    let Some(pending) = open.checkpoint.as_mut() else {
        return Ok(());
    };
    if !pending.resolved {
        pending.task.abort();
        let _completion =
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut pending.task)
                .await
                .context("checkpoint upload cancellation did not finish")?;
    }
    open.checkpoint = None;
    Ok(())
}
