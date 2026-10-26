//! One protected background consolidation; checkpoint publication stays independent.
use super::{open::Open, program::relational::Key};
use crate::{
    catalog::Protection,
    engine::plan::{Checkpoint, relational::UploadedConsolidation},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use tokio::{task::JoinHandle, time::Instant};
type Uploaded = UploadedConsolidation<Key, Row>;
pub(super) struct Pending {
    task: JoinHandle<Result<Uploaded>>,
    upload: Option<Protection>,
    reader: Option<Protection>,
    started: Instant,
    resolved: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) fn must_drain(checkpoint: &Checkpoint) -> bool {
    checkpoint.arrangements.iter().any(|member| {
        member.trace.objects.len() >= crate::engine::trace::CONSOLIDATION_ROOTS
            && member.trace.cohorts.len() > 1
    })
}
pub(super) async fn start(open: &mut Open, checkpoint: &Checkpoint) -> Result<()> {
    if open.consolidation.is_some()
        || open.checkpoint.is_some()
        || !open.query.consolidation_due(checkpoint)?
    {
        return Ok(());
    }
    super::operation::run(open.settings.health.operation_timeout_secs, reserve(open, checkpoint))
        .await
}
async fn reserve(open: &mut Open, checkpoint: &Checkpoint) -> Result<()> {
    let token = format!("worker:{}:consolidation:{}", open.nonce, open.writer.epoch());
    let reader =
        open.writer.protect_recovery(&mut open.session.client, open.query.plan(), &token).await?;
    ensure!(reader.stored()?.checkpoint == *checkpoint, "consolidation input checkpoint changed");
    let upload =
        open.writer.protect_upload(&mut open.session.client, &format!("{token}:upload")).await?;
    let frozen = open.query.capture_consolidation(checkpoint, upload.namespace()?.to_owned())?;
    open.consolidation = Some(Pending {
        task: tokio::spawn(frozen.upload()),
        upload: Some(upload),
        reader: Some(reader),
        started: Instant::now(),
        resolved: false,
    });
    Ok(())
}
pub(super) async fn ready(pending: &mut Option<Pending>) -> Result<Uploaded> {
    let Some(pending) = pending.as_mut() else {
        return std::future::pending().await;
    };
    // Storage requests have their own deadlines. A streaming merge may safely
    // exceed a foreground operation timeout without forcing a worker restart.
    let result = (&mut pending.task).await;
    pending.resolved = true;
    result.context("consolidation task failed")?
}
pub(super) async fn complete(open: &mut Open, uploaded: Uploaded) -> Result<()> {
    ensure!(open.checkpoint.is_none(), "cannot replace an in-flight checkpoint prefix");
    super::operation::run(open.settings.health.operation_timeout_secs, install(open, uploaded))
        .await
}
async fn install(open: &mut Open, uploaded: Uploaded) -> Result<()> {
    let mut pending = open.consolidation.take().context("consolidation completion without task")?;
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    let (prepared, checkpoint) =
        open.query.prepare_uploaded_consolidation(&uploaded, &durable.stored.checkpoint).await?;
    open.writer.maintain(&mut open.session.client, open.query.plan(), &checkpoint).await?;
    open.query.commit_compaction(prepared)?;
    pending
        .upload
        .take()
        .context("missing consolidation upload protection")?
        .published(&mut open.session.client, &open.writer, open.query.plan(), &checkpoint)
        .await?;
    pending
        .reader
        .take()
        .context("missing consolidation reader protection")?
        .reader_finished(&mut open.session.client)
        .await?;
    open.collector.start(
        open.catalog.clone(),
        open.store.clone(),
        open.settings.health.operation_timeout_secs,
    )?;
    super::drive::emit(&serde_json::json!({"event":"consolidation","time":checkpoint.time,
        "published_time":open.query.time(),"epoch":open.writer.epoch(),"elapsed_us":pending.started.elapsed().as_micros()}))
}
pub(super) async fn finish(open: &mut Open) -> Result<()> {
    if open.consolidation.is_some() {
        let uploaded = ready(&mut open.consolidation).await?;
        complete(open, uploaded).await?;
    }
    Ok(())
}
pub(super) async fn cancel(open: &mut Open) -> Result<()> {
    if let Some(pending) = open.consolidation.as_mut()
        && !pending.resolved
    {
        pending.task.abort();
        let _completion =
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut pending.task)
                .await
                .context("consolidation cancellation did not finish")?;
    }
    // A failed/aborted task is not proof of quiescence. Durable reservations and
    // reader pins remain for fenced recovery, just like checkpoint uploads.
    open.consolidation = None;
    Ok(())
}

/// All checkpoint callers, including forced memtable drains, reserve headroom.
/// A completed old prefix merge may leave newer durable cohorts above the bound;
/// continue consolidation until the next delta upload has its full root budget.
pub(super) async fn before_checkpoint(open: &mut Open) -> Result<()> {
    loop {
        let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
        if !must_drain(&durable.stored.checkpoint) {
            return Ok(());
        }
        start(open, &durable.stored.checkpoint).await?;
        finish(open).await?;
    }
}

/// Service debt before another checkpoint can consume reserved root headroom.
pub(super) async fn service(
    open: &mut Open,
    checkpoint: &Checkpoint,
    stopping: bool,
) -> Result<bool> {
    start(open, checkpoint).await?;
    // A maintenance_ticks=1 workload can otherwise start another checkpoint
    // on every loop and indefinitely mask the ready consolidation select arm.
    if open.checkpoint.is_none()
        && open.consolidation.as_ref().is_some_and(|pending| pending.task.is_finished())
    {
        finish(open).await?;
        return Ok(true);
    }
    let pressure = must_drain(checkpoint);
    if pressure || stopping {
        if open.checkpoint.is_some() {
            super::checkpoint::finish(open).await?;
        }
        let latest = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
        start(open, &latest.stored.checkpoint).await?;
        finish(open).await?;
    }
    Ok(pressure && !stopping)
}

#[cfg(test)]
mod tests;
