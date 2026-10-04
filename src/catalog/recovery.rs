use super::{Binding, Catalog, Lsn, Progress, Publication, Stored, read};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, IsolationLevel};
#[cfg(test)]
mod tests;

/// One consistent durable publication boundary, including its source contract.
#[derive(Debug, Clone)]
pub struct Durable {
    /// Coarse arrangement membership and publication epoch.
    pub stored: Stored,
    /// Immutable source and destination registration.
    pub binding: Binding,
    /// Worker ownership counter observed in the same snapshot.
    pub fence: u64,
    /// Last source commit address; equal to end at an empty bootstrap boundary.
    pub commit: Lsn,
    /// Last durably published source end address.
    pub end: Lsn,
    /// Last committed transaction identity; absent only at bootstrap.
    pub xid: Option<u32>,
}
/// Authoritative resolution of one uncertain publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// Exact candidate, transaction identity and source position are durable.
    Committed,
    /// Exact prior boundary remains durable; the attempt did not commit.
    NotCommitted,
}
impl Durable {
    /// Decide whether a replayed committed transaction is already durably covered.
    /// The replication source must match this boundary's verified source registration.
    ///
    /// # Errors
    /// Rejects overlapping positions or a conflicting identity at the exact durable end.
    pub fn covers(&self, progress: &Progress) -> Result<bool> {
        ensure!(progress.end >= progress.commit, "invalid replay transaction positions");
        if progress.end < self.end {
            return Ok(true);
        }
        if progress.end == self.end {
            ensure!(
                self.commit == progress.commit && self.xid == Some(progress.xid),
                "conflicting transaction at durable source end"
            );
            return Ok(true);
        }
        ensure!(progress.commit >= self.end, "source transaction overlaps durable boundary");
        Ok(false)
    }
}
impl Catalog {
    /// Read arrangements and source progress from one repeatable-read snapshot.
    /// No object bytes are opened here; cold reopen remains mandatory before execution.
    ///
    /// # Errors
    /// Rejects missing/incompatible progress, invalid positions or corrupt membership.
    pub async fn load_durable(&self, sql: &mut Client, plan: &Plan) -> Result<Option<Durable>> {
        load(self, sql, plan, false).await
    }
}
async fn load(
    catalog: &Catalog,
    sql: &mut Client,
    plan: &Plan,
    lock: bool,
) -> Result<Option<Durable>> {
    let tx = sql
        .build_transaction()
        .isolation_level(if lock {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::RepeatableRead
        })
        .read_only(!lock)
        .start()
        .await?;
    if lock {
        tx.query_opt(
            &format!(
                "SELECT query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE",
                catalog.schema
            ),
            &[&catalog.query],
        )
        .await?;
    }
    let Some(stored) = read::boundary(&tx, catalog, plan).await? else {
        tx.commit().await?;
        return Ok(None);
    };
    let row = tx.query_opt(&format!("SELECT binding,sink_table,fence,commit_lsn::text,end_lsn::text,xid FROM {}.pgderive_progress WHERE query_id=$1", catalog.schema), &[&catalog.query]).await?.context("query has no durable publication contract")?;
    let binding: Binding = serde_json::from_value(row.try_get(0)?)?;
    binding.validate()?;
    ensure!(
        row.try_get::<_, String>(1)? == binding.sink.table(),
        "durable destination identity mismatch"
    );
    let fence = u64::try_from(row.try_get::<_, i64>(2)?)?;
    let commit = row.try_get::<_, String>(3)?.parse()?;
    let end = row.try_get::<_, String>(4)?.parse()?;
    let xid = row.try_get::<_, Option<i64>>(5)?.map(u32::try_from).transpose()?;
    ensure!(fence > 0 && end >= commit, "invalid durable fence/source position");
    ensure!(
        if stored.checkpoint.time == 0 { xid.is_none() && commit == end } else { xid.is_some() },
        "durable transaction identity/clock mismatch"
    );
    tx.commit().await?;
    Ok(Some(Durable { stored, binding, fence, commit, end, xid }))
}

impl super::Writer {
    /// Recheck this capability against the authoritative boundary before feedback.
    ///
    /// # Errors
    /// Rejects uncertain publication, replaced writers or changed membership/progress.
    pub async fn confirmed(&self, sql: &mut Client, plan: &Plan) -> Result<Durable> {
        ensure!(!self.uncertain, "uncertain publication cannot authorize acknowledgement");
        let durable = self
            .catalog
            .load_durable(sql, plan)
            .await?
            .context("missing acknowledgement boundary")?;
        ensure!(
            durable.binding == self.binding
                && durable.fence == u64::try_from(self.fence)?
                && durable.stored.epoch == self.epoch
                && durable.stored.checkpoint == self.checkpoint
                && durable.end == self.end
                && durable.commit == self.last_commit
                && durable.xid == self.last_xid,
            "acknowledgement capability is stale or incompatible"
        );
        Ok(durable)
    }
    /// Resolve an uncertain COMMIT using the authoritative publication boundary.
    ///
    /// Exact committed candidates update this capability's durable position. An exact
    /// unchanged prior boundary permits retry; replaced workers or other outcomes fail.
    /// No destination DML or replication acknowledgement occurs during resolution.
    ///
    /// # Errors
    /// Rejects incompatible, superseded, or nonmatching outcomes and database failures.
    pub async fn reconcile(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        attempt: Publication<'_>,
    ) -> Result<Resolution> {
        attempt.checkpoint.validate(plan)?;
        let durable = load(&self.catalog, sql, plan, true)
            .await?
            .context("missing durable query during resolution")?;
        ensure!(
            durable.binding == self.binding && durable.fence == u64::try_from(self.fence)?,
            "publication worker replaced during resolution"
        );
        let resolution = resolve(self, &durable, attempt.checkpoint, attempt.progress)?;
        if resolution == Resolution::Committed {
            self.epoch = durable.stored.epoch;
            self.time = durable.stored.checkpoint.time;
            self.end = durable.end;
            self.checkpoint = durable.stored.checkpoint;
            self.last_commit = durable.commit;
            self.last_xid = durable.xid;
        }
        self.uncertain = false;
        Ok(resolution)
    }
}
fn resolve(
    writer: &super::Writer,
    durable: &Durable,
    checkpoint: &Checkpoint,
    progress: &Progress,
) -> Result<Resolution> {
    ensure!(
        writer.time.checked_add(1) == Some(checkpoint.time),
        "resolution attempt clock mismatch"
    );
    ensure!(
        progress.end > writer.end && progress.end >= progress.commit,
        "resolution attempt source position mismatch"
    );
    ensure!(progress.commit >= writer.end, "resolution commit precedes prior end");
    if durable.stored.epoch == writer.epoch
        && durable.stored.checkpoint.time == writer.time
        && durable.end == writer.end
        && durable.commit == writer.last_commit
        && durable.xid == writer.last_xid
        && durable.stored.checkpoint == writer.checkpoint
    {
        return Ok(Resolution::NotCommitted);
    }
    ensure!(
        writer.epoch.checked_add(1) == Some(durable.stored.epoch)
            && durable.stored.checkpoint == *checkpoint
            && durable.commit == progress.commit
            && durable.end == progress.end
            && durable.xid == Some(progress.xid),
        "uncertain publication has a nonmatching authoritative outcome"
    );
    Ok(Resolution::Committed)
}
