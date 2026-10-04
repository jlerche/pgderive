use super::{Catalog, Deltas, Lsn, Progress, Sink, read, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, Transaction};

/// Immutable source and destination contract bound to this query registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// Stable source registration (database/publication/slot/schema contract).
    pub source: String,
    /// Owned explicit destination table and encoding.
    pub sink: Sink,
}
impl Binding {
    pub(super) fn validate(&self) -> Result<()> {
        self.sink.validate()?;
        ensure!(!self.source.is_empty() && self.source.len() <= 4096, "invalid source binding");
        Ok(())
    }
}
/// Complete prepared transaction, whose immutable objects must already exist.
#[derive(Clone, Copy)]
pub struct Publication<'a> {
    /// Checked complete candidate arrangement membership.
    pub checkpoint: &'a Checkpoint,
    /// Complete source transaction's progress.
    pub progress: &'a Progress,
    /// Destination deltas from that same preparation.
    pub deltas: &'a Deltas,
}
/// Exclusive publication capability fenced by a `PostgreSQL` counter.
///
/// A newly claimed capability invalidates every previously claimed writer.
/// Do not retry an uncertain COMMIT blindly; reload the authoritative boundary.
pub struct Writer {
    pub(super) catalog: Catalog,
    pub(super) binding: Binding,
    pub(super) fence: i64,
    pub(super) epoch: u64,
    pub(super) time: u64,
    pub(super) end: Lsn,
    pub(super) checkpoint: Checkpoint,
    pub(super) last_commit: Lsn,
    pub(super) last_xid: Option<u32>,
    pub(super) uncertain: bool,
}
impl Catalog {
    /// Claim publication ownership, initializing progress only at an empty tick zero.
    /// The caller must verify the source binding and supply its bootstrap position.
    /// Destinations are owned engine tables; they must be empty on first registration.
    ///
    /// # Errors
    /// Rejects incompatible registration, nonempty bootstrap, or `PostgreSQL` failures.
    pub async fn claim(
        &self,
        sql: &mut Client,
        plan: &Plan,
        binding: Binding,
        initial: Lsn,
    ) -> Result<Writer> {
        binding.validate()?;
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        let row = tx.query_one(&format!("SELECT plan_identity,definition,logical_time,epoch FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE", self.schema), &[&self.query]).await?;
        ensure!(
            row.try_get::<_, String>(0)? == plan.identity()
                && row.try_get::<_, serde_json::Value>(1)?
                    == serde_json::to_value(plan.definition())?,
            "publication registration mismatch"
        );
        let time = u64::try_from(row.try_get::<_, i64>(2)?)?;
        let epoch = u64::try_from(row.try_get::<_, i64>(3)?)?;
        let checkpoint = read::boundary(&tx, self, plan)
            .await?
            .context("missing claimed checkpoint")?
            .checkpoint;
        let encoded = serde_json::to_value(&binding)?;
        let row = tx.query_opt(&format!("SELECT binding,fence,end_lsn::text,commit_lsn::text,xid FROM {}.pgderive_progress WHERE query_id=$1 FOR UPDATE", self.schema), &[&self.query]).await?;
        let (fence, end, last_commit, last_xid) = if let Some(row) = row {
            ensure!(
                row.try_get::<_, serde_json::Value>(0)? == encoded,
                "source/destination binding mismatch"
            );
            let fence =
                row.try_get::<_, i64>(1)?.checked_add(1).context("worker fence overflow")?;
            let end = row.try_get::<_, String>(2)?.parse()?;
            let commit = row.try_get::<_, String>(3)?.parse()?;
            let xid = row.try_get::<_, Option<i64>>(4)?.map(u32::try_from).transpose()?;
            ensure!(
                end >= commit
                    && if time == 0 { xid.is_none() && end == commit } else { xid.is_some() },
                "invalid claimed source progress"
            );
            tx.execute(
                &format!("UPDATE {}.pgderive_progress SET fence=$2 WHERE query_id=$1", self.schema),
                &[&self.query, &fence],
            )
            .await?;
            (fence, end, commit, xid)
        } else {
            initialize(&tx, self, &binding, time, initial).await?;
            (1, initial, initial, None)
        };
        tx.commit()
            .await
            .context("worker claim COMMIT requires authoritative reload on failure")?;
        Ok(Writer {
            catalog: self.clone(),
            binding,
            fence,
            epoch,
            time,
            end,
            checkpoint,
            last_commit,
            last_xid,
            uncertain: false,
        })
    }
}
async fn initialize(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    binding: &Binding,
    time: u64,
    initial: Lsn,
) -> Result<()> {
    ensure!(time == 0, "publication must initialize at logical tick zero");
    let roots = tx
        .query_one(
            &format!("SELECT COUNT(*) FROM {}.pgderive_objects WHERE query_id=$1", catalog.schema),
            &[&catalog.query],
        )
        .await?;
    ensure!(
        roots.try_get::<_, i64>(0)? == 0,
        "publication bootstrap has nonempty arrangement roots"
    );
    binding.sink.install(tx, catalog).await?;
    let rows = tx
        .query_one(
            &format!("SELECT COUNT(*) FROM {}.{}", catalog.schema, binding.sink.table()),
            &[],
        )
        .await?;
    ensure!(rows.try_get::<_, i64>(0)? == 0, "publication destination must initially be empty");
    let encoded = serde_json::to_value(binding)?;
    let initial = initial.to_string();
    tx.execute(&format!("INSERT INTO {}.pgderive_progress(query_id,binding,sink_table,fence,commit_lsn,end_lsn) VALUES($1,$2,$3,1,$4::text::pg_lsn,$4::text::pg_lsn)", catalog.schema), &[&catalog.query, &encoded, &binding.sink.table(), &initial]).await?;
    Ok(())
}
impl Writer {
    /// Last confirmed durable source end position; never advances on failed COMMIT.
    #[must_use]
    pub const fn end(&self) -> Lsn {
        self.end
    }
    /// Current durable arrangement publication epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Publish memberships, destination DML and source position with one durable COMMIT.
    /// Caller holds exclusive runtime ownership, then commits local visibility on success.
    /// Only a successful durable boundary authorizes replication acknowledgement.
    ///
    /// # Errors
    /// Rejects stale workers/epochs, wrong clocks, replay, DML and `PostgreSQL` failures.
    /// An uncertain COMMIT requires authoritative recovery before any retry.
    pub async fn publish(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        publication: Publication<'_>,
    ) -> Result<()> {
        ensure!(!self.uncertain, "uncertain COMMIT must be reconciled before publication");
        let Publication { checkpoint, progress, deltas } = publication;
        checkpoint.validate(plan)?;
        ensure!(
            self.time.checked_add(1) == Some(checkpoint.time),
            "publication logical tick mismatch"
        );
        ensure!(progress.commit >= self.end, "publication commit precedes durable source end");
        ensure!(progress.end > self.end, "publication source end is stale");
        ensure!(progress.end >= progress.commit, "publication source end precedes commit");
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        let epoch = write::stage(&tx, &self.catalog, plan, checkpoint, self.epoch).await?;
        let affected = tx.execute(&format!("UPDATE {}.pgderive_progress SET commit_lsn=$3::text::pg_lsn,end_lsn=$4::text::pg_lsn,xid=$5 WHERE query_id=$1 AND fence=$2 AND end_lsn=$6::text::pg_lsn AND binding=$7", self.catalog.schema),
            &[&self.catalog.query, &self.fence, &progress.commit.to_string(), &progress.end.to_string(), &i64::from(progress.xid), &self.end.to_string(), &serde_json::to_value(&self.binding)?]).await?;
        ensure!(affected == 1, "stale or incompatible publication worker");
        deltas.apply(&tx, &self.catalog, &self.binding.sink).await?;
        self.uncertain = true;
        tx.commit()
            .await
            .context("publication COMMIT outcome requires authoritative reload on failure")?;
        self.epoch = epoch;
        self.time = checkpoint.time;
        self.end = progress.end;
        self.checkpoint = checkpoint.clone();
        self.last_commit = progress.commit;
        self.last_xid = Some(progress.xid);
        self.uncertain = false;
        Ok(())
    }
}
