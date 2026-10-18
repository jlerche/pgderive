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
    pub source: crate::source::Contract,
    /// Owned explicit destination table and encoding.
    pub sink: Sink,
}
impl Binding {
    pub(super) fn validate(&self) -> Result<()> {
        self.sink.validate()?;
        self.source.validate()?;
        ensure!(serde_json::to_vec(&self.source)?.len() <= 65536, "invalid source binding");
        Ok(())
    }
    pub(super) async fn verify_ownership(
        &self,
        sql: &Transaction<'_>,
        catalog: &Catalog,
    ) -> Result<()> {
        let row = sql
            .query_opt(
                &format!(
                    "SELECT query_id FROM {}.pgderive_source_slots WHERE source_key=$1",
                    catalog.schema
                ),
                &[&self.source.ownership_key()?],
            )
            .await?
            .context("registered source ownership missing")?;
        ensure!(
            row.try_get::<_, String>(0)? == catalog.query,
            "registered source owned by another query"
        );
        Ok(())
    }
    async fn verify_environment(&self, sql: &Transaction<'_>) -> Result<()> {
        self.source.verify(sql).await
    }
    async fn verify_destination(&self, tx: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
        self.sink.owned(tx, catalog).await?.context("registered destination ownership missing")?;
        self.sink.prepare_index(tx, catalog).await
    }
    pub(super) async fn initialize(
        &self,
        tx: &Transaction<'_>,
        catalog: &Catalog,
        boundary: Lsn,
    ) -> Result<()> {
        self.verify_environment(tx).await?;
        self.sink.install(tx, catalog).await?;
        let rows: i64 = tx
            .query_one(
                &format!("SELECT COUNT(*) FROM {}.{}", catalog.schema, self.sink.table.as_str()),
                &[],
            )
            .await?
            .try_get(0)?;
        ensure!(rows == 0, "publication destination must initially be empty");
        let boundary = boundary.to_string();
        tx.execute(&format!("INSERT INTO {}.pgderive_progress(query_id,binding,sink_table,fence,commit_lsn,end_lsn) VALUES($1,$2,$3,1,$4::text::pg_lsn,$4::text::pg_lsn)", catalog.schema), &[&catalog.query, &serde_json::to_value(self)?, &self.sink.table.as_str(), &boundary]).await?;
        Ok(())
    }
    pub(super) fn valid_identity(time: u64, xid: Option<u32>, commit: Lsn, end: Lsn) -> bool {
        (time == 1 && xid.is_none() && end == commit) || (time > 1 && xid.is_some())
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
    pub(super) boundary: super::Durable,
    pub(super) uncertain: bool,
}
impl Catalog {
    /// Reclaim an atomically activated native source and owned destination.
    /// Every claim validates its boundary and fences all earlier writers.
    ///
    /// # Errors
    /// Rejects missing or incompatible activation and `PostgreSQL` failures.
    pub async fn claim(&self, sql: &mut Client, plan: &Plan, binding: Binding) -> Result<Writer> {
        binding.validate()?;
        let tx = sql.transaction().await?;
        super::storage::shared(&tx, self).await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        tx.query_one(
            &format!(
                "SELECT query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE",
                self.schema
            ),
            &[&self.query],
        )
        .await?;
        let super::Stored { epoch, checkpoint } =
            read::boundary(&tx, self, plan).await?.context("missing atomic source activation")?;
        let time = checkpoint.time;
        binding.verify_ownership(&tx, self).await?;
        let encoded = serde_json::to_value(&binding)?;
        let row = tx.query_one(&format!("SELECT binding,fence,end_lsn::text,commit_lsn::text,xid FROM {}.pgderive_progress WHERE query_id=$1 FOR UPDATE", self.schema), &[&self.query]).await?;
        let (fence, end, last_commit, last_xid) = {
            ensure!(
                row.try_get::<_, serde_json::Value>(0)? == encoded,
                "source/destination binding mismatch"
            );
            binding.verify_environment(&tx).await?;
            binding.verify_destination(&tx, self).await?;
            let fence =
                row.try_get::<_, i64>(1)?.checked_add(1).context("worker fence overflow")?;
            let end = row.try_get::<_, String>(2)?.parse()?;
            let commit = row.try_get::<_, String>(3)?.parse()?;
            let xid = row.try_get::<_, Option<i64>>(4)?.map(u32::try_from).transpose()?;
            ensure!(
                end >= commit && Binding::valid_identity(time, xid, commit, end),
                "invalid claimed source progress"
            );
            tx.execute(
                &format!("UPDATE {}.pgderive_progress SET fence=$2 WHERE query_id=$1", self.schema),
                &[&self.query, &fence],
            )
            .await?;
            (fence, end, commit, xid)
        };
        tx.commit()
            .await
            .context("worker claim COMMIT requires authoritative reload on failure")?;
        Ok(Writer {
            catalog: self.clone(),
            boundary: super::Durable {
                stored: super::Stored { epoch, checkpoint },
                binding,
                fence,
                end,
                commit: last_commit,
                xid: last_xid,
            },
            uncertain: false,
        })
    }
}
impl Writer {
    pub(super) async fn commit(&mut self, tx: Transaction<'_>, operation: &str) -> Result<()> {
        self.uncertain = true;
        tx.commit().await.with_context(|| {
            format!("{operation} COMMIT requires authoritative reload on failure")
        })?;
        self.uncertain = false;
        Ok(())
    }
    /// Last confirmed durable source end position; never advances on failed COMMIT.
    #[must_use]
    pub const fn end(&self) -> Lsn {
        self.boundary.end
    }
    /// Current durable arrangement publication epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.boundary.stored.epoch
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
        self.publish_timed(sql, plan, publication, false).await.map(|_| ())
    }
    pub(crate) async fn publish_timed(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        publication: Publication<'_>,
        measured: bool,
    ) -> Result<std::collections::BTreeMap<&'static str, u128>> {
        let mut clock = crate::timing::Clock::new(measured);
        ensure!(!self.uncertain, "uncertain COMMIT must be reconciled before publication");
        let Publication { checkpoint, progress, deltas } = publication;
        checkpoint.validate(plan)?;
        ensure!(
            self.boundary.stored.checkpoint.time.checked_add(1) == Some(checkpoint.time),
            "publication logical tick mismatch"
        );
        ensure!(
            progress.commit >= self.boundary.end,
            "publication commit precedes durable source end"
        );
        ensure!(progress.end > self.boundary.end, "publication source end is stale");
        ensure!(progress.end >= progress.commit, "publication source end precedes commit");
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        self.boundary.binding.source.lock_and_verify_layout(&tx).await?;
        let epoch =
            write::stage(&tx, &self.catalog, plan, checkpoint, self.boundary.stored.epoch).await?;
        let affected = tx.execute(&format!("UPDATE {}.pgderive_progress SET commit_lsn=$3::text::pg_lsn,end_lsn=$4::text::pg_lsn,xid=$5 WHERE query_id=$1 AND fence=$2 AND end_lsn=$6::text::pg_lsn AND binding=$7", self.catalog.schema),
            &[&self.catalog.query, &self.boundary.fence, &progress.commit.to_string(), &progress.end.to_string(), &i64::from(progress.xid), &self.boundary.end.to_string(), &serde_json::to_value(&self.boundary.binding)?]).await?;
        ensure!(affected == 1, "stale or incompatible publication worker");
        let membership = clock.lap();
        deltas.apply(&tx, &self.catalog, &self.boundary.binding.sink).await?;
        let sink = clock.lap();
        self.commit(tx, "publication").await?;
        let commit = clock.lap();
        self.boundary.stored.epoch = epoch;
        self.boundary.end = progress.end;
        self.boundary.stored.checkpoint = checkpoint.clone();
        self.boundary.commit = progress.commit;
        self.boundary.xid = Some(progress.xid);
        Ok(if measured {
            [("membership_layout_progress", membership), ("sink_dml", sink), ("commit", commit)]
                .into()
        } else {
            std::collections::BTreeMap::new()
        })
    }
}
