//! Sink publication backed by retained source WAL rather than a new object root.
use super::{Deltas, Progress, Writer, read, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, Transaction};

impl Writer {
    pub(crate) async fn publish_memtable(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        publication: (u64, &Progress, &Deltas),
        measured: bool,
    ) -> Result<std::collections::BTreeMap<&'static str, u128>> {
        let mut clock = crate::timing::Clock::new(measured);
        let (time, progress, deltas) = publication;
        ensure!(!self.uncertain, "uncertain publication requires recovery");
        ensure!(
            self.boundary.published_time.checked_add(1) == Some(time),
            "memtable publication clock mismatch"
        );
        ensure!(!self.boundary.covers(progress)?, "memtable transaction already published");
        let tx = sql.transaction().await?;
        self.lock_memtable(&tx, plan, false).await?;
        let boundary_guard = clock.lap();
        self.boundary.binding.source.lock_and_verify_layout(&tx).await?;
        let source_layout = clock.lap();
        tx.execute(
            &format!("UPDATE {}.pgderive_progress SET commit_lsn=$3::text::pg_lsn,end_lsn=$4::text::pg_lsn,xid=$5,published_time=$2 WHERE query_id=$1", self.catalog.schema),
            &[&self.catalog.query, &i64::try_from(time)?, &progress.commit.to_string(), &progress.end.to_string(), &i64::from(progress.xid)],
        ).await?;
        let progress_time = clock.lap();
        let membership = boundary_guard + source_layout + progress_time;
        deltas.apply(&tx, &self.catalog, &self.boundary.binding.sink, (time, progress.end)).await?;
        let sink = clock.lap();
        self.commit(tx, "memtable publication").await?;
        let commit = clock.lap();
        self.boundary.published_time = time;
        self.boundary.commit = progress.commit;
        self.boundary.end = progress.end;
        self.boundary.xid = Some(progress.xid);
        Ok(if measured {
            [
                ("membership_layout_progress", membership),
                ("boundary_guard", boundary_guard),
                ("source_layout", source_layout),
                ("progress", progress_time),
                ("sink_dml", sink),
                ("commit", commit),
            ]
            .into()
        } else {
            std::collections::BTreeMap::new()
        })
    }
    pub(crate) async fn checkpoint_memtable(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        candidate: &Checkpoint,
    ) -> Result<()> {
        ensure!(!self.uncertain, "uncertain checkpoint requires recovery");
        candidate.validate(plan)?;
        ensure!(
            candidate.time == self.boundary.published_time,
            "checkpoint does not cover published memtable"
        );
        let tx = sql.transaction().await?;
        self.lock_memtable(&tx, plan, true).await?;
        let epoch =
            write::stage(&tx, &self.catalog, plan, candidate, self.boundary.stored.epoch).await?;
        tx.execute(
            &format!(
                "UPDATE {}.pgderive_progress SET checkpoint_lsn=end_lsn WHERE query_id=$1",
                self.catalog.schema
            ),
            &[&self.catalog.query],
        )
        .await?;
        self.commit(tx, "memtable checkpoint").await?;
        self.boundary.stored.checkpoint = candidate.clone();
        self.boundary.stored.epoch = epoch;
        self.boundary.checkpoint_end = self.boundary.end;
        Ok(())
    }
    async fn lock_memtable(
        &self,
        tx: &Transaction<'_>,
        plan: &Plan,
        checkpoint: bool,
    ) -> Result<()> {
        super::storage::publication_lock(tx, &self.catalog).await?;
        if !checkpoint {
            return self.guard_memtable(tx, plan).await;
        }
        let marker = if checkpoint { "/* pgderive:maintenance */" } else { "" };
        tx.query_one(
            &format!(
                "SELECT {marker} query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE",
                self.catalog.schema
            ),
            &[&self.catalog.query],
        )
        .await?;
        let prior = read::durable(tx, &self.catalog, plan, true)
            .await?
            .context("memtable query disappeared")?;
        ensure!(prior == self.boundary, "memtable worker fenced or durable boundary changed");
        Ok(())
    }
}
