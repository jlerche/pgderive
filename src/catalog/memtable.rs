//! Sink publication backed by retained source WAL rather than a new object root.
use super::{Deltas, Progress, Writer, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};

impl Writer {
    pub(crate) async fn publish_epoch(
        &mut self,
        sql: &mut crate::catalog::Connection,
        plan: &Plan,
        publication: (u64, &Progress, &Deltas, u64),
        measured: bool,
    ) -> Result<std::collections::BTreeMap<&'static str, u128>> {
        let mut clock = crate::timing::Clock::new(measured);
        let (time, progress, deltas, transactions) = publication;
        ensure!(!self.uncertain, "uncertain publication requires recovery");
        ensure!(
            self.boundary.published_time.checked_add(1) == Some(time),
            "memtable publication clock mismatch"
        );
        ensure!(!self.boundary.covers(progress)?, "memtable transaction already published");
        self.metadata.refresh(plan, &self.boundary)?;
        let (tx, program) = sql.publication(&self.catalog, &self.boundary.binding.sink).await?;
        let locked = self.lock_boundary(&tx, &program.guard).await?;
        let boundary_guard = clock.lap();
        self.boundary.binding.source.lock_and_verify_layout(&tx).await?;
        let source_layout = clock.lap();
        locked.record_epoch((time, progress, transactions), &program.epoch).await?;
        let progress_time = clock.lap();
        let membership = boundary_guard + source_layout + progress_time;
        deltas.apply(&tx, &self.catalog, (time, progress.end), &program.sink).await?;
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
        sql: &mut crate::catalog::Connection,
        plan: &Plan,
        candidate: &Checkpoint,
    ) -> Result<()> {
        ensure!(!self.uncertain, "uncertain checkpoint requires recovery");
        candidate.validate(plan)?;
        ensure!(
            candidate.time > self.boundary.stored.checkpoint.time
                && candidate.time <= self.boundary.published_time,
            "checkpoint is outside the published epoch suffix"
        );
        self.metadata.refresh(plan, &self.boundary)?;
        let (tx, program) = sql.publication(&self.catalog, &self.boundary.binding.sink).await?;
        let locked = self.lock_boundary(&tx, &program.maintenance).await?;
        let row = tx.query_one(
            &format!("SELECT end_lsn::text FROM {}.pgderive_epoch_boundaries WHERE query_id=$1 AND logical_time=$2", self.catalog.schema),
            &[&self.catalog.query, &i64::try_from(candidate.time)?],
        ).await.context("checkpoint has no captured epoch boundary")?;
        let checkpoint_end: super::Lsn = row.try_get::<_, String>(0)?.parse()?;
        ensure!(
            checkpoint_end > self.boundary.checkpoint_end && checkpoint_end <= self.boundary.end,
            "checkpoint source boundary is outside published progress"
        );
        let epoch = write::stage(&locked, candidate).await?;
        tx.execute(
            &format!(
                "UPDATE {}.pgderive_progress SET checkpoint_lsn=$2::text::pg_lsn WHERE query_id=$1",
                self.catalog.schema
            ),
            &[&self.catalog.query, &checkpoint_end.to_string()],
        )
        .await?;
        tx.execute(
            &format!(
                "DELETE FROM {}.pgderive_epoch_boundaries WHERE query_id=$1 AND logical_time<=$2",
                self.catalog.schema
            ),
            &[&self.catalog.query, &i64::try_from(candidate.time)?],
        )
        .await?;
        self.commit(tx, "memtable checkpoint").await?;
        self.boundary.stored.checkpoint = candidate.clone();
        self.boundary.stored.epoch = epoch;
        self.boundary.checkpoint_end = checkpoint_end;
        Ok(())
    }
}
