//! Reuse exact boundary validation only while its transaction holds the locks.
use super::{Writer, metadata::Jsonb};
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Statement, Transaction};

pub(super) struct LockedBoundary<'a, 'tx> {
    protection: super::storage::Publication<'a, 'tx>,
    boundary: &'a super::Durable,
}
impl<'a, 'tx> LockedBoundary<'a, 'tx> {
    pub(super) const fn transaction(&self) -> &'a Transaction<'tx> {
        self.protection.transaction()
    }
    pub(super) const fn catalog(&self) -> &'a super::Catalog {
        self.protection.catalog()
    }
    pub(super) const fn boundary(&self) -> &'a super::Durable {
        self.boundary
    }
    pub(super) const fn epoch(&self) -> u64 {
        self.boundary.stored.epoch
    }
}
impl Writer {
    pub(super) async fn lock_boundary<'a, 'tx>(
        &'a self,
        tx: &'a Transaction<'tx>,
        statement: &Statement,
    ) -> Result<LockedBoundary<'a, 'tx>> {
        // First polling queues lifecycle protection before the separate guard
        // statement, retaining a fresh snapshot after any lifecycle wait.
        let (protection, ()) = tokio::try_join!(
            biased;
            super::storage::publication_lock(tx, &self.catalog),
            self.guard_boundary(tx, statement),
        )?;
        Ok(LockedBoundary { protection, boundary: &self.boundary })
    }

    async fn guard_boundary(&self, tx: &Transaction<'_>, statement: &Statement) -> Result<()> {
        let boundary = &self.boundary;
        let expected_progress = Jsonb::new(&(
            boundary.binding.sink.table.as_str(),
            boundary.fence,
            boundary.commit.to_string(),
            boundary.end.to_string(),
            boundary.xid,
            boundary.checkpoint_end.to_string(),
            boundary.published_time,
        ))?;
        // The progress CTE depends on the locked query CTE: keep the same query ->
        // progress lock order as claims, checkpoints and recovery. Compare every
        // field used by read::durable, including JSON bodies and their digest.
        // Compare canonical wire bytes: JSONB numeric equality accepts 1.0 as 1,
        // whereas integer metadata codecs reject the floating representation.
        let row = tx
            .query_opt(
                statement,
                &[
                    &self.catalog.query,
                    &self.metadata.query,
                    &self.metadata.binding,
                    &expected_progress,
                    &self.metadata.source_key,
                ],
            )
            .await?
            .context("memtable query or progress disappeared")?;
        ensure!(row.try_get::<_, bool>(0)?, "memtable worker fenced or durable boundary changed");
        Ok(())
    }
}

pub(super) fn statement(schema: &str, marker: &str) -> String {
    format!("{marker} WITH q AS MATERIALIZED (
                SELECT * FROM {schema}.pgderive_queries WHERE query_id=$1 FOR UPDATE
            ), p AS MATERIALIZED (
                SELECT p.* FROM {schema}.pgderive_progress p CROSS JOIN q
                WHERE p.query_id=q.query_id FOR UPDATE OF p
            ) SELECT q.format_version=3
                AND pg_catalog.jsonb_send(jsonb_build_array(q.plan_identity,q.definition,q.logical_time,q.epoch,q.checkpoint,q.checkpoint_digest))=pg_catalog.jsonb_send($2)
                AND pg_catalog.jsonb_send(p.binding)=pg_catalog.jsonb_send($3)
                AND jsonb_build_array(p.sink_table,p.fence,p.commit_lsn::text,p.end_lsn::text,p.xid,p.checkpoint_lsn::text,p.published_time)=$4
                AND EXISTS(SELECT 1 FROM {schema}.pgderive_source_slots s
                    WHERE s.source_key=$5 AND s.query_id=$1 FOR SHARE OF s)
            FROM q CROSS JOIN p")
}
