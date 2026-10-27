//! Compare the complete expected boundary inside `PostgreSQL` without decoding it again.
use super::{Writer, checkpoint_digest};
use crate::engine::plan::Plan;
use anyhow::{Context, Result, ensure};
use serde_json::json;
use tokio_postgres::{Statement, Transaction};

impl Writer {
    pub(super) async fn guard_memtable(
        &self,
        tx: &Transaction<'_>,
        plan: &Plan,
        statement: &Statement,
    ) -> Result<()> {
        let boundary = &self.boundary;
        let checkpoint = &boundary.stored.checkpoint;
        let expected_query = json!([
            plan.identity(),
            plan.definition(),
            checkpoint.time,
            boundary.stored.epoch,
            checkpoint,
            checkpoint_digest(checkpoint)?,
        ]);
        let expected_progress = json!([
            boundary.binding,
            boundary.binding.sink.table.as_str(),
            boundary.fence,
            boundary.commit.to_string(),
            boundary.end.to_string(),
            boundary.xid,
            boundary.checkpoint_end.to_string(),
            boundary.published_time,
        ]);
        // The progress CTE depends on the locked query CTE: keep the same query ->
        // progress lock order as claims, checkpoints and recovery. Compare every
        // field used by read::durable, including JSON bodies and their digest.
        let row = tx
            .query_opt(
                statement,
                &[
                    &self.catalog.query,
                    &expected_query,
                    &expected_progress,
                    &boundary.binding.source.ownership_key()?,
                ],
            )
            .await?
            .context("memtable query or progress disappeared")?;
        ensure!(row.try_get::<_, bool>(0)?, "memtable worker fenced or durable boundary changed");
        Ok(())
    }
}

pub(super) fn statement(schema: &str) -> String {
    format!("/* pgderive:boundary */ WITH q AS MATERIALIZED (
                SELECT * FROM {schema}.pgderive_queries WHERE query_id=$1 FOR UPDATE
            ), p AS MATERIALIZED (
                SELECT p.* FROM {schema}.pgderive_progress p CROSS JOIN q
                WHERE p.query_id=q.query_id FOR UPDATE OF p
            ) SELECT q.format_version=3
                AND jsonb_build_array(q.plan_identity,q.definition,q.logical_time,q.epoch,q.checkpoint,q.checkpoint_digest)=$2
                AND jsonb_build_array(p.binding,p.sink_table,p.fence,p.commit_lsn::text,p.end_lsn::text,p.xid,p.checkpoint_lsn::text,p.published_time)=$3
                AND EXISTS(SELECT 1 FROM {schema}.pgderive_source_slots WHERE source_key=$4 AND query_id=$1)
            FROM q CROSS JOIN p")
}
