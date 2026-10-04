use super::{Writer, read, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

impl Writer {
    /// Publish equivalent physical memberships without changing source or sink state.
    /// The caller must prove weighted equivalence against the exact prior checkpoint.
    /// Immutable output objects must already exist; local visibility follows COMMIT.
    ///
    /// # Errors
    /// Rejects changed clocks/schemas, stale ownership or membership, and SQL failures.
    /// An uncertain COMMIT blocks further publication/ACK until authoritative recovery.
    pub async fn maintain(
        &mut self,
        sql: &mut Client,
        plan: &Plan,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        ensure!(!self.uncertain, "uncertain COMMIT must be reconciled before maintenance");
        validate(&self.checkpoint, checkpoint, plan)?;
        let tx = sql.transaction().await?;
        super::storage::shared(&tx, &self.catalog).await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        tx.query_one(
            &format!(
                "SELECT query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE",
                self.catalog.schema
            ),
            &[&self.catalog.query],
        )
        .await?;
        let prior = read::boundary(&tx, &self.catalog, plan)
            .await?
            .context("maintenance query disappeared")?;
        ensure!(
            prior.epoch == self.epoch && prior.checkpoint == self.checkpoint,
            "maintenance prior membership changed"
        );
        let row = tx.query_one(&format!("SELECT binding,fence,commit_lsn::text,end_lsn::text,xid FROM {}.pgderive_progress WHERE query_id=$1 FOR UPDATE",self.catalog.schema), &[&self.catalog.query]).await?;
        ensure!(
            row.try_get::<_, serde_json::Value>(0)? == serde_json::to_value(&self.binding)?
                && row.try_get::<_, i64>(1)? == self.fence
                && row.try_get::<_, String>(2)? == self.last_commit.to_string()
                && row.try_get::<_, String>(3)? == self.end.to_string()
                && row.try_get::<_, Option<i64>>(4)? == self.last_xid.map(i64::from),
            "maintenance ownership/source boundary changed"
        );
        self.binding.verify_ownership(&tx, &self.catalog).await?;
        let epoch = write::stage(&tx, &self.catalog, plan, checkpoint, self.epoch).await?;
        self.uncertain = true;
        tx.commit()
            .await
            .context("maintenance COMMIT requires authoritative resolution on failure")?;
        self.epoch = epoch;
        self.checkpoint = checkpoint.clone();
        self.uncertain = false;
        Ok(())
    }
}
pub(super) fn validate(prior: &Checkpoint, candidate: &Checkpoint, plan: &Plan) -> Result<()> {
    candidate.validate(plan)?;
    ensure!(candidate.time == prior.time, "maintenance changed logical time");
    ensure!(
        candidate.arrangements.len() == prior.arrangements.len(),
        "maintenance arrangement set changed"
    );
    for (before, after) in prior.arrangements.iter().zip(&candidate.arrangements) {
        ensure!(
            before.id == after.id
                && before.schema == after.schema
                && before.trace.time == after.trace.time
                && before.trace.generation.checked_add(1) == Some(after.trace.generation),
            "maintenance changed identity/clock or skipped physical generation"
        );
    }
    Ok(())
}
