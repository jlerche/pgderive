use super::{Writer, read, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};

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
        sql: &mut crate::catalog::Connection,
        plan: &Plan,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        ensure!(!self.uncertain, "uncertain COMMIT must be reconciled before maintenance");
        validate(&self.boundary.stored.checkpoint, checkpoint, plan)?;
        let tx = sql.transaction().await?;
        super::storage::shared(&tx, &self.catalog).await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        // Stable operation marker lets crash probes hold maintenance, not claim/ACK COMMITs.
        tx.query_one(
            &format!(
                "SELECT /* pgderive:maintenance */ query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR UPDATE",
                self.catalog.schema
            ),
            &[&self.catalog.query],
        )
        .await?;
        let prior = read::durable(&tx, &self.catalog, plan, true)
            .await?
            .context("maintenance query disappeared")?;
        ensure!(prior == self.boundary, "maintenance durable boundary changed");
        let epoch =
            write::stage(&tx, &self.catalog, plan, checkpoint, self.boundary.stored.epoch).await?;
        self.commit(tx, "maintenance").await?;
        self.boundary.stored.epoch = epoch;
        self.boundary.stored.checkpoint = checkpoint.clone();
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
                && (before.trace == after.trace
                    || before.trace.generation.checked_add(1) == Some(after.trace.generation)),
            "maintenance changed identity/clock or skipped physical generation"
        );
    }
    Ok(())
}
