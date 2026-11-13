use super::{Writer, write};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Result, ensure};

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
        self.metadata.refresh(plan, &self.boundary)?;
        let (tx, program) = sql.publication(&self.catalog, &self.boundary.binding.sink).await?;
        let locked = self.lock_boundary(&tx, &program.maintenance).await?;
        let epoch = write::stage(&locked, checkpoint).await?;
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
