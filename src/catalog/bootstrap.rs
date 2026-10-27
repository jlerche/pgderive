use super::{Binding, Catalog, Deltas, Lsn, Sink, write};
use crate::{
    engine::plan::{Checkpoint, Plan},
    source::Contract,
};
use anyhow::{Context, Result, ensure};

/// Initial immutable state and destination output for one slot-consistent snapshot.
/// No synthetic source transaction ID is assigned to snapshot rows.
#[derive(Clone)]
pub struct Snapshot<'a> {
    /// Candidate logical tick one, prepared from the complete imported snapshot.
    pub checkpoint: &'a Checkpoint,
    /// Initial destination deltas produced by the same preparation.
    pub deltas: &'a Deltas,
    /// Frozen source contract inspected inside the imported snapshot.
    pub source: &'a Contract,
    /// Exact `CREATE_REPLICATION_SLOT` consistent point.
    pub boundary: Lsn,
    /// Empty owned destination to initialize atomically.
    pub sink: Sink,
}
impl Catalog {
    /// Activate a complete initial snapshot with one synchronous durable transaction.
    /// Objects must already exist. Until this commits there is no source capability
    /// and no query membership, destination data or source progress is visible.
    ///
    /// # Errors
    /// Rejects existing registrations, source drift, nonempty sinks or SQL failures.
    /// After an uncertain COMMIT, load the exact authoritative boundary before retry.
    pub async fn activate_snapshot(
        &self,
        sql: &mut crate::catalog::Connection,
        plan: &Plan,
        snapshot: Snapshot<'_>,
    ) -> Result<()> {
        snapshot.checkpoint.validate(plan)?;
        ensure!(snapshot.checkpoint.time == 1, "snapshot activation requires logical tick one");
        let binding = Binding { source: snapshot.source.clone(), sink: snapshot.sink };
        binding.validate()?;
        let tx = sql.transaction().await?;
        let protected = super::storage::publication_lock(&tx, self).await?;
        snapshot.source.lock_and_verify_layout(&tx).await?;
        let source_key = snapshot.source.ownership_key()?;
        tx.execute(
            &format!(
                "INSERT INTO {}.pgderive_source_slots(source_key,query_id) VALUES($1,$2)",
                self.schema
            ),
            &[&source_key, &self.query],
        )
        .await?;
        write::create(&protected, plan, snapshot.checkpoint).await?;
        binding.initialize(&tx, self, snapshot.boundary).await?;
        let program = super::sink::program::Program::prepare(
            &tx,
            &format!("{}.{}", self.schema, binding.sink.table),
            &binding.sink,
        )
        .await?;
        snapshot
            .deltas
            .apply(&tx, self, (snapshot.checkpoint.time, snapshot.boundary), &program)
            .await?;
        tx.commit()
            .await
            .context("snapshot activation COMMIT requires authoritative reload on failure")?;
        Ok(())
    }
}
