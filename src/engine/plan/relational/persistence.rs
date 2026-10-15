use super::{Compaction, Entry, Prepared, Relational, RelationalState};
use crate::{
    catalog::{Protection, Writer},
    engine::{
        plan::{Checkpoint, Membership},
        reader::BatchData,
    },
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;
impl<K: BatchData, V: BatchData> super::super::checkpoint::DurableState for RelationalState<K, V> {
    fn memberships(&self) -> Result<Vec<Membership>> {
        self.entries
            .iter()
            .map(|(id, entry)| {
                Ok(Membership {
                    id: id.clone(),
                    schema: entry.schema.clone(),
                    trace: entry.trace.manifest()?,
                })
            })
            .collect()
    }
}
impl<K: BatchData, V: BatchData> Relational<K, V> {
    /// Export complete memberships without publishing progress or acknowledging CDC.
    ///
    /// # Errors
    /// Returns invalid membership or schema errors.
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        self.engine.checkpoint()
    }
    /// Export unpublished membership after verifying exact candidate ownership.
    ///
    /// # Errors
    /// Rejects foreign/stale candidates and invalid memberships.
    pub fn prepared_checkpoint(&self, work: &Prepared<K, V>) -> Result<Checkpoint> {
        self.engine.prepared_checkpoint(work)
    }
    /// Cold-validate and adopt authoritative membership, invalidating prior work.
    ///
    /// # Errors
    /// Rejects incompatible/older checkpoints, missing/corrupt objects or bad codecs.
    pub async fn restore_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        checkpoint.validate(self.plan())?;
        ensure!(checkpoint.time >= self.time(), "recovery cannot roll back logical time");
        let mut entries = std::collections::BTreeMap::new();
        for member in &checkpoint.arrangements {
            let writer = self.execution.writers.get(&member.id).context("unknown arrangement")?;
            entries.insert(
                member.id.clone(),
                Entry {
                    schema: member.schema.clone(),
                    trace: writer.reopen(member.trace.clone()).await?,
                },
            );
        }
        let restored =
            Self::bind(self.execution.clone(), RelationalState { entries }, checkpoint.time)?;
        self.engine = restored.engine;
        Ok(())
    }
    /// Stage equivalent physical state under an upload protection at the same tick.
    ///
    /// # Errors
    /// Returns reservation, storage, resource or equivalence errors.
    pub async fn prepare_compaction_protected(
        &self,
        protection: &Protection,
    ) -> Result<Compaction<K, V>> {
        let namespace = protection.namespace()?.to_owned();
        let writers = self.execution.writers.clone();
        self.engine
            .maintenance(move |state| async move {
                let mut entries = std::collections::BTreeMap::new();
                for (id, entry) in &state.entries {
                    let writer = writers
                        .get(id)
                        .context("missing compaction writer")?
                        .clone()
                        .with_namespace(&namespace);
                    entries.insert(id.clone(), compact_entry(entry, &writer).await?);
                }
                Ok(RelationalState { entries })
            })
            .await
    }
    /// Export an owned physical candidate without advancing logical time.
    ///
    /// # Errors
    /// Rejects foreign/stale maintenance and incompatible metadata.
    pub fn prepared_compaction_checkpoint(&self, work: &Compaction<K, V>) -> Result<Checkpoint> {
        self.engine.prepared_compaction_checkpoint(work)
    }
    /// Publish equivalent membership before moving local visibility.
    ///
    /// # Errors
    /// Returns stale/foreign, fencing or PG errors; uncertain COMMIT requires recovery.
    pub async fn publish_compaction(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        work: Compaction<K, V>,
    ) -> Result<()> {
        self.engine.publish_compaction(sql, writer, work).await
    }
}

async fn compact_entry<K: BatchData, V: BatchData>(
    entry: &Entry<K, V>,
    writer: &crate::engine::dataflow::Arrangement<K, V>,
) -> Result<Entry<K, V>> {
    if entry.trace.run_count() <= 1 {
        return Ok(entry.clone());
    }
    Ok(Entry { schema: entry.schema.clone(), trace: writer.compact(&entry.trace).await? })
}
#[cfg(test)]
#[path = "compaction_tests.rs"]
mod tests;
