use super::{Compaction, Entry, Prepared, Relational, RelationalState};
use crate::{
    catalog::{Protection, Writer},
    engine::{
        plan::{Checkpoint, Membership},
        reader::BatchData,
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt as _, stream};
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
    /// Reopen an authoritative object checkpoint.
    ///
    /// # Errors
    /// Rejects incompatible registrations or missing/corrupt objects.
    pub async fn reopen(
        plan: super::super::Plan,
        operators: super::Operators<K, V>,
        settings: super::super::Settings,
        checkpoint: Checkpoint,
    ) -> Result<Self> {
        let mut query = Self::new(plan, operators, &settings)?;
        query.restore_checkpoint(checkpoint).await?;
        Ok(query)
    }
    /// Atomically publish a prepared full-tuple bag before changing local visibility.
    ///
    /// # Errors
    /// Rejects foreign work, invalid codecs, fencing or publication failures.
    pub async fn publish_prepared(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        work: Prepared<K, V>,
        progress: &crate::catalog::Progress,
    ) -> Result<crate::engine::dataflow::TimedBatch<crate::engine::Batch<K, V>>> {
        let checkpoint = self.prepared_checkpoint(&work)?;
        let deltas = crate::catalog::Deltas::bag(&work.output().batch)?;
        writer
            .publish(
                sql,
                self.plan(),
                crate::catalog::Publication { checkpoint: &checkpoint, progress, deltas: &deltas },
            )
            .await?;
        self.commit(work)
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
        self.prepare_compaction_scoped(Some(protection.namespace()?)).await
    }
    /// Prepare equivalent local physical state.
    ///
    /// # Errors
    /// Returns storage, resource or equivalence failures.
    pub async fn prepare_compaction(&self) -> Result<Compaction<K, V>> {
        self.prepare_compaction_scoped(None).await
    }
    async fn prepare_compaction_scoped(&self, namespace: Option<&str>) -> Result<Compaction<K, V>> {
        let namespace = namespace.map(str::to_owned);
        let writers = self.execution.writers.clone();
        self.engine
            .prepare_maintenance(move |state| async move {
                let uploads = state.entries.iter().map(|(id, entry)| {
                    let writers = &writers;
                    let namespace = namespace.as_deref();
                    async move {
                        let writer = writers.get(id).context("missing compaction writer")?.clone();
                        let writer = namespace.map_or_else(
                            || writer.clone(),
                            |scope| writer.clone().with_namespace(scope),
                        );
                        Ok::<_, anyhow::Error>((id.clone(), compact_entry(entry, &writer).await?))
                    }
                });
                // Drain all uploads before returning any error; protection covers all of them.
                let completed = stream::iter(uploads).buffer_unordered(8).collect::<Vec<_>>().await;
                let entries = completed.into_iter().collect::<Result<_>>()?;
                Ok(RelationalState { entries })
            })
            .await
    }
    /// Install equivalent local physical state.
    ///
    /// # Errors
    /// Rejects stale or foreign candidates.
    pub fn commit_compaction(&mut self, work: Compaction<K, V>) -> Result<()> {
        self.engine.commit_maintenance(work)
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
    if entry.trace.run_count() <= 1 && !entry.trace.has_memory() {
        return Ok(entry.clone());
    }
    Ok(Entry { schema: entry.schema.clone(), trace: writer.compact(&entry.trace).await? })
}
#[cfg(test)]
#[path = "compaction_tests.rs"]
mod tests;
