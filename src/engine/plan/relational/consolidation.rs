//! Owned consolidation of an immutable durable prefix, independent of ingestion.
use super::{Compaction, Entry, Execution, Relational, RelationalState};
use crate::engine::{
    plan::{Checkpoint, checkpoint::DurableState},
    reader::{BatchData, BlockCache},
};
use anyhow::{Context, Result, ensure};
use std::sync::Arc;

pub struct FrozenConsolidation<K: BatchData, V: BatchData> {
    captured: RelationalState<K, V>,
    execution: Arc<Execution<K, V>>,
    incarnation: Arc<()>,
    namespace: String,
}
pub struct UploadedConsolidation<K: BatchData, V: BatchData> {
    frozen: FrozenConsolidation<K, V>,
    state: RelationalState<K, V>,
}
impl<K: BatchData, V: BatchData> FrozenConsolidation<K, V> {
    pub(crate) async fn upload(self) -> Result<UploadedConsolidation<K, V>> {
        let cache = BlockCache::output(self.execution.cache.clone());
        let mut state = self.captured.clone();
        // One streaming arrangement at a time: consolidation must not multiply
        // scan workspaces by graph width while urgent checkpoint uploads run.
        for (id, entry) in &mut state.entries {
            let writer = self
                .execution
                .writers
                .get(id)
                .context("missing consolidation writer")?
                .clone()
                .with_namespace(&self.namespace)
                .with_cache(cache.clone());
            entry.trace = writer.consolidate(&entry.trace).await?;
        }
        cache.promote()?;
        for entry in state.entries.values_mut() {
            entry.trace.share_cache(&self.execution.cache);
        }
        Ok(UploadedConsolidation { frozen: self, state })
    }
}
impl<K: BatchData, V: BatchData> Relational<K, V> {
    pub(crate) fn consolidation_due(&self, checkpoint: &Checkpoint) -> Result<bool> {
        checkpoint.validate(self.plan())?;
        let state = self.snapshot();
        for member in &checkpoint.arrangements {
            let entry =
                state.entries.get(&member.id).context("missing consolidation arrangement")?;
            if entry.trace.durable_prefix(&member.trace)?.consolidation_due()? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn capture_consolidation(
        &self,
        checkpoint: &Checkpoint,
        namespace: String,
    ) -> Result<FrozenConsolidation<K, V>> {
        checkpoint.validate(self.plan())?;
        let current = self.snapshot();
        let entries = checkpoint
            .arrangements
            .iter()
            .map(|member| {
                let entry =
                    current.entries.get(&member.id).context("missing consolidation arrangement")?;
                Ok((
                    member.id.clone(),
                    Entry {
                        schema: member.schema.clone(),
                        trace: entry.trace.durable_prefix(&member.trace)?,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        Ok(FrozenConsolidation {
            captured: RelationalState { entries },
            execution: self.execution.clone(),
            incarnation: self.incarnation.clone(),
            namespace,
        })
    }
    pub(crate) async fn prepare_uploaded_consolidation(
        &self,
        uploaded: &UploadedConsolidation<K, V>,
        checkpoint: &Checkpoint,
    ) -> Result<(Compaction<K, V>, Checkpoint)> {
        ensure!(
            Arc::ptr_eq(&self.incarnation, &uploaded.frozen.incarnation),
            "foreign consolidation"
        );
        checkpoint.validate(self.plan())?;
        let current = self.snapshot();
        let mut entries = current.entries.clone();
        let mut durable = current.entries.clone();
        for member in &checkpoint.arrangements {
            let entry = current.entries.get(&member.id).context("missing current arrangement")?;
            let captured = uploaded
                .frozen
                .captured
                .entries
                .get(&member.id)
                .context("missing captured consolidation")?;
            let replacement =
                uploaded.state.entries.get(&member.id).context("missing consolidation output")?;
            entries.insert(
                member.id.clone(),
                Entry {
                    schema: entry.schema.clone(),
                    trace: entry.trace.rebase_consolidated(&captured.trace, &replacement.trace)?,
                },
            );
            durable.insert(
                member.id.clone(),
                Entry {
                    schema: entry.schema.clone(),
                    trace: entry
                        .trace
                        .durable_prefix(&member.trace)?
                        .rebase_consolidated(&captured.trace, &replacement.trace)?,
                },
            );
        }
        let checkpoint = Checkpoint::new(
            self.plan(),
            checkpoint.time,
            RelationalState { entries: durable }.memberships()?,
        )?;
        let prepared = self
            .engine
            .prepare_maintenance(|_| async move { Ok(RelationalState { entries }) })
            .await?;
        Ok((prepared, checkpoint))
    }
}

#[cfg(test)]
mod tests;
