//! Owned immutable checkpoint uploads with append-only physical rebasing.
use super::{Compaction, Entry, Execution, Relational, RelationalState};
use crate::{
    catalog::Protection,
    engine::{
        plan::{Checkpoint, checkpoint::DurableState},
        reader::{BatchData, BlockCache},
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt as _, stream};
use std::sync::Arc;

/// Captured immutable state and upload namespace. Keep its protection active.
pub struct FrozenFlush<K: BatchData, V: BatchData> {
    captured: Arc<RelationalState<K, V>>,
    execution: Arc<Execution<K, V>>,
    incarnation: Arc<()>,
    namespace: String,
    time: u64,
}
/// Completed object uploads for an exact captured logical boundary.
pub struct UploadedFlush<K: BatchData, V: BatchData> {
    frozen: FrozenFlush<K, V>,
    state: RelationalState<K, V>,
    checkpoint: Checkpoint,
}
impl<K: BatchData, V: BatchData> UploadedFlush<K, V> {
    /// Uploaded membership at the captured time, independent of newer publications.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}
impl<K: BatchData, V: BatchData> FrozenFlush<K, V> {
    /// Upload this snapshot while the originating query continues processing.
    /// All sibling uploads finish before an error returns; retain protection on error.
    ///
    /// # Errors
    /// Returns upload, codec, resource or checkpoint validation failures.
    pub async fn upload(self) -> Result<UploadedFlush<K, V>> {
        let execution = self.execution.clone();
        let namespace = self.namespace.clone();
        let output_cache = BlockCache::output(execution.cache.clone());
        let shared_output = output_cache.clone();
        #[expect(
            clippy::needless_collect,
            reason = "owned IDs keep spawned upload futures independent of a borrowed map iterator"
        )]
        let ids = self.captured.entries.keys().cloned().collect::<Vec<_>>();
        let captured = self.captured.clone();
        let uploads = ids.into_iter().map(move |id| {
            let execution = execution.clone();
            let output_cache = shared_output.clone();
            let namespace = namespace.clone();
            let captured = captured.clone();
            async move {
                let _workspace = execution.workspaces.background().await?;
                tokio::task::consume_budget().await;
                let entry = captured.entries.get(&id).context("missing captured flush entry")?;
                let writer = execution
                    .writers
                    .get(&id)
                    .context("missing flush writer")?
                    .clone()
                    .with_namespace(&namespace)
                    .with_cache(output_cache);
                let entry = Entry {
                    schema: entry.schema.clone(),
                    trace: writer.flush(&entry.trace).await?,
                };
                Ok::<_, anyhow::Error>((id, entry))
            }
        });
        let completed = stream::iter(uploads)
            .buffer_unordered(self.execution.workspaces.capacity())
            .collect::<Vec<_>>()
            .await;
        let mut state = RelationalState { entries: completed.into_iter().collect::<Result<_>>()? };
        let checkpoint = Checkpoint::new(&self.execution.plan, self.time, state.memberships()?)?;
        output_cache.promote()?;
        for entry in state.entries.values_mut() {
            entry.trace.share_cache(&self.execution.cache);
        }
        Ok(UploadedFlush { frozen: self, state, checkpoint })
    }
}
impl<K: BatchData, V: BatchData> Relational<K, V> {
    /// Pin an immutable checkpoint and the exact upload protection namespace.
    ///
    /// # Errors
    /// Rejects a protection that does not authorize uploads.
    pub async fn freeze_flush(&mut self, protection: &Protection) -> Result<FrozenFlush<K, V>> {
        self.capture_flush(protection.namespace()?.to_owned()).await
    }
    pub(crate) async fn capture_flush(&mut self, namespace: String) -> Result<FrozenFlush<K, V>> {
        let sealed = self
            .engine
            .prepare_maintenance(|state| async move {
                let entries = state
                    .entries
                    .iter()
                    .map(|(id, entry)| {
                        (
                            id.clone(),
                            Entry {
                                schema: entry.schema.clone(),
                                trace: entry.trace.seal_memory(),
                            },
                        )
                    })
                    .collect();
                Ok(RelationalState { entries })
            })
            .await?;
        // Renew runtime ownership before capturing. A candidate prepared before
        // the seal could otherwise merge across this newly frozen prefix.
        self.engine.commit_maintenance(sealed)?;
        Ok(FrozenFlush {
            captured: self.snapshot(),
            execution: self.execution.clone(),
            incarnation: self.incarnation.clone(),
            namespace,
            time: self.time(),
        })
    }
    /// Prepare installation of uploaded prefix objects, retaining every newer delta.
    /// Publish the captured checkpoint before committing this local candidate.
    ///
    /// # Errors
    /// Rejects another runtime, recovery incarnation, altered prefix or invalid state.
    pub async fn prepare_uploaded_flush(
        &self,
        uploaded: &UploadedFlush<K, V>,
    ) -> Result<Compaction<K, V>> {
        ensure!(
            Arc::ptr_eq(&self.incarnation, &uploaded.frozen.incarnation),
            "foreign checkpoint upload"
        );
        let _workspace = self.execution.workspaces.acquire().await?;
        let current = self.snapshot();
        let entries = current
            .entries
            .iter()
            .map(|(id, entry)| {
                let base = uploaded
                    .frozen
                    .captured
                    .entries
                    .get(id)
                    .context("missing captured arrangement")?;
                let replacement =
                    uploaded.state.entries.get(id).context("missing uploaded arrangement")?;
                ensure!(
                    entry.schema == base.schema && entry.schema == replacement.schema,
                    "checkpoint schema changed"
                );
                Ok((
                    id.clone(),
                    Entry {
                        schema: entry.schema.clone(),
                        trace: entry.trace.rebase_flushed(&base.trace, &replacement.trace)?,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        self.engine.prepare_maintenance(|_| async move { Ok(RelationalState { entries }) }).await
    }
}

#[cfg(test)]
#[path = "flush_tests.rs"]
mod tests;
