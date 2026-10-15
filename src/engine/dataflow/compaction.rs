use super::Arrangement;
use crate::engine::{
    reader::{BatchData, BlockCache},
    trace::{Manifest, TraceSnapshot},
};
use anyhow::{Result, ensure};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Serializable physical work, executable without a PG connection or writer ownership.
///
/// The coordinator must durably pin the input and reserve the upload namespace until
/// every executor has stopped. Jobs never publish membership or acknowledge source WAL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionJob {
    version: u32,
    schema: String,
    input: Manifest,
    namespace: String,
    block_rows: usize,
}
/// Unpublished replacement from a detached executor. Treat deserialized results as untrusted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionResult {
    job: CompactionJob,
    output: Manifest,
}
impl CompactionJob {
    /// Exact input membership that must remain pinned during detached execution.
    #[must_use]
    pub const fn input(&self) -> &Manifest {
        &self.input
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && !self.schema.is_empty(),
            "invalid compaction job version/schema"
        );
        ensure!((1..=65_536).contains(&self.block_rows), "invalid compaction block rows");
        ensure!(self.input.objects.len() <= 128, "compaction input run limit exceeded");
        ensure!(
            crate::engine::reader::valid_namespace(&self.namespace),
            "invalid compaction upload namespace"
        );
        self.input.validate(&self.schema)
    }
    /// Cold-read, consolidate and upload a replacement using the ordinary streaming compactor.
    /// Call this in another process with the same typed codec and object-store prefix.
    ///
    /// # Errors
    /// Rejects invalid jobs, missing/corrupt input, overflow or failed immutable PUTs.
    pub async fn execute<K: BatchData, V: BatchData>(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> Result<CompactionResult> {
        self.validate()?;
        let arrangement = Arrangement::<K, V>::new(store, self.schema.clone(), self.block_rows)?
            .with_namespace(&self.namespace);
        let input = arrangement.reopen(self.input.clone()).await?;
        let output = arrangement.compact(&input).await?.manifest()?;
        Ok(CompactionResult { job: self.clone(), output })
    }
}
impl<K: BatchData, V: BatchData> Arrangement<K, V> {
    /// Prepare portable work under an existing durable upload reservation.
    /// Separately pin `prior` in the durable catalog before handing work to another process.
    ///
    /// # Errors
    /// Rejects non-object input, invalid schemas, limits or upload reservations.
    pub fn compaction_job(
        &self,
        prior: &TraceSnapshot<K, V>,
        protection: &crate::catalog::Protection,
    ) -> Result<CompactionJob> {
        self.make_job(prior, protection.namespace()?)
    }
    pub(super) fn make_job(
        &self,
        prior: &TraceSnapshot<K, V>,
        namespace: &str,
    ) -> Result<CompactionJob> {
        let job = CompactionJob {
            version: 1,
            schema: self.schema.clone(),
            input: prior.manifest()?,
            namespace: namespace.into(),
            block_rows: self.block_rows.min(65_536),
        };
        job.validate()?;
        Ok(job)
    }
    /// Validate a detached result against exact current membership and weighted contents.
    /// Returns an unpublished snapshot; the caller still needs the fenced PG publication.
    /// Retain the input/upload protections through publication and executor quiescence.
    ///
    /// # Errors
    /// Rejects stale/foreign jobs, foreign output paths, corruption or changed tuple weights.
    pub async fn adopt_compaction(
        &self,
        prior: &TraceSnapshot<K, V>,
        job: &CompactionJob,
        result: CompactionResult,
    ) -> Result<TraceSnapshot<K, V>> {
        job.validate()?;
        ensure!(
            self.schema == job.schema && prior.manifest()? == job.input && *job == result.job,
            "stale or foreign detached compaction"
        );
        ensure!(
            result.output.time == job.input.time
                && job.input.generation.checked_add(1) == Some(result.output.generation)
                && result.output.objects.len() <= 128
                && result
                    .output
                    .objects
                    .iter()
                    .all(|object| object.namespace() == Some(job.namespace.as_str())),
            "invalid detached compaction output boundary"
        );
        let candidate = TraceSnapshot::reopen(
            self.store.clone(),
            result.output,
            &self.schema,
            Arc::new(BlockCache::new(0, 0)),
        )
        .await?;
        prior.replace_runs(candidate.runs).await
    }
}
#[cfg(test)]
#[path = "compaction_tests.rs"]
mod tests;
