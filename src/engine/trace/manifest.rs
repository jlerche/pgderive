use super::{Run, TraceSnapshot};
use crate::engine::reader::{BatchData, BlockCache, ObjectBatch, ObjectRef};
use anyhow::{Result, ensure};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Versioned coarse trace membership; no fine-grained key/value fences are stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Manifest format version; currently one.
    pub version: u32,
    /// Last committed DBSP tick, independent of source LSN.
    pub time: u64,
    /// Physical membership generation, including maintenance changes.
    pub generation: u64,
    /// Ordered run references; repeated object paths retain run multiplicity.
    pub objects: Vec<ObjectRef>,
}
impl Manifest {
    /// Check coarse version, clock and exact codec identity before any object GET.
    ///
    /// # Errors
    /// Rejects unsupported format, invalid generation or mismatched object schemas.
    pub fn validate(&self, schema: &str) -> Result<()> {
        ensure!(self.version == 1, "unsupported trace manifest version");
        ensure!(self.generation >= self.time, "invalid trace manifest generation");
        ensure!(
            self.objects.iter().all(|object| object.schema() == schema),
            "manifest object schema mismatch"
        );
        Ok(())
    }
}
impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    /// Export exact object membership for a durable coarse catalog.
    ///
    /// # Errors
    /// Rejects memory runs, which have no durable object identity.
    pub fn manifest(&self) -> Result<Manifest> {
        let objects = self
            .runs
            .iter()
            .map(|run| match run {
                Run::Object(object) => Ok(object.reference().clone()),
                Run::Memory(_) => anyhow::bail!("memory run cannot be durably checkpointed"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Manifest { version: 1, time: self.time, generation: self.generation, objects })
    }
    /// Reopen and cold-validate every referenced run and consolidated coefficient.
    /// Cached bytes from an old process cannot mask missing or corrupt storage.
    ///
    /// # Errors
    /// Returns format/schema, missing/corrupt object, or final weight failures.
    pub async fn reopen(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        schema: &str,
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        manifest.validate(schema)?;
        let mut runs = Vec::new();
        for reference in manifest.objects {
            runs.push(Run::Object(ObjectBatch::open(store.clone(), reference, schema).await?));
        }
        let mut snapshot = Self { runs, generation: manifest.generation, time: manifest.time };
        snapshot.validate().await?;
        for run in &mut snapshot.runs {
            if let Run::Object(object) = run {
                object.share_cache(cache.clone());
            }
        }
        Ok(snapshot)
    }
}
