//! Verified immutable pack dependencies, bounded independently of tuple indexes.
use super::GcLimits;
use crate::engine::reader::ObjectRef;
use anyhow::{Context as _, Result, ensure};
use object_store::ObjectStore;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Disposable dependency memoization for repeated physical collection.
/// Entries never establish liveness: every mark uses its current SQL snapshot.
#[derive(Debug, Default)]
pub struct GcCache {
    store: Option<Arc<dyn ObjectStore>>,
    entries: BTreeMap<String, Cached>,
    bytes: usize,
    limit: usize,
}
#[derive(Debug)]
struct Cached {
    root: ObjectRef,
    paths: Arc<Vec<String>>,
    charge: usize,
}
impl GcCache {
    /// Mark exactly the supplied authoritative root snapshot using verified dependencies.
    /// This does not establish SQL liveness or authorize concurrent physical deletion.
    pub(crate) async fn mark_snapshot(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        roots: Vec<ObjectRef>,
        limits: GcLimits,
    ) -> Result<BTreeSet<String>> {
        limits.validate()?;
        ensure!(roots.len() <= limits.roots, "GC snapshot root limit exceeded");
        root_metadata(&roots, limits.metadata_bytes)?;
        self.prepare(store, &roots, limits.metadata_bytes)?;
        let mut retained = BTreeSet::new();
        for root in roots {
            let children = self.dependencies(&root).await?;
            retained.insert(root.path().into());
            for child in children.iter() {
                retained.insert(child.clone());
                ensure!(retained.len() <= limits.objects, "GC mark limit exceeded");
            }
            ensure!(retained.len() <= limits.objects, "GC mark limit exceeded");
        }
        Ok(retained)
    }
    /// Current verified dependency-cache charge; excludes the caller's mark set.
    pub(crate) const fn resident_bytes(&self) -> usize {
        self.bytes
    }
    pub(super) fn prepare(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        roots: &[ObjectRef],
        limit: u64,
    ) -> Result<()> {
        self.limit = usize::try_from(limit)?;
        if !self.store.as_ref().is_some_and(|prior| Arc::ptr_eq(prior, store)) {
            self.entries.clear();
            self.store = Some(store.clone());
        }
        let live = roots.iter().map(|root| (root.path(), root)).collect::<BTreeMap<_, _>>();
        self.entries
            .retain(|path, entry| live.get(path.as_str()).is_some_and(|root| **root == entry.root));
        self.bytes = self.entries.values().map(|entry| entry.charge).sum();
        self.trim(0);
        Ok(())
    }
    pub(super) async fn dependencies(&mut self, root: &ObjectRef) -> Result<Arc<Vec<String>>> {
        if let Some(entry) = self.entries.get(root.path())
            && entry.root == *root
        {
            return Ok(entry.paths.clone());
        }
        let paths = Arc::new(
            root.dependencies(self.store.as_ref().context("GC cache store missing")?).await?,
        );
        let charge = charge(root, &paths)?;
        if charge <= self.limit {
            if let Some(old) = self.entries.remove(root.path()) {
                self.bytes -= old.charge;
            }
            self.trim(charge);
            self.entries.insert(
                root.path().into(),
                Cached { root: root.clone(), paths: paths.clone(), charge },
            );
            self.bytes += charge;
        }
        Ok(paths)
    }
    fn trim(&mut self, incoming: usize) {
        while self.bytes > self.limit - incoming {
            let Some((_, old)) = self.entries.pop_first() else { break };
            self.bytes -= old.charge;
        }
    }
}
fn root_metadata(roots: &[ObjectRef], limit: u64) -> Result<()> {
    let mut bytes = 2_u64;
    for (ordinal, root) in roots.iter().enumerate() {
        bytes = bytes
            .checked_add(u64::try_from(serde_json::to_vec(root)?.len())?)
            .and_then(|bytes| bytes.checked_add(u64::from(ordinal > 0)))
            .context("GC root metadata overflow")?;
        ensure!(bytes <= limit, "GC snapshot metadata limit exceeded");
    }
    ensure!(bytes <= limit, "GC snapshot metadata limit exceeded");
    Ok(())
}
fn charge(root: &ObjectRef, paths: &Vec<String>) -> Result<usize> {
    let bytes = std::mem::size_of::<Cached>()
        .checked_add(serde_json::to_vec(root)?.len())
        .and_then(|bytes| bytes.checked_add(root.path().len()))
        .and_then(|bytes| bytes.checked_add(5 * std::mem::size_of::<usize>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<String>>()))
        .and_then(|bytes| {
            bytes.checked_add(paths.capacity().checked_mul(std::mem::size_of::<String>())?)
        })
        .context("GC dependency cache charge overflow")?;
    paths.iter().try_fold(bytes, |bytes, path| {
        bytes.checked_add(path.capacity()).context("GC dependency cache charge overflow")
    })
}

#[cfg(test)]
mod tests;
