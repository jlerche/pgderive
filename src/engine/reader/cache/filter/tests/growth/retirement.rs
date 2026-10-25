//! Retire objects only in this fixture's private in-memory store.
use super::Fixture;
use crate::catalog::{GcCache, GcLimits};
use anyhow::{Context as _, Result, ensure};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::sync::{Arc, atomic::Ordering};

impl Fixture {
    pub(super) async fn retire(&self, cache: &mut GcCache) -> Result<serde_json::Value> {
        let store: Arc<dyn ObjectStore> = self.store.clone();
        let roots = self.trace.manifest()?.objects;
        let limits = GcLimits::default();
        let before = self.store.reads.load(Ordering::SeqCst);
        let bytes = self.store.range_bytes.load(Ordering::SeqCst);
        let clock = tokio::time::Instant::now();
        let wall = std::time::Instant::now();
        // Use the production mark kernel. This single-threaded fixture owns the
        // entire store and has no upload reservations or active prior readers.
        // It does not simulate PostgreSQL snapshot/protection/sweep barriers.
        let live = cache.mark_snapshot(&store, roots.clone(), limits).await?;
        let marked = self.store.reads.load(Ordering::SeqCst);
        assert_eq!(live, cache.mark_snapshot(&store, roots, limits).await?);
        assert_eq!(self.store.reads.load(Ordering::SeqCst), marked);
        let objects = store.list(None).take(limits.objects + 1).try_collect::<Vec<_>>().await?;
        ensure!(objects.len() <= limits.objects, "fixture object inventory exceeds limit");
        let state_bytes = objects
            .iter()
            .filter(|object| live.contains(object.location.as_ref()))
            .try_fold(0_u64, |sum, object| {
            sum.checked_add(object.size).context("state byte overflow")
        })?;
        let obsolete = objects
            .into_iter()
            .filter(|object| !live.contains(object.location.as_ref()))
            .map(|object| object.location)
            .collect::<Vec<Path>>();
        let deleted = obsolete.len();
        stream::iter(obsolete)
            .map(|path| {
                let store = store.clone();
                async move { store.delete(&path).await }
            })
            .buffered(crate::engine::reader::READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(store.list(None).try_collect::<Vec<_>>().await?.len(), live.len());
        let (rss_bytes, peak_rss_bytes) = memory_bytes()?;
        Ok(serde_json::json!({
            "gets":self.store.reads.load(Ordering::SeqCst)-before,
            "read_bytes":self.store.range_bytes.load(Ordering::SeqCst)-bytes,
            "cache_bytes":cache.resident_bytes(),"deleted_objects":deleted,
            "state_objects":live.len(),"state_bytes":state_bytes,"rss_bytes":rss_bytes,
            "peak_rss_bytes":peak_rss_bytes,
            "virtual_io_us":clock.elapsed().as_micros(),"wall_us":wall.elapsed().as_micros()
        }))
    }
}

fn memory_bytes() -> Result<(usize, usize)> {
    let status = std::fs::read_to_string("/proc/self/status")
        .context("longevity RSS measurement requires Linux /proc")?;
    Ok((memory_field(&status, "VmRSS:")?, memory_field(&status, "VmHWM:")?))
}

fn memory_field(status: &str, name: &str) -> Result<usize> {
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|line| line.split_whitespace().next())
        .with_context(|| format!("missing {name}"))?
        .parse::<usize>()?;
    kib.checked_mul(1024).context("RSS byte overflow")
}
