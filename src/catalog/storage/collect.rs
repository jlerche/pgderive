use super::{Catalog, lock};
use crate::engine::{plan::Plan, reader::ObjectRef};
use crate::storage::PageStore;
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, path::Path};
use std::{collections::BTreeSet, sync::Arc};
use tokio_postgres::{Client, Transaction};

mod cache;
pub use cache::GcCache;

/// Limits bound coarse catalog/mark/list metadata before any destructive request.
#[derive(Debug, Clone, Copy)]
pub struct GcLimits {
    /// Maximum committed and protected root references.
    pub roots: usize,
    /// Maximum reachable paths and closed upload ledger entries.
    pub objects: usize,
    /// Maximum encoded catalog definitions, references and active protection metadata.
    pub metadata_bytes: u64,
    /// Maximum physical objects requested in one global listing page.
    pub page_objects: usize,
}
impl Default for GcLimits {
    fn default() -> Self {
        Self { roots: 4096, objects: 262_144, metadata_bytes: 16 * 1024 * 1024, page_objects: 1000 }
    }
}
impl GcLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            self.roots > 0
                && self.objects > 0
                && self.metadata_bytes > 0
                && (1..=1000).contains(&self.page_objects),
            "invalid GC limits"
        );
        Ok(())
    }
}
/// Physical collection result; logical/source progress is never modified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Collection {
    /// Another physical collector already owns this catalog schema's sweep.
    AlreadyRunning,
    /// An unresolved upload reservation protects the whole namespace.
    UploadProtected,
    /// Completed idempotent deletion of unreferenced canonical immutable objects.
    Collected {
        /// Reachable root and block paths retained.
        retained: usize,
        /// Unreferenced managed objects deleted.
        deleted: usize,
        /// Physical objects examined in this bounded turn.
        scanned: usize,
        /// Global listing pages examined in this turn.
        pages: usize,
        /// Whether this turn reached the end of a complete physical sweep.
        pass_complete: bool,
        /// Durable physical sweep generation; distinct from source/logical time.
        generation: i64,
        /// Finished-upload ledger rows retired after a complete sweep.
        retired_namespaces: u64,
    },
}
impl Catalog {
    /// Collect exclusively within a store namespace owned by this catalog schema.
    /// A bounded SQL snapshot captures live roots and sealed namespaces; mark,
    /// list and DELETE run without holding the publication lifecycle lock.
    /// The caller may retain verified immutable dependencies in a bounded cache;
    /// cached dependencies never establish liveness or scrub unchanged objects.
    /// Every concurrent uploader must reserve before PUT; readers must pin before
    /// opening roots. All publications use the same lifecycle barrier. Unknown
    /// object names are retained. Never run against a shared unmanaged namespace.
    ///
    /// # Errors
    /// Rejects corruption/limits before DELETE, or returns a sweep/SQL failure.
    /// A failed sweep is safe to retry; completed deletions remain physical only.
    pub async fn collect(
        &self,
        sql: &mut Client,
        store: Arc<dyn PageStore>,
        limits: GcLimits,
        cache: &mut GcCache,
    ) -> Result<Collection> {
        limits.validate()?;
        let mut tx = sql
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await?;
        let scope = format!("pgderive-object-collection:{}", self.schema);
        let acquired: bool = tx
            .query_one(
                "SELECT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended($1,0))",
                &[&scope],
            )
            .await?
            .try_get(0)?;
        if !acquired {
            tx.rollback().await?;
            return Ok(Collection::AlreadyRunning);
        }
        let Some(snapshot) = snapshot(&mut tx, self, limits).await? else {
            tx.rollback().await?;
            return Ok(Collection::UploadProtected);
        };
        let objects: Arc<dyn ObjectStore> = store.clone();
        let retained = cache.mark_snapshot(&objects, snapshot.roots.clone(), limits).await?;
        let sweep = candidates(&store, &retained, &snapshot, limits.page_objects).await?;
        let deleted = sweep.candidates.len();
        delete_candidates(&objects, sweep.candidates).await?;
        let pass_complete = sweep.next.is_none();
        let retired_namespaces = finish(&tx, self, &snapshot, sweep.next, &sweep.surviving).await?;
        tx.commit().await.context("GC cursor COMMIT failed; physical sweep is idempotent")?;
        Ok(Collection::Collected {
            retained: retained.len(),
            deleted,
            scanned: sweep.scanned,
            pages: 1,
            pass_complete,
            generation: snapshot.generation,
            retired_namespaces,
        })
    }
}
struct Snapshot {
    roots: Vec<ObjectRef>,
    sealed: BTreeSet<String>,
    quiescent: Vec<String>,
    generation: i64,
    cursor: Option<String>,
    starting: bool,
}
async fn snapshot(
    parent: &mut Transaction<'_>,
    catalog: &Catalog,
    limits: GcLimits,
) -> Result<Option<Snapshot>> {
    let tx = parent.transaction().await?;
    lock(&tx, catalog, true).await?;
    let uploading: bool = tx
        .query_one(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {}.pgderive_protections WHERE uploading AND active)",
                catalog.schema
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    if uploading {
        tx.rollback().await?;
        return Ok(None);
    }
    let metadata = tx.query_one(&format!("SELECT (SELECT COALESCE(SUM(octet_length(definition::text)+octet_length(checkpoint::text)),0) FROM {0}.pgderive_queries)+(SELECT COALESCE(SUM(octet_length(roots::text)),0) FROM {0}.pgderive_protections WHERE active)+(SELECT COALESCE(SUM(octet_length(namespace)),0) FROM {0}.pgderive_protections WHERE uploading AND NOT active),(SELECT COUNT(*) FROM {0}.pgderive_protections WHERE uploading AND NOT active)",catalog.schema),&[]).await?;
    ensure!(
        u64::try_from(metadata.try_get::<_, i64>(0)?)? <= limits.metadata_bytes
            && usize::try_from(metadata.try_get::<_, i64>(1)?)? <= limits.objects,
        "GC catalog metadata budget exceeded"
    );
    let roots = roots(&tx, catalog, limits.roots).await?;
    let rows = tx.query(&format!("SELECT namespace,quiescent FROM {}.pgderive_protections WHERE uploading AND NOT active AND namespace IS NOT NULL",catalog.schema),&[]).await?;
    let mut sealed = BTreeSet::new();
    let mut quiescent = Vec::new();
    for row in rows {
        let path: String = row.try_get(0)?;
        if row.try_get::<_, bool>(1)? {
            quiescent.push(path.clone());
        }
        sealed.insert(path);
    }
    let state = tx
        .query_one(
            &format!(
                "SELECT generation,cursor,in_progress FROM {}.pgderive_gc WHERE singleton",
                catalog.schema
            ),
            &[],
        )
        .await?;
    let starting = !state.try_get::<_, bool>(2)?;
    let prior: i64 = state.try_get(0)?;
    let generation =
        if starting { prior.checked_add(1).context("GC generation overflow")? } else { prior };
    let cursor = state.try_get(1)?;
    // The snapshot's sealed namespaces cannot acquire new live roots:
    // write::stage accepts exact current roots or active fresh uploads only.
    // New roots may reuse packs from sealed namespaces, but only from current
    // or pinned input roots: a preceding GC snapshot marks those dependencies,
    // an active upload blocks snapshots, and a later snapshot marks the new root.
    // Detached adoption verifies that provenance before accepting shared packs.
    // New read protections pin current membership under the same barrier.
    // Release the lifecycle lock before all object-store I/O.
    // Roll back this read-only savepoint to release its lifecycle lock while
    // retaining the outer transaction's independent collector coordination.
    tx.rollback().await.context("GC snapshot savepoint rollback failed")?;
    Ok(Some(Snapshot { roots, sealed, quiescent, generation, cursor, starting }))
}

async fn roots(tx: &Transaction<'_>, catalog: &Catalog, limit: usize) -> Result<Vec<ObjectRef>> {
    let bound = i64::try_from(limit)?;
    let size = tx.query_one(&format!("SELECT (SELECT COUNT(*) FROM {0}.pgderive_roots)+(SELECT COALESCE(SUM(jsonb_array_length(roots)),0) FROM {0}.pgderive_protections WHERE active),(SELECT COUNT(*) FROM {0}.pgderive_queries),(SELECT COALESCE(MAX(octet_length(definition::text)),0) FROM {0}.pgderive_queries)",catalog.schema),&[]).await?;
    ensure!(
        size.try_get::<_, i64>(0)? <= bound
            && size.try_get::<_, i64>(1)? <= bound
            && size.try_get::<_, i32>(2)? <= 1_048_576,
        "GC catalog root/definition limit exceeded"
    );
    let queries = tx
        .query(
            &format!(
                "SELECT query_id,definition FROM {}.pgderive_queries ORDER BY query_id",
                catalog.schema
            ),
            &[],
        )
        .await?;
    let mut roots = Vec::new();
    for row in queries {
        let plan = Plan::new(serde_json::from_value(row.try_get(1)?)?)?;
        let catalog = Catalog::new(&catalog.schema, &row.try_get::<_, String>(0)?)?;
        let boundary = crate::catalog::read::boundary(tx, &catalog, &plan)
            .await?
            .context("GC checkpoint disappeared")?;
        roots.extend(
            boundary.checkpoint.arrangements.into_iter().flat_map(|member| member.trace.objects),
        );
    }
    for row in tx
        .query(
            &format!(
                "SELECT roots FROM {}.pgderive_protections WHERE active ORDER BY token",
                catalog.schema
            ),
            &[],
        )
        .await?
    {
        roots.extend(serde_json::from_value::<Vec<ObjectRef>>(row.try_get(0)?)?);
    }
    ensure!(roots.len() <= limit, "GC protected root limit exceeded");
    Ok(roots)
}
struct Sweep {
    candidates: Vec<Path>,
    next: Option<String>,
    scanned: usize,
    surviving: BTreeSet<String>,
}
async fn candidates(
    store: &Arc<dyn PageStore>,
    retained: &BTreeSet<String>,
    snapshot: &Snapshot,
    limit: usize,
) -> Result<Sweep> {
    let page = store.page("pgderive/", snapshot.cursor.clone(), limit).await?;
    ensure!(
        page.result.objects.len() <= limit && page.result.common_prefixes.is_empty(),
        "invalid GC namespace page"
    );
    ensure!(
        page.page_token.as_ref().is_none_or(|token| !token.is_empty()
            && token.len() <= 16_384
            && Some(token) != snapshot.cursor.as_ref()),
        "invalid GC continuation token"
    );
    let scanned = page.result.objects.len();
    let mut candidates = Vec::new();
    let mut surviving = BTreeSet::new();
    for object in page.result.objects {
        ensure!(object.location.as_ref().starts_with("pgderive/"), "GC page escaped namespace");
        if managed(object.location.as_ref(), &snapshot.sealed)
            && !retained.contains(object.location.as_ref())
        {
            candidates.push(object.location);
        } else if let Some(namespace) = listed_namespace(object.location.as_ref()) {
            surviving.insert(namespace);
        }
    }
    Ok(Sweep { candidates, next: page.page_token, scanned, surviving })
}
fn listed_namespace(path: &str) -> Option<String> {
    let id = path.strip_prefix("pgderive/upload-v1/")?.split('/').next()?;
    Some(format!("pgderive/upload-v1/{id}"))
}
async fn finish(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    snapshot: &Snapshot,
    cursor: Option<String>,
    surviving: &BTreeSet<String>,
) -> Result<u64> {
    lock(tx, catalog, true).await?;
    if snapshot.starting {
        let namespaces = &snapshot.quiescent;
        tx.execute(&format!("UPDATE {}.pgderive_protections SET gc_candidate=$2 WHERE namespace=ANY($1) AND NOT active AND uploading",catalog.schema), &[namespaces, &snapshot.generation]).await?;
    }
    let namespaces = surviving.iter().collect::<Vec<_>>();
    tx.execute(
        &format!(
            "UPDATE {}.pgderive_protections SET gc_seen=$2 WHERE namespace=ANY($1)",
            catalog.schema
        ),
        &[&namespaces, &snapshot.generation],
    )
    .await?;
    let retired = if cursor.is_none() {
        tx.execute(&format!("DELETE FROM {}.pgderive_protections WHERE NOT active AND uploading AND quiescent AND gc_candidate=$1 AND gc_seen<>$1",catalog.schema), &[&snapshot.generation]).await?
    } else {
        0
    };
    tx.execute(
        &format!(
            "DELETE FROM {}.pgderive_protections WHERE NOT active AND NOT uploading",
            catalog.schema
        ),
        &[],
    )
    .await?;
    tx.execute(
        &format!(
            "UPDATE {}.pgderive_gc SET generation=$1,cursor=$2,in_progress=$3 WHERE singleton",
            catalog.schema
        ),
        &[&snapshot.generation, &cursor, &cursor.is_some()],
    )
    .await?;
    Ok(retired)
}
fn managed(path: &str, sealed: &BTreeSet<String>) -> bool {
    let Some((namespace, relative)) = path.split_once("/pgderive/") else { return false };
    if !sealed.contains(namespace) {
        return false;
    }
    let Some(hash) = relative.strip_prefix("batch/").or_else(|| relative.strip_prefix("pack/"))
    else {
        return false;
    };
    hash.len() == 64
        && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

// Explicit application batches bound each adapter stream. S3's adapter otherwise
// chunks 1,000 paths and buffers 20 requests; a finite <=256-path stream yields
// one bulk request. Finish this stream before opening the next, and never retire
// namespace protections following a request-level or per-object error.
async fn delete_candidates(store: &Arc<dyn ObjectStore>, paths: Vec<Path>) -> Result<()> {
    let mut paths = paths.into_iter();
    loop {
        let batch = paths.by_ref().take(256).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(());
        }
        let stream = futures_util::stream::iter(batch.into_iter().map(Ok)).boxed();
        store.delete_stream(stream).try_for_each(|_| async { Ok(()) }).await?;
    }
}

#[cfg(test)]
#[path = "tests/collect_delete.rs"]
mod tests;
