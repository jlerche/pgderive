use super::{Catalog, lock};
use crate::engine::{plan::Plan, reader::ObjectRef};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{collections::BTreeSet, sync::Arc};
use tokio_postgres::{Client, Transaction};

/// Limits bound coarse catalog/mark/list metadata before any destructive request.
#[derive(Debug, Clone, Copy)]
pub struct GcLimits {
    /// Maximum committed and protected root references.
    pub roots: usize,
    /// Maximum reachable paths and enumerated managed objects.
    pub objects: usize,
    /// Maximum encoded catalog definitions, references and active protection metadata.
    pub metadata_bytes: u64,
}
impl Default for GcLimits {
    fn default() -> Self {
        Self { roots: 4096, objects: 262_144, metadata_bytes: 16 * 1024 * 1024 }
    }
}
/// Physical collection result; logical/source progress is never modified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collection {
    /// An unresolved upload reservation protects the whole namespace.
    UploadProtected,
    /// Completed idempotent deletion of unreferenced canonical v2 objects.
    Collected {
        /// Reachable root and block paths retained.
        retained: usize,
        /// Unreferenced managed objects deleted.
        deleted: usize,
    },
}
impl Catalog {
    /// Collect exclusively within a store namespace owned by this catalog schema.
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
        store: Arc<dyn ObjectStore>,
        limits: GcLimits,
    ) -> Result<Collection> {
        ensure!(
            limits.roots > 0 && limits.objects > 0 && limits.metadata_bytes > 0,
            "invalid GC limits"
        );
        let tx = sql.transaction().await?;
        lock(&tx, self, true).await?;
        let uploading: bool = tx
            .query_one(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {}.pgderive_protections WHERE uploading AND active)",
                    self.schema
                ),
                &[],
            )
            .await?
            .try_get(0)?;
        if uploading {
            tx.commit().await?;
            return Ok(Collection::UploadProtected);
        }
        let metadata = tx.query_one(&format!("SELECT (SELECT COALESCE(SUM(octet_length(definition::text)),0) FROM {0}.pgderive_queries)+(SELECT COALESCE(SUM(octet_length(reference::text)),0) FROM {0}.pgderive_objects)+(SELECT COALESCE(SUM(octet_length(roots::text)),0) FROM {0}.pgderive_protections WHERE active),(SELECT COUNT(*) FROM {0}.pgderive_protections WHERE uploading AND NOT active)",self.schema),&[]).await?;
        ensure!(
            u64::try_from(metadata.try_get::<_, i64>(0)?)? <= limits.metadata_bytes
                && usize::try_from(metadata.try_get::<_, i64>(1)?)? <= limits.objects,
            "GC catalog metadata budget exceeded"
        );
        let roots = roots(&tx, self, limits.roots).await?;
        let retained = mark(&store, roots, limits.objects).await?;
        let sealed = tx.query(&format!("SELECT namespace FROM {}.pgderive_protections WHERE uploading AND NOT active AND namespace IS NOT NULL",self.schema),&[]).await?.into_iter().map(|row|row.try_get::<_,String>(0)).collect::<std::result::Result<BTreeSet<_>,_>>()?;
        ensure!(sealed.len() <= limits.objects, "GC namespace metadata limit exceeded");
        let candidates = candidates(&store, &retained, &sealed, limits.objects).await?;
        let deleted = candidates.len();
        for path in candidates {
            store.delete(&path).await?;
        }
        tx.commit()
            .await
            .context("GC metadata barrier COMMIT failed; physical sweep is idempotent")?;
        Ok(Collection::Collected { retained: retained.len(), deleted })
    }
}
async fn roots(tx: &Transaction<'_>, catalog: &Catalog, limit: usize) -> Result<Vec<ObjectRef>> {
    let bound = i64::try_from(limit)?;
    let size = tx.query_one(&format!("SELECT (SELECT COUNT(*) FROM {0}.pgderive_objects)+(SELECT COALESCE(SUM(jsonb_array_length(roots)),0) FROM {0}.pgderive_protections WHERE active),(SELECT COUNT(*) FROM {0}.pgderive_queries),(SELECT COALESCE(MAX(octet_length(definition::text)),0) FROM {0}.pgderive_queries)",catalog.schema),&[]).await?;
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
async fn mark(
    store: &Arc<dyn ObjectStore>,
    roots: Vec<ObjectRef>,
    limit: usize,
) -> Result<BTreeSet<String>> {
    let mut retained = BTreeSet::new();
    for root in roots {
        let children = root.dependencies(store).await?;
        retained.insert(root.path().into());
        for child in children {
            retained.insert(child);
            ensure!(retained.len() <= limit, "GC mark limit exceeded");
        }
        ensure!(retained.len() <= limit, "GC mark limit exceeded");
    }
    Ok(retained)
}
async fn candidates(
    store: &Arc<dyn ObjectStore>,
    retained: &BTreeSet<String>,
    sealed: &BTreeSet<String>,
    limit: usize,
) -> Result<Vec<Path>> {
    let mut list = store.list(Some(&Path::from("pgderive")));
    let mut candidates = Vec::new();
    let mut count = 0_usize;
    while let Some(object) = list.try_next().await? {
        count = count.checked_add(1).context("GC listing count overflow")?;
        ensure!(count <= limit, "GC listing limit exceeded");
        if managed(object.location.as_ref(), sealed) && !retained.contains(object.location.as_ref())
        {
            candidates.push(object.location);
        }
    }
    Ok(candidates)
}
fn managed(path: &str, sealed: &BTreeSet<String>) -> bool {
    let Some((namespace, relative)) = path.split_once("/pgderive/") else { return false };
    if !sealed.contains(namespace) {
        return false;
    }
    let Some(hash) =
        relative.strip_prefix("batch-v2/").or_else(|| relative.strip_prefix("block-v2/"))
    else {
        return false;
    };
    hash.len() == 64
        && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
