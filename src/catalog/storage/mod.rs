mod collect;
mod protections;
use super::Catalog;
use anyhow::{Context, Result};
pub use collect::{Collection, GcCache, GcLimits};
pub use protections::Protection;
use tokio_postgres::Transaction;

pub(super) async fn shared(sql: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
    lock(sql, catalog, false).await
}
/// Transaction-bound lifecycle protection and synchronous durability configuration.
/// It cannot be constructed, cloned or retained beyond its transaction.
pub(super) struct Publication<'a, 'tx> {
    sql: &'a Transaction<'tx>,
    catalog: &'a Catalog,
}
impl<'a, 'tx> Publication<'a, 'tx> {
    pub(super) const fn transaction(&self) -> &'a Transaction<'tx> {
        self.sql
    }
    pub(super) const fn catalog(&self) -> &'a Catalog {
        self.catalog
    }
}
// Configure durable publication and acquire lifecycle protection in one round trip.
pub(super) async fn publication_lock<'a, 'tx>(
    sql: &'a Transaction<'tx>,
    catalog: &'a Catalog,
) -> Result<Publication<'a, 'tx>> {
    let scope = format!("pgderive-object-lifecycle:{}", catalog.schema);
    sql.query_typed_one(
        "SELECT pg_catalog.set_config('synchronous_commit','on',true), pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended($1,0))",
        &[(&scope, tokio_postgres::types::Type::TEXT)],
    ).await?;
    Ok(Publication { sql, catalog })
}
async fn lock(sql: &Transaction<'_>, catalog: &Catalog, exclusive: bool) -> Result<()> {
    let function = if exclusive { "pg_advisory_xact_lock" } else { "pg_advisory_xact_lock_shared" };
    let scope = format!("pgderive-object-lifecycle:{}", catalog.schema);
    sql.query_one(
        &format!("SELECT pg_catalog.{function}(pg_catalog.hashtextextended($1,0))"),
        &[&scope],
    )
    .await?;
    Ok(())
}

// A sealed namespace may already have a delayed DELETE in flight. Publication
// can retain/narrow current extents or add roots in an active, fresh reservation;
// it cannot resurrect an old sealed object after its protection has ended.
pub async fn validate_additions(
    sql: &Transaction<'_>,
    catalog: &Catalog,
    checkpoint: &crate::engine::plan::Checkpoint,
) -> Result<()> {
    use anyhow::ensure;
    let candidates = checkpoint
        .arrangements
        .iter()
        .flat_map(|member| &member.trace.objects)
        .map(|root| {
            root.validate_identity()?;
            Ok((
                root,
                root.namespace().context("catalog objects require managed upload namespaces")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let row = sql.query_one(
        &format!("WITH current_roots AS MATERIALIZED (SELECT reference FROM {0}.pgderive_roots WHERE query_id=$1), candidates AS (SELECT value->0 AS reference,value->>1 AS namespace FROM jsonb_array_elements($2::jsonb))
            SELECT NOT EXISTS(SELECT 1 FROM candidates c WHERE NOT EXISTS(SELECT 1 FROM current_roots r WHERE r.reference-'blocks'=c.reference-'blocks' AND (r.reference->'blocks'='null'::jsonb OR (c.reference->'blocks'<>'null'::jsonb AND (c.reference->'blocks'->>'start')::numeric >= (r.reference->'blocks'->>'start')::numeric AND (c.reference->'blocks'->>'end')::numeric <= (r.reference->'blocks'->>'end')::numeric))) AND NOT EXISTS(SELECT 1 FROM {0}.pgderive_protections p WHERE p.query_id=$1 AND p.uploading AND p.active AND p.namespace=c.namespace AND p.owner_fence=COALESCE((SELECT fence FROM {0}.pgderive_progress WHERE query_id=$1),0)))", catalog.schema),
        &[&catalog.query, &serde_json::to_value(candidates)?],
    ).await?;
    ensure!(row.try_get::<_, bool>(0)?, "new object has no active upload reservation");
    Ok(())
}
