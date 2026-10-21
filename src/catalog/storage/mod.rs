mod collect;
mod protections;
use super::Catalog;
use anyhow::Result;
pub use collect::{Collection, GcLimits};
pub use protections::Protection;
use tokio_postgres::Transaction;

pub(super) async fn shared(sql: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
    lock(sql, catalog, false).await
}
// Configure durable publication and acquire lifecycle protection in one round trip.
pub(super) async fn publication_lock(sql: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
    let scope = format!("pgderive-object-lifecycle:{}", catalog.schema);
    sql.query_typed_one(
        "SELECT pg_catalog.set_config('synchronous_commit','on',true), pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended($1,0))",
        &[(&scope, tokio_postgres::types::Type::TEXT)],
    ).await?;
    Ok(())
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
// can retain exact current roots or add roots in an active, fresh reservation;
// it cannot resurrect an old sealed object after its protection has ended.
pub(super) async fn validate_additions(
    sql: &Transaction<'_>,
    catalog: &Catalog,
    checkpoint: &crate::engine::plan::Checkpoint,
) -> Result<()> {
    use anyhow::ensure;
    let candidates = checkpoint
        .arrangements
        .iter()
        .flat_map(|member| &member.trace.objects)
        .filter_map(|root| root.namespace().map(|namespace| (root, namespace)))
        .collect::<Vec<_>>();
    let row = sql.query_one(
        &format!("WITH current_roots AS MATERIALIZED (SELECT reference FROM {0}.pgderive_roots WHERE query_id=$1), candidates AS (SELECT value->0 AS reference,value->>1 AS namespace FROM jsonb_array_elements($2::jsonb))
            SELECT NOT EXISTS(SELECT 1 FROM candidates c WHERE NOT EXISTS(SELECT 1 FROM current_roots r WHERE r.reference=c.reference) AND NOT EXISTS(SELECT 1 FROM {0}.pgderive_protections p WHERE p.query_id=$1 AND p.uploading AND p.active AND p.namespace=c.namespace AND p.owner_fence=COALESCE((SELECT fence FROM {0}.pgderive_progress WHERE query_id=$1),0)))", catalog.schema),
        &[&catalog.query, &serde_json::to_value(candidates)?],
    ).await?;
    ensure!(row.try_get::<_, bool>(0)?, "new object has no active upload reservation");
    Ok(())
}
