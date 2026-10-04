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
    for member in &checkpoint.arrangements {
        for root in &member.trace.objects {
            let Some(namespace) = root.namespace() else { continue };
            let row = sql.query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM {0}.pgderive_objects WHERE query_id=$1 AND reference=$2) OR EXISTS(SELECT 1 FROM {0}.pgderive_protections WHERE query_id=$1 AND uploading AND active AND namespace=$3 AND owner_fence=COALESCE((SELECT fence FROM {0}.pgderive_progress WHERE query_id=$1),0))", catalog.schema),
                &[&catalog.query, &serde_json::to_value(root)?, &namespace],
            ).await?;
            ensure!(row.try_get::<_, bool>(0)?, "new object has no active upload reservation");
        }
    }
    Ok(())
}
