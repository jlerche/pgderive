use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};

/// Create membership under bootstrap's already-held lifecycle protection.
pub(super) async fn create(
    protected: &super::storage::Publication<'_, '_>,
    plan: &Plan,
    checkpoint: &Checkpoint,
) -> Result<()> {
    let tx = protected.transaction();
    let catalog = protected.catalog();
    super::storage::validate_additions(tx, catalog, checkpoint).await?;
    let affected = tx.execute(&format!("INSERT INTO {}.pgderive_queries(query_id,format_version,plan_identity,definition,logical_time,epoch,checkpoint,checkpoint_digest)
        VALUES($1,3,$2,$3,$4,1,$5,$6) ON CONFLICT(query_id) DO NOTHING", catalog.schema),
        &[&catalog.query, &plan.identity(), &serde_json::to_value(plan.definition())?, &i64::try_from(checkpoint.time)?, &serde_json::to_value(checkpoint)?, &super::checkpoint_digest(checkpoint)?]).await?;
    ensure!(affected == 1, "stale or incompatible catalog checkpoint writer");
    Ok(())
}
/// Replace memberships using the same transaction's complete boundary proof.
/// Keep the epoch CAS and affected-row check even with the query row held locked.
pub(super) async fn stage(
    locked: &super::validation::LockedBoundary<'_, '_>,
    checkpoint: &Checkpoint,
) -> Result<u64> {
    let tx = locked.transaction();
    let catalog = locked.catalog();
    super::storage::validate_additions(tx, catalog, checkpoint).await?;
    let epoch = locked.epoch().checked_add(1).context("catalog epoch overflow")?;
    let affected = tx.execute(&format!("UPDATE {}.pgderive_queries SET logical_time=$4,epoch=$3,checkpoint=$5,checkpoint_digest=$6
        WHERE query_id=$1 AND epoch=$2 AND logical_time<=$4", catalog.schema),
        &[&catalog.query, &i64::try_from(locked.epoch())?, &i64::try_from(epoch)?, &i64::try_from(checkpoint.time)?, &serde_json::to_value(checkpoint)?, &super::checkpoint_digest(checkpoint)?]).await?;
    ensure!(affected == 1, "stale or incompatible catalog checkpoint writer");
    Ok(epoch)
}
