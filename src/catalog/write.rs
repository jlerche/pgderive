use super::Catalog;
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Transaction;

/// Stage membership in the caller's transaction so sink/progress can share COMMIT.
pub(super) async fn stage(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    plan: &Plan,
    checkpoint: &Checkpoint,
    expected: u64,
) -> Result<u64> {
    super::storage::shared(tx, catalog).await?;
    super::storage::validate_additions(tx, catalog, checkpoint).await?;
    let epoch = expected.checked_add(1).context("catalog epoch overflow")?;
    let next = i64::try_from(epoch)?;
    let expected = i64::try_from(expected)?;
    let time = i64::try_from(checkpoint.time)?;
    let definition = serde_json::to_value(plan.definition())?;
    let encoded = serde_json::to_value(checkpoint)?;
    let digest = super::checkpoint_digest(checkpoint)?;
    let affected = if expected == 0 {
        tx.execute(&format!("INSERT INTO {}.pgderive_queries(query_id,format_version,plan_identity,definition,logical_time,epoch,checkpoint,checkpoint_digest)
            VALUES($1,2,$2,$3,$4,$5,$6,$7) ON CONFLICT(query_id) DO NOTHING", catalog.schema),
            &[&catalog.query, &plan.identity(), &definition, &time, &next, &encoded, &digest]).await?
    } else {
        tx.execute(&format!("UPDATE {}.pgderive_queries SET logical_time=$4,epoch=$5,checkpoint=$7,checkpoint_digest=$8
            WHERE query_id=$1 AND plan_identity=$2 AND definition=$3 AND epoch=$6 AND logical_time<=$4 AND format_version IN (1,2,3)", catalog.schema),
            &[&catalog.query, &plan.identity(), &definition, &time, &next, &expected, &encoded, &digest]).await?
    };
    ensure!(affected == 1, "stale or incompatible catalog checkpoint writer");
    Ok(epoch)
}
