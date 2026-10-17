use super::{Catalog, Stored};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Result, ensure};
use tokio_postgres::Transaction;

pub(super) async fn boundary(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    plan: &Plan,
) -> Result<Option<Stored>> {
    let row = tx.query_opt(&format!("SELECT format_version,plan_identity,definition,logical_time,epoch,checkpoint,checkpoint_digest FROM {}.pgderive_queries WHERE query_id=$1", catalog.schema), &[&catalog.query]).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    ensure!(row.try_get::<_, i32>(0)? == 1, "unsupported catalog query format");
    let identity: String = row.try_get(1)?;
    let definition: serde_json::Value = row.try_get(2)?;
    let registered = Plan::new(serde_json::from_value(definition)?)?;
    ensure!(
        registered.identity() == identity && identity == plan.identity(),
        "catalog registered plan mismatch"
    );
    let time = u64::try_from(row.try_get::<_, i64>(3)?)?;
    let epoch = u64::try_from(row.try_get::<_, i64>(4)?)?;
    ensure!(epoch > 0, "invalid catalog epoch");
    let checkpoint: Checkpoint = serde_json::from_value(row.try_get(5)?)?;
    ensure!(
        super::checkpoint_digest(&checkpoint)? == row.try_get::<_, String>(6)?,
        "catalog checkpoint checksum mismatch"
    );
    ensure!(checkpoint.time == time, "catalog checkpoint clock mismatch");
    checkpoint.validate(plan)?;
    Ok(Some(Stored { epoch, checkpoint }))
}
