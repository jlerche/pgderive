use super::{Binding, Catalog, Durable, Stored};
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
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
    ensure!(matches!(row.try_get::<_, i32>(0)?, 1..=3), "unsupported catalog query format");
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

// The caller locks the query row first for write-side operations. Keep progress
// decoding and integrity checks identical for readers, claims and maintenance.
pub(super) async fn durable(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    plan: &Plan,
    lock_progress: bool,
) -> Result<Option<Durable>> {
    let suffix = if lock_progress { " FOR UPDATE" } else { "" };
    let Some(stored) = boundary(tx, catalog, plan).await? else {
        return Ok(None);
    };
    let row = tx.query_opt(&format!("SELECT binding,sink_table,fence,commit_lsn::text,end_lsn::text,xid,checkpoint_lsn::text,published_time FROM {}.pgderive_progress WHERE query_id=$1{suffix}", catalog.schema), &[&catalog.query]).await?.context("query has no durable publication contract")?;
    let binding: Binding = serde_json::from_value(row.try_get(0)?)?;
    binding.validate()?;
    binding.verify_ownership(tx, catalog).await?;
    ensure!(
        row.try_get::<_, String>(1)? == binding.sink.table.as_str(),
        "durable destination identity mismatch"
    );
    let fence: i64 = row.try_get(2)?;
    let commit = row.try_get::<_, String>(3)?.parse()?;
    let end = row.try_get::<_, String>(4)?.parse()?;
    let xid = row.try_get::<_, Option<i64>>(5)?.map(u32::try_from).transpose()?;
    let checkpoint_end = row.try_get::<_, String>(6)?.parse()?;
    let published_time = u64::try_from(row.try_get::<_, i64>(7)?)?;
    ensure!(
        checkpoint_end <= end && published_time >= stored.checkpoint.time,
        "invalid checkpoint/publication boundary"
    );
    ensure!(
        (checkpoint_end == end) == (published_time == stored.checkpoint.time),
        "checkpoint LSN/clock disagreement"
    );
    ensure!(fence > 0 && end >= commit, "invalid durable fence/source position");
    ensure!(
        Binding::valid_identity(published_time, xid, commit, end),
        "durable transaction identity/clock mismatch"
    );
    Ok(Some(Durable { stored, binding, fence, commit, end, checkpoint_end, published_time, xid }))
}
