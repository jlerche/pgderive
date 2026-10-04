use crate::{catalog::Catalog, engine::plan::Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub(super) async fn corrupt_membership(
    sql: &mut Client,
    schema: &str,
    catalog: &Catalog,
    plan: &Plan,
) -> Result<()> {
    let query = super::super::recovery::QUERY;
    let before =
        catalog.load(sql, plan).await?.context("missing checkpoint before corruption checks")?;
    let reference: serde_json::Value = sql.query_one(&format!("SELECT reference FROM {schema}.pgderive_objects WHERE query_id=$1 AND arrangement_id='output' AND ordinal=0"), &[&query]).await?.try_get(0)?;
    let deleted = sql.execute(&format!("DELETE FROM {schema}.pgderive_objects WHERE query_id=$1 AND arrangement_id='output' AND ordinal=0"), &[&query]).await?;
    ensure!(deleted == 1, "missing metadata corruption target");
    expect_failure(catalog.load(sql, plan).await, "incomplete catalog object membership")?;
    sql.execute(&format!("INSERT INTO {schema}.pgderive_objects(query_id,arrangement_id,ordinal,reference) VALUES($1,'output',0,$2)"), &[&query, &reference]).await?;
    sql.execute(&format!("UPDATE {schema}.pgderive_objects SET reference=jsonb_set(reference,'{{bytes}}','0'::jsonb) WHERE query_id=$1 AND arrangement_id='output' AND ordinal=0"), &[&query]).await?;
    expect_failure(catalog.load(sql, plan).await, "catalog membership checksum mismatch")?;
    sql.execute(&format!("UPDATE {schema}.pgderive_objects SET reference=$2 WHERE query_id=$1 AND arrangement_id='output' AND ordinal=0"), &[&query, &reference]).await?;
    let after = catalog
        .load(sql, plan)
        .await?
        .context("checkpoint missing after corruption restoration")?;
    ensure!(
        after.epoch == before.epoch && after.checkpoint == before.checkpoint,
        "restored catalog differs from original checkpoint"
    );
    eprintln!("MVP incomplete and corrupt catalog membership rejected without epoch change");
    Ok(())
}
fn expect_failure<T>(result: Result<T>, expected: &str) -> Result<()> {
    let error = result.err().context("corrupt catalog was accepted")?;
    ensure!(format!("{error:#}").contains(expected), "unexpected catalog failure: {error:#}");
    eprintln!("expected catalog recovery failure: {error:#}");
    Ok(())
}
