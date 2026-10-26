use crate::{catalog::Catalog, engine::plan::Plan};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStoreExt as _, path::Path};
use std::sync::Arc;
use tokio_postgres::Client;

pub(super) async fn corrupt_membership(
    sql: &mut Client,
    schema: &str,
    catalog: &Catalog,
    plan: &Plan,
    store: Arc<dyn crate::storage::PageStore>,
) -> Result<()> {
    let query = super::super::recovery::QUERY;
    let table = format!("{schema}.pgderive_queries");
    let before = catalog
        .load_durable(sql, plan)
        .await?
        .context("missing checkpoint before corruption checks")?;
    let encoded = serde_json::to_value(&before.stored.checkpoint)?;
    let index = before
        .stored
        .checkpoint
        .arrangements
        .iter()
        .position(|member| member.id == "output")
        .context("missing output corruption target")?;
    for (expression, expected) in [
        (
            format!("checkpoint #- '{{arrangements,{index},trace,objects,0}}'"),
            "catalog checkpoint checksum mismatch",
        ),
        (
            format!(
                "jsonb_set(checkpoint,'{{arrangements,{index},trace,objects,0,bytes}}','0'::jsonb)"
            ),
            "catalog checkpoint checksum mismatch",
        ),
        (
            format!(
                "jsonb_set(checkpoint,'{{arrangements,{index},trace,objects,0,namespace}}','null'::jsonb)"
            ),
            "catalog objects require managed upload namespaces",
        ),
    ] {
        let changed = sql
            .execute(
                &format!("UPDATE {table} SET checkpoint={expression} WHERE query_id=$1"),
                &[&query],
            )
            .await?;
        ensure!(changed == 1, "missing metadata corruption target");
        expect_failure(catalog.load_durable(sql, plan).await, expected)?;
        expect_failure(
            catalog
                .collect(
                    sql,
                    store.clone(),
                    crate::catalog::GcLimits::default(),
                    &mut crate::catalog::GcCache::default(),
                )
                .await,
            expected,
        )?;
        for member in &before.stored.checkpoint.arrangements {
            for root in &member.trace.objects {
                store.head(&Path::from(root.path())).await?;
            }
        }

        sql.execute(
            &format!("UPDATE {table} SET checkpoint=$2 WHERE query_id=$1"),
            &[&query, &encoded],
        )
        .await?;
    }
    let after = catalog
        .load_durable(sql, plan)
        .await?
        .context("checkpoint missing after corruption restoration")?;
    ensure!(after == before, "restored catalog differs from original checkpoint");
    eprintln!("MVP unmanaged catalog roots rejected before recovery or GC");
    eprintln!("MVP incomplete and corrupt catalog membership rejected without epoch change");
    Ok(())
}
fn expect_failure<T>(result: Result<T>, expected: &str) -> Result<()> {
    let error = result.err().context("corrupt catalog was accepted")?;
    ensure!(format!("{error:#}").contains(expected), "unexpected catalog failure: {error:#}");
    eprintln!("expected catalog recovery failure: {error:#}");
    Ok(())
}
