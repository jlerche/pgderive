//! Transactional presentation cutover with catalog attestation and dependency checks.
use super::{Settings, catalog_definition, definition, quote};
use crate::{catalog::Sink, compiler::Compiled};
use anyhow::{Result, ensure};
use serde_json::Value;
use tokio_postgres::Transaction;

pub(in crate::worker) async fn replace(
    sql: &Transaction<'_>,
    old: &Settings,
    new: &Settings,
    compiled: &Compiled,
) -> Result<()> {
    let Some(view) = &new.result_view else {
        return Ok(());
    };
    let desired =
        definition(sql, new, matches!(compiled.sink(&new.sink_table), Sink::Grouped(_))).await?;
    let table = format!("{}.pgderive_result_views", new.catalog_schema);
    sql.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {table}(query_id text PRIMARY KEY,view_name text NOT NULL UNIQUE,definition text NOT NULL,catalog_definition text NOT NULL)")).await?;
    let prior = sql
        .query_opt(
            &format!("SELECT view_name,catalog_definition FROM {table} WHERE query_id=$1"),
            &[&old.query_id],
        )
        .await?;
    let target = format!("{}.{}", new.catalog_schema, quote(view));
    if let Some(row) = prior {
        ensure!(row.try_get::<_, String>(0)? == *view, "rebuild public view identity changed");
        sql.batch_execute(&format!("LOCK TABLE {target} IN ACCESS EXCLUSIVE MODE")).await?;
        ensure!(
            catalog_definition(sql, &new.catalog_schema, view).await?
                == row.try_get::<_, String>(1)?,
            "rebuild public view was altered"
        );
        if !compatible(sql, &target, &desired).await? {
            sql.batch_execute(&format!("DROP VIEW {target} RESTRICT")).await?;
        }
        sql.batch_execute(&format!("CREATE OR REPLACE VIEW {target} AS {desired}")).await?;
        sql.execute(&format!("DELETE FROM {table} WHERE query_id=$1"), &[&old.query_id]).await?;
    } else {
        // An existing unregistered relation causes CREATE to fail; it is never adopted.
        sql.batch_execute(&format!("CREATE VIEW {target} AS {desired}")).await?;
    }
    let actual = catalog_definition(sql, &new.catalog_schema, view).await?;
    sql.execute(
        &format!("INSERT INTO {table} VALUES($1,$2,$3,$4)"),
        &[&new.query_id, view, &desired, &actual],
    )
    .await?;
    Ok(())
}

async fn layout(sql: &Transaction<'_>, relation: &str) -> Result<Value> {
    Ok(sql.query_one("SELECT coalesce(jsonb_agg(jsonb_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum),'[]'::jsonb) FROM pg_catalog.pg_attribute WHERE attrelid=pg_catalog.to_regclass($1) AND attnum>0 AND NOT attisdropped", &[&relation]).await?.try_get(0)?)
}

async fn compatible(sql: &Transaction<'_>, target: &str, desired: &str) -> Result<bool> {
    let token: String = sql
        .query_one("SELECT replace(pg_catalog.gen_random_uuid()::text,'-','')", &[])
        .await?
        .try_get(0)?;
    let probe = format!("pgderive_layout_{token}");
    sql.batch_execute(&format!("CREATE TEMP VIEW {probe} AS {desired}")).await?;
    let same = layout(sql, target).await? == layout(sql, &format!("pg_temp.{probe}")).await?;
    sql.batch_execute(&format!("DROP VIEW pg_temp.{probe}")).await?;
    Ok(same)
}
