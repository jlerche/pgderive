//! Atomic typed destination cutover. Compatible layouts preserve the public OID.
use super::Generation;
use crate::{
    catalog::Binding,
    worker::{Settings, admin::quote},
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Transaction;

async fn binding(sql: &Transaction<'_>, settings: &Settings, query: &str) -> Result<Binding> {
    let value = sql
        .query_one(
            &format!(
                "SELECT binding FROM {}.pgderive_progress WHERE query_id=$1 FOR UPDATE",
                settings.catalog_schema
            ),
            &[&query],
        )
        .await?
        .try_get(0)?;
    Ok(serde_json::from_value(value)?)
}

async fn retarget(
    sql: &Transaction<'_>,
    settings: &Settings,
    query: &str,
    name: &str,
    binding: &mut Binding,
) -> Result<()> {
    binding.sink.table = name.into();
    let catalog = crate::catalog::Catalog::new(&settings.catalog_schema, query)?;
    let relation = format!("{}.{}", settings.catalog_schema, quote(name));
    let oid: u32 =
        sql.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&relation]).await?.try_get(0)?;
    sql.execute(
        &format!(
            "INSERT INTO {}.pgderive_sink_ownership VALUES($1,$2,$3)",
            settings.catalog_schema
        ),
        &[&name, &query, &oid],
    )
    .await?;
    sql.execute(&format!("UPDATE {}.pgderive_progress SET binding=$2,sink_table=$3,fence=fence+1 WHERE query_id=$1",settings.catalog_schema), &[&query,&serde_json::to_value(&binding)?,&name]).await?;
    sql.execute(
        &format!(
            "UPDATE {}.pgderive_worker_registration SET sink=$2 WHERE query_id=$1",
            settings.catalog_schema
        ),
        &[&query, &serde_json::to_value(&binding.sink)?],
    )
    .await?;
    binding.sink.owned(sql, &catalog).await?.context("cutover destination ownership missing")?;
    Ok(())
}

pub(super) async fn cutover(
    sql: &Transaction<'_>,
    settings: &Settings,
    old: &mut Generation,
    new: &mut Generation,
) -> Result<()> {
    let mut prior = binding(sql, settings, &old.query).await?;
    let mut next = binding(sql, settings, &new.query).await?;
    let schema = &settings.catalog_schema;
    let target = format!("{schema}.{}", quote(&old.sink));
    let staging = format!("{schema}.{}", quote(&new.sink));
    sql.batch_execute(&format!("LOCK TABLE {target},{staging} IN ACCESS EXCLUSIVE MODE")).await?;
    for (generation, binding) in [(&*old, &prior), (&*new, &next)] {
        let catalog = crate::catalog::Catalog::new(schema, &generation.query)?;
        binding
            .sink
            .owned(sql, &catalog)
            .await?
            .context("cutover destination ownership missing")?;
    }
    ensure!(
        prior.sink.columns == next.sink.columns && prior.sink.primary_key == next.sink.primary_key,
        "result schema changed; drop and recreate explicitly, and resnapshot logical-replication consumers"
    );
    // Destination contents are user-visible results, not engine recovery state.
    sql.batch_execute(&format!(
        "DELETE FROM {target}; INSERT INTO {target} SELECT * FROM {staging}; DELETE FROM {staging}"
    ))
    .await?;
    sql.execute(
        &format!("DELETE FROM {schema}.pgderive_sink_ownership WHERE query_id=$1 OR query_id=$2"),
        &[&old.query, &new.query],
    )
    .await?;
    // Vacate the unique progress-table name while swapping the two bindings.
    // The reserved name cannot be configured as a destination and is never committed.
    sql.execute(
        &format!("UPDATE {schema}.pgderive_progress SET sink_table=$2 WHERE query_id=$1"),
        &[&new.query, &format!("pgderive_cutover_{}", new.sink)],
    )
    .await?;
    let public = old.sink.clone();
    old.sink.clone_from(&new.sink);
    new.sink = public;
    retarget(sql, settings, &old.query, &old.sink, &mut prior).await?;
    retarget(sql, settings, &new.query, &new.sink, &mut next).await
}

pub(super) async fn rename(
    sql: &Transaction<'_>,
    settings: &Settings,
    generation: &mut Generation,
) -> Result<()> {
    let mut next = binding(sql, settings, &generation.query).await?;
    if next.sink.output == crate::catalog::SinkOutput::Stdout {
        next.sink.table.clone_from(&settings.sink_table);
        sql.execute(
            &format!(
                "UPDATE {}.pgderive_progress SET binding=$2,sink_table=$3 WHERE query_id=$1",
                settings.catalog_schema
            ),
            &[&generation.query, &serde_json::to_value(&next)?, &settings.sink_table],
        )
        .await?;
        sql.execute(
            &format!(
                "UPDATE {}.pgderive_worker_registration SET sink=$2 WHERE query_id=$1",
                settings.catalog_schema
            ),
            &[&generation.query, &serde_json::to_value(&next.sink)?],
        )
        .await?;
        generation.sink.clone_from(&settings.sink_table);
        return Ok(());
    }
    let catalog = crate::catalog::Catalog::new(&settings.catalog_schema, &generation.query)?;
    next.sink.owned(sql, &catalog).await?.context("new destination ownership missing")?;
    sql.batch_execute(&format!(
        "ALTER TABLE {}.{} RENAME TO {}",
        settings.catalog_schema,
        quote(&generation.sink),
        quote(&settings.sink_table)
    ))
    .await?;
    sql.execute(
        &format!(
            "DELETE FROM {}.pgderive_sink_ownership WHERE query_id=$1",
            settings.catalog_schema
        ),
        &[&generation.query],
    )
    .await?;
    generation.sink.clone_from(&settings.sink_table);
    retarget(sql, settings, &generation.query, &generation.sink, &mut next).await
}
