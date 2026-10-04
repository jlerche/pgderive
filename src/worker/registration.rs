use super::{program, spec::Settings};
use crate::{
    Config,
    catalog::Sink,
    source::{Contract, Identity},
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

// The logical configured slot name is an alias. A durably journaled random
// physical name lets restart discard only this initialization's expired snapshot
// slot, never an unrelated pre-existing slot with the configured name.
pub(super) async fn source(
    sql: &mut Client,
    config: &Config,
    settings: &Settings,
) -> Result<Contract> {
    sql.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {}.pgderive_worker_registration(
        query_id text PRIMARY KEY, format_version integer NOT NULL CHECK(format_version=1),
        logical_slot text NOT NULL UNIQUE, slot_name text NOT NULL UNIQUE,
        source jsonb NOT NULL, sink jsonb NOT NULL, plan_identity text NOT NULL,
        object_prefix text NOT NULL)",
        settings.catalog_schema
    ))
    .await?;
    let identity = Identity::inspect(config).await?;
    let row=sql.query_opt(&format!("SELECT format_version,logical_slot,slot_name,source,sink,plan_identity,object_prefix FROM {}.pgderive_worker_registration WHERE query_id=$1",settings.catalog_schema),&[&settings.query_id]).await?;
    if let Some(row) = row {
        ensure!(
            row.try_get::<_, i32>(0)? == 1
                && row.try_get::<_, String>(1)? == config.replication.slot
                && row.try_get::<_, String>(6)? == settings.object_prefix,
            "worker registration alias/object namespace changed"
        );
        let source: Contract = serde_json::from_value(row.try_get(3)?)?;
        ensure!(
            source.slot == row.try_get::<_, String>(2)?
                && source.identity == identity
                && source.publication == config.replication.publication,
            "worker registration source identity changed"
        );
        ensure!(
            row.try_get::<_, serde_json::Value>(4)?
                == serde_json::to_value(Sink::Grouped(settings.sink_table.clone()))?,
            "worker registration destination changed"
        );
        source.verify(sql).await?;
        ensure!(
            program::plan(&source, &settings.query)?.identity() == row.try_get::<_, String>(5)?,
            "worker registration query changed"
        );
        return Ok(source);
    }
    create(sql, config, settings, identity).await
}
async fn create(
    sql: &mut Client,
    config: &Config,
    settings: &Settings,
    identity: Identity,
) -> Result<Contract> {
    let occupied:bool=sql.query_one(&format!("SELECT EXISTS(SELECT 1 FROM {}.pgderive_queries WHERE query_id=$1) OR EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$2)",settings.catalog_schema),&[&settings.query_id,&config.replication.slot]).await?.try_get(0)?;
    ensure!(!occupied, "new worker cannot adopt an existing unjournaled query or slot");
    let nonce: String = sql
        .query_one("SELECT replace(pg_catalog.gen_random_uuid()::text,'-','')", &[])
        .await?
        .try_get(0)?;
    let prefix = &config.replication.slot[..config.replication.slot.len().min(30)];
    let slot = format!("{prefix}_{nonce}");
    let source = Contract::inspect(sql, identity, &config.replication.publication, &slot).await?;
    settings.query.validate(&source)?;
    let plan = program::plan(&source, &settings.query)?;
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    tx.execute(&format!("INSERT INTO {}.pgderive_worker_registration(query_id,format_version,logical_slot,slot_name,source,sink,plan_identity,object_prefix) VALUES($1,1,$2,$3,$4,$5,$6,$7)",settings.catalog_schema),&[&settings.query_id,&config.replication.slot,&source.slot,&serde_json::to_value(&source)?,&serde_json::to_value(Sink::Grouped(settings.sink_table.clone()))?,&plan.identity(),&settings.object_prefix]).await?;
    tx.commit().await.context("worker registration COMMIT requires authoritative reload")?;
    Ok(source)
}

pub(super) async fn reset_unactivated(sql: &Client, source: &Contract) -> Result<()> {
    // Called only after the query is proven unactivated, under exclusive worker
    // ownership. This exact unpredictable name was recorded before CREATE SLOT.
    source.verify(sql).await?;
    if let Some(row)=sql.query_opt("SELECT plugin,database,temporary,active FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",&[&source.slot]).await? {
        ensure!(row.try_get::<_,String>(0)?=="pgoutput" && row.try_get::<_,String>(1)?==source.identity.database && !row.try_get::<_,bool>(2)? && !row.try_get::<_,bool>(3)?,"unactivated owned slot is incompatible or still active");
        sql.query_one("SELECT pg_catalog.pg_drop_replication_slot($1)",&[&source.slot]).await?;
    }
    Ok(())
}
