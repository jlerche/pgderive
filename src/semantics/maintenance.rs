//! Cooperative database-wide semantic maintenance without hot-path catalog scans.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use tokio_postgres::{Client, GenericClient};
const LOCK: &str = "pgderive-semantic-environment-v1";
const TABLE: &str = "pgderive_environment.revision";

/// Hold a shared session lease before taking any initialization snapshots.
pub async fn lease(sql: &(impl GenericClient + Sync)) -> Result<()> {
    let held: bool = sql
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_lock_shared(pg_catalog.hashtextextended($1,0))",
            &[&LOCK],
        )
        .await?
        .try_get(0)?;
    ensure!(held, "semantic environment maintenance owns the database; retry after finish");
    revision(sql).await?;
    Ok(())
}

pub(super) async fn revision(sql: &(impl GenericClient + Sync)) -> Result<i64> {
    let exists: bool = sql
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&TABLE])
        .await?
        .try_get(0)?;
    if !exists {
        return Ok(0);
    }
    let row = sql
        .query_one("SELECT epoch,pending FROM pgderive_environment.revision WHERE singleton", &[])
        .await?;
    ensure!(
        !row.try_get::<_, bool>(1)?,
        "semantic environment maintenance is pending; finish then explicitly rebuild queries"
    );
    let epoch = row.try_get(0)?;
    ensure!(epoch > 0, "invalid semantic environment revision");
    Ok(epoch)
}

pub async fn change(sql: &mut Client, begin: bool) -> Result<Value> {
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    let exclusive: bool = tx
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended($1,0))",
            &[&LOCK],
        )
        .await?
        .try_get(0)?;
    ensure!(
        exclusive,
        "semantic maintenance requires all workers and validators in this database to stop first"
    );
    if begin {
        tx.batch_execute("CREATE SCHEMA IF NOT EXISTS pgderive_environment; CREATE TABLE IF NOT EXISTS pgderive_environment.revision(singleton boolean PRIMARY KEY CHECK(singleton),epoch bigint NOT NULL CHECK(epoch>0),pending boolean NOT NULL)").await?;
        tx.execute("INSERT INTO pgderive_environment.revision VALUES(true,1,true) ON CONFLICT(singleton) DO UPDATE SET epoch=CASE WHEN pgderive_environment.revision.pending THEN pgderive_environment.revision.epoch ELSE pgderive_environment.revision.epoch+1 END,pending=true", &[]).await?;
    } else {
        tx.execute("UPDATE pgderive_environment.revision SET pending=false WHERE singleton", &[])
            .await?;
    }
    let row = tx
        .query_one("SELECT epoch,pending FROM pgderive_environment.revision WHERE singleton", &[])
        .await?;
    let result = json!({"version":1,"revision":row.try_get::<_,i64>(0)?,"pending":row.try_get::<_,bool>(1)?,"scope":"database","requires":"explicit rebuild of pre-maintenance registrations"});
    tx.commit().await?;
    Ok(result)
}
