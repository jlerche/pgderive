//! Immutable catalog ownership, separate from connection authorization.
use anyhow::{Context, Result, ensure};
use tokio_postgres::GenericClient;
mod namespace;
pub(super) use namespace::claim as namespace;

pub(super) async fn verify(
    sql: &(impl GenericClient + Sync),
    schema: &str,
    tenant: &str,
) -> Result<bool> {
    let owner = format!("{schema}.pgderive_tenant_owner");
    let exists: bool =
        sql.query_one("SELECT to_regclass($1) IS NOT NULL", &[&owner]).await?.try_get(0)?;
    if !exists {
        let bound: bool = sql.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname LIKE 'pgderive\\_%' ESCAPE '\\')",
            &[&schema],
        ).await?.try_get(0)?;
        ensure!(!bound, "unbound pgderive catalog requires a fresh tenant-owned namespace");
        return Ok(false);
    }
    let stored: String = sql
        .query_opt(&format!("SELECT tenant_id FROM {owner} WHERE singleton"), &[])
        .await?
        .context("catalog tenant owner is missing")?
        .try_get(0)?;
    ensure!(stored == tenant, "catalog belongs to another tenant");
    Ok(true)
}

pub(super) async fn claim(
    sql: &(impl GenericClient + Sync),
    schema: &str,
    tenant: &str,
) -> Result<()> {
    sql.batch_execute("BEGIN").await?;
    let result = async {
        let scope = format!("pgderive-tenant:{schema}");
        sql.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", &[&scope]).await?;
        if !verify(sql, schema, tenant).await? {
            sql.batch_execute(&format!("CREATE TABLE {schema}.pgderive_tenant_owner(singleton boolean PRIMARY KEY CHECK(singleton),tenant_id text NOT NULL,catalog_id uuid NOT NULL DEFAULT pg_catalog.gen_random_uuid())")).await?;
            sql.execute(&format!("INSERT INTO {schema}.pgderive_tenant_owner VALUES(true,$1)"), &[&tenant]).await?;
        }
        Ok(())
    }.await;
    let ending = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
    crate::outcome::combine(
        result,
        sql.batch_execute(ending).await.map_err(Into::into),
        "tenant ownership transaction",
    )
}

/// Disposable persisted cache entries are private to the tenant and catalog.
pub(super) async fn cache_scope(
    sql: &(impl GenericClient + Sync),
    settings: &super::Settings,
) -> Result<String> {
    let catalog: String = sql
        .query_one(
            &format!(
                "SELECT catalog_id::text FROM {}.pgderive_tenant_owner WHERE singleton",
                settings.catalog_schema
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    Ok(serde_json::to_string(&("tenant-catalog-v1", &settings.tenant_id, catalog))?)
}
