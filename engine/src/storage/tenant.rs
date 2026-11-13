//! Tenant-wide virtual objects, bounded physical packing, and source-independent GC.
mod config;
pub use config::Config;
mod batch;
mod gc;
mod manifest;
mod objects;
mod ownership;
mod pool;
mod read;
mod registry;
mod store;
mod upload;
use anyhow::{Context, Result, ensure};
pub use registry::Registry;
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

pub fn checksum(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn namespace(path: &str) -> Result<String> {
    let remainder = path
        .strip_prefix("pgderive/upload-v1/")
        .context("logical objects require an owned upload namespace")?;
    let (id, tail) = remainder.split_once('/').context("logical object lacks path")?;
    let namespace = format!("pgderive/upload-v1/{id}");
    ensure!(
        crate::engine::reader::valid_namespace(&namespace) && !tail.is_empty(),
        "invalid logical upload namespace"
    );
    Ok(namespace)
}
fn error(error: anyhow::Error) -> object_store::Error {
    match error.downcast::<object_store::Error>() {
        Ok(error) => error,
        Err(source) => {
            object_store::Error::Generic { store: "tenant-manifest", source: source.into() }
        }
    }
}
pub async fn build(
    config: &crate::Config,
    settings: &crate::worker::Settings,
    sql: &crate::catalog::Connection,
) -> Result<Arc<dyn super::PageStore>> {
    let row=sql.query_one(&format!("SELECT o.catalog_id::text,json_build_object('cluster',s.system_identifier::text,'database',(SELECT oid FROM pg_database WHERE datname=current_database()),'schema',$1::text) FROM {}.pgderive_tenant_owner o CROSS JOIN pg_control_system() s WHERE singleton AND tenant_id=$2",settings.catalog_schema),&[&settings.catalog_schema,&settings.tenant_id]).await?;
    let id = row.try_get(0)?;
    let identity = row.try_get(1)?;
    Box::pin(config.storage_runtime()?.store(config, settings, &identity, id)).await
}

#[cfg(test)]
mod tests;
