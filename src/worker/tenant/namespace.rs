//! Immutable namespace claims prevent another catalog/tenant's physical GC.
use crate::{
    Config,
    storage::PageStore,
    worker::{Settings, sql::Session},
};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStoreExt as _, PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

const MARKERS: &str = ".pgderive-ownership";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Owner {
    Branch {
        version: u8,
        prefix: String,
    },
    Catalog {
        version: u8,
        tenant_id: String,
        cluster: String,
        database: u32,
        schema: String,
        catalog_id: String,
        prefix: String,
    },
}

pub(in crate::worker) async fn claim(
    config: &Config,
    settings: &Settings,
    session: &Session,
) -> Result<()> {
    let storage = config.object_store.as_ref().context("worker requires durable object storage")?;
    let identity = crate::source::Identity::inspect(config).await?;
    let database: u32 = session
        .client
        .query_one("SELECT oid FROM pg_database WHERE datname=current_database()", &[])
        .await?
        .try_get(0)?;
    let catalog_id: String = session
        .client
        .query_one(
            &format!(
                "SELECT catalog_id::text FROM {}.pgderive_tenant_owner WHERE singleton",
                settings.catalog_schema
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    let owner = Owner::Catalog {
        version: 1,
        tenant_id: settings.tenant_id.clone(),
        cluster: identity.system,
        database,
        schema: settings.catalog_schema.clone(),
        catalog_id,
        prefix: settings.object_prefix.clone(),
    };
    let base = storage.build("", config.execution.object_requests)?;
    let mut prefix = String::new();
    let parts = settings.object_prefix.split('/').collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(part);
        let digest = format!("{:x}", Sha256::digest(prefix.as_bytes()));
        let marker = Path::from(format!("{MARKERS}/{digest}.json"));
        let expected = if index + 1 == parts.len() {
            &owner
        } else {
            &Owner::Branch { version: 1, prefix: prefix.clone() }
        };
        if matches!(expected, Owner::Catalog { .. })
            && read(base.as_ref(), &marker).await?.is_none()
        {
            let scoped =
                storage.build(&settings.object_prefix, config.execution.object_requests)?;
            let page = scoped.page("", None, 1).await?;
            ensure!(
                page.result.objects.is_empty()
                    || read(base.as_ref(), &marker).await?.as_ref() == Some(expected),
                "unbound object namespace requires a fresh owned prefix"
            );
        }
        claim_marker(base.clone(), marker, expected).await?;
    }
    Ok(())
}

async fn read(store: &dyn PageStore, path: &Path) -> Result<Option<Owner>> {
    match store.get(path).await {
        Ok(result) => {
            ensure!(result.meta.size <= 16 * 1024, "oversized namespace ownership marker");
            Ok(Some(serde_json::from_slice(&result.bytes().await?)?))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn claim_marker(store: Arc<dyn PageStore>, path: Path, expected: &Owner) -> Result<()> {
    if let Some(owner) = read(store.as_ref(), &path).await? {
        ensure!(&owner == expected, "object namespace has another catalog or descendant owner");
        return Ok(());
    }
    let options = PutOptions { mode: PutMode::Create, ..PutOptions::default() };
    match store.put_opts(&path, serde_json::to_vec(expected)?.into(), options).await {
        Ok(_) => Ok(()),
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => {
            ensure!(
                read(store.as_ref(), &path).await?.as_ref() == Some(expected),
                "object namespace ownership race lost"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
