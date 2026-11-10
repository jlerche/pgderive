//! Immutable namespace claims prevent another catalog/tenant's physical GC.
use crate::storage::PageStore;
use anyhow::{Result, ensure};
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

pub(super) async fn claim(
    storage: &crate::storage::Storage,
    manifest: &super::manifest::Manifest,
    tenant: &str,
    id: &str,
    requests: usize,
) -> Result<()> {
    let prefix_root = format!("{}/{id}", manifest.config.object_prefix);
    let mut session = manifest.pool.acquire().await?;
    let row=session.connection()?.query_one("SELECT s.system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system() s",&[]).await?;
    drop(session);
    let owner = Owner::Catalog {
        version: 1,
        tenant_id: tenant.into(),
        cluster: row.try_get(0)?,
        database: row.try_get(1)?,
        schema: manifest.config.schema.clone(),
        catalog_id: id.into(),
        prefix: prefix_root.clone(),
    };
    let base = storage.build("", requests)?;
    let mut prefix = String::new();
    let parts = prefix_root.split('/').collect::<Vec<_>>();
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
            let scoped = storage.build(&prefix_root, requests)?;
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
