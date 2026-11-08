//! Reject conflicting ownership before any task can mutate `PostgreSQL` or S3.
use crate::Config;
use anyhow::{Context, Result, ensure};
use std::path::{Component, Path, PathBuf};

pub(super) fn validate(configs: &[Config]) -> Result<()> {
    let paths = configs
        .iter()
        .map(|config| {
            config.encoded_cache.as_ref().map(|cache| directory(&cache.directory)).transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    for (index, config) in configs.iter().enumerate() {
        let left = config.worker.as_ref().context("configuration requires a worker section")?;
        let storage =
            config.object_store.as_ref().context("worker requires durable object storage")?;
        for (other_index, other) in configs.iter().enumerate().take(index) {
            let right = other.worker.as_ref().context("configuration requires a worker section")?;
            let same_database = config.postgres.host.eq_ignore_ascii_case(&other.postgres.host)
                && config.postgres.port == other.postgres.port
                && config.postgres.database == other.postgres.database;
            let same_catalog = same_database && left.catalog_schema == right.catalog_schema;
            if same_catalog {
                ensure!(left.tenant_id == right.tenant_id, "catalog assigned to multiple tenants");
                ensure!(left.query_id != right.query_id, "duplicate logical query ownership");
                ensure!(left.sink_table != right.sink_table, "duplicate result table ownership");
                ensure!(
                    left.object_prefix == right.object_prefix
                        && storage.same_namespace(
                            other.object_store.as_ref().context("object store missing")?
                        )?,
                    "one catalog requires one object namespace"
                );
            }
            if storage
                .same_namespace(other.object_store.as_ref().context("object store missing")?)?
                && overlaps(&left.object_prefix, &right.object_prefix)
            {
                ensure!(
                    same_catalog
                        && left.tenant_id == right.tenant_id
                        && left.object_prefix == right.object_prefix,
                    "overlapping object namespaces have different catalog owners"
                );
            }
            if let (Some(left), Some(right)) = (&paths[index], &paths[other_index]) {
                ensure!(
                    !left.starts_with(right) && !right.starts_with(left),
                    "encoded cache directories overlap"
                );
            }
        }
    }
    Ok(())
}

fn overlaps(left: &str, right: &str) -> bool {
    left == right
        || left.strip_prefix(right).is_some_and(|tail| tail.starts_with('/'))
        || right.strip_prefix(left).is_some_and(|tail| tail.starts_with('/'))
}

fn directory(path: &Path) -> Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            component => {
                resolved.push(component.as_os_str());
                match std::fs::symlink_metadata(&resolved) {
                    Ok(_) => resolved = std::fs::canonicalize(&resolved)?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests;
