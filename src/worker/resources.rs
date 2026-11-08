//! One process capacity domain, with explicit per-tenant admission ceilings.
use crate::{
    Config,
    engine::capacity::{Pool, Query},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{path::Path, sync::Arc};
use tokio::sync::{Semaphore, watch};

/// Shared process retention budgets. These are charges, not an RSS limit.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Maximum concurrently configured queries, including stopped/failed entries.
    pub max_queries: usize,
    /// Maximum configured queries assigned to one explicit tenant.
    pub max_queries_per_tenant: usize,
    /// Aggregate retained cache charges across all query cache families.
    pub cache_bytes: usize,
    /// Maximum cache charges held by any one explicit tenant.
    pub cache_bytes_per_tenant: usize,
    /// Aggregate live immutable memory-run charges, including pinned checkpoints.
    pub memtable_bytes: usize,
    /// Maximum live memory-run charges held by any one tenant.
    pub memtable_bytes_per_tenant: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_queries: 64,
            max_queries_per_tenant: 64,
            cache_bytes: 128 * 1024 * 1024,
            cache_bytes_per_tenant: 128 * 1024 * 1024,
            memtable_bytes: 128 * 1024 * 1024,
            memtable_bytes_per_tenant: 128 * 1024 * 1024,
        }
    }
}
impl Limits {
    /// Load strict process-wide limits from TOML, without tenant-config overlays.
    ///
    /// # Errors
    /// Returns invalid files, unknown fields or inconsistent limits.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let limits: Self = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?
            .try_deserialize()?;
        limits.validate()?;
        Ok(limits)
    }
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.max_queries > 0
                && self.max_queries_per_tenant > 0
                && self.max_queries_per_tenant <= self.max_queries,
            "invalid process query limits"
        );
        Pool::new(self.cache_bytes, self.cache_bytes_per_tenant)?;
        Pool::new(self.memtable_bytes, self.memtable_bytes_per_tenant)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Process {
    pub(crate) cache: Arc<Pool>,
    pub(crate) memtable: Arc<Pool>,
    pub(crate) pressure: std::collections::BTreeMap<String, watch::Sender<u64>>,
    pub(crate) recovery: Arc<Semaphore>,
}
#[derive(Debug)]
pub struct Resources {
    pub(crate) query: Query,
    pub(crate) process: Arc<Process>,
    pub(crate) encoded: Option<Arc<()>>,
    pub(crate) pressure: watch::Sender<u64>,
}
impl Resources {
    pub(crate) fn pressure(&self, tenant_only: bool) {
        let tenant = self.query.memtable.tenant_id();
        self.process.cache.reclaim(tenant_only.then_some(tenant));
        for (id, signal) in &self.process.pressure {
            if !tenant_only || id == tenant {
                signal.send_modify(|value| *value = value.wrapping_add(1));
            }
        }
    }
}

pub(super) fn install(configs: &mut [Config], limits: &Limits) -> Result<()> {
    limits.validate()?;
    ensure!(configs.len() <= limits.max_queries, "process query limit exceeded");
    let mut tenants = std::collections::BTreeMap::new();
    for config in &*configs {
        let tenant = &config.worker.as_ref().context("worker settings missing")?.tenant_id;
        let count = tenants.entry(tenant).or_insert(0_usize);
        *count += 1;
        ensure!(*count <= limits.max_queries_per_tenant, "tenant query limit exceeded");
    }
    let process = Arc::new(Process {
        cache: Pool::new(limits.cache_bytes, limits.cache_bytes_per_tenant)?,
        memtable: Pool::new(limits.memtable_bytes, limits.memtable_bytes_per_tenant)?,
        pressure: tenants.keys().map(|tenant| ((*tenant).clone(), watch::channel(0).0)).collect(),
        recovery: Arc::new(Semaphore::new(1)),
    });
    for config in configs {
        let tenant = &config.worker.as_ref().context("worker settings missing")?.tenant_id;
        let query = Query {
            cache: process.cache.tenant(tenant),
            memtable: process.memtable.tenant(tenant),
        };
        let encoded = if let Some(cache) = &config.encoded_cache {
            let reservation = Arc::new(());
            ensure!(
                query.cache.try_admit(reservation.clone(), cache.memory_bytes)?,
                "encoded cache exceeds process or tenant cache capacity"
            );
            Some(reservation)
        } else {
            None
        };
        config.capacity = Some(Arc::new(Resources {
            query,
            process: process.clone(),
            encoded,
            pressure: process
                .pressure
                .get(tenant)
                .context("tenant pressure channel missing")?
                .clone(),
        }));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
