//! Shared capacity accounting without sharing tenant data or logical state.
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Owner = (Arc<str>, Weak<dyn Reclaim>);
type Allocation = dyn Any + Send + Sync;
pub(crate) type ChargedAllocation = (Arc<Allocation>, usize);
type Requested = BTreeMap<usize, ChargedAllocation>;

/// Broken shared accounting requires draining the entire process.
#[derive(Debug)]
pub(crate) struct Invariant(pub(crate) &'static str);
impl std::fmt::Display for Invariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Invariant {}

/// Admission pressure may be reclaimed by checkpointing existing memory first.
#[derive(Debug)]
pub(crate) struct MemoryPressure {
    pub(crate) required: usize,
    pub(crate) limit: usize,
    pub(crate) tenant_only: bool,
}
impl std::fmt::Display for MemoryPressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shared memtable capacity requires {} bytes (ceiling {})",
            self.required, self.limit
        )
    }
}
impl std::error::Error for MemoryPressure {}

/// A cache can discard unpinned retained data when another query needs capacity.
pub(crate) trait Reclaim: Send + Sync + std::fmt::Debug {
    fn reclaim(&self);
}

mod accounting;
use accounting::{Entry, State};

/// Process-wide charged-allocation limit with a per-tenant ceiling.
#[derive(Debug)]
pub struct Pool {
    limit: usize,
    tenant_limit: usize,
    state: Mutex<State>,
}

/// Current live allocation charges; not an allocator or RSS measurement.
#[derive(Debug, Serialize)]
pub struct Stats {
    /// Process-wide byte ceiling.
    pub limit: usize,
    /// Per-tenant ceiling within the shared process budget.
    pub tenant_limit: usize,
    /// Current charges, including allocations pinned outside their cache.
    pub used: usize,
    /// Highest reserved charge, including weak-backed credits awaiting a sweep.
    pub peak: usize,
    /// Current live charges by explicit tenant ID.
    pub tenants: BTreeMap<String, usize>,
}

/// Cheap diagnostic credits; weak-backed retirement is sampled at most once a second.
#[derive(Serialize)]
pub(crate) struct Observation {
    limit: usize,
    tenant_limit: usize,
    used: usize,
    peak: usize,
    tenant_used: usize,
}

/// An explicit tenant's access to a shared capacity pool.
#[derive(Clone, Debug)]
pub struct Admission {
    pool: Arc<Pool>,
    tenant: Arc<str>,
}

/// Tenant-bound cache and memtable capacity for one independently owned query.
#[derive(Clone, Debug)]
pub struct Query {
    /// Shared retained-cache allocation accounting.
    pub cache: Admission,
    /// Shared live immutable memory-run accounting.
    pub memtable: Admission,
}

impl Pool {
    /// Construct a charged-allocation pool. A tenant may borrow unused capacity
    /// up to its configured ceiling; capacity is not reserved in advance.
    ///
    /// # Errors
    /// Rejects zero or inconsistent ceilings.
    pub fn new(limit: usize, tenant_limit: usize) -> Result<Arc<Self>> {
        ensure!(
            limit > 0 && tenant_limit > 0 && tenant_limit <= limit,
            "invalid shared capacity limits"
        );
        Ok(Arc::new(Self { limit, tenant_limit, state: Mutex::default() }))
    }

    /// Bind accounting to an explicit tenant; this does not authorize access.
    #[must_use]
    pub fn tenant(self: &Arc<Self>, tenant: &str) -> Admission {
        Admission { pool: self.clone(), tenant: tenant.into() }
    }

    /// Sweep dead allocations and report actual admitted live charges.
    ///
    /// # Errors
    /// Returns a poisoned accounting lock.
    pub fn stats(&self) -> Result<Stats> {
        let mut state = self.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
        state.sweep()?;
        let tenants =
            state.tenants.iter().map(|(tenant, bytes)| (tenant.to_string(), *bytes)).collect();
        let used = state.total;
        Ok(Stats {
            limit: self.limit,
            tenant_limit: self.tenant_limit,
            used,
            peak: state.peak,
            tenants,
        })
    }
    pub(crate) fn observation(&self, tenant: &str) -> Result<Observation> {
        let mut state = self.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
        state.observe()?;
        Ok(Observation {
            limit: self.limit,
            tenant_limit: self.tenant_limit,
            used: state.total,
            peak: state.peak,
            tenant_used: state.tenants.get(tenant).copied().unwrap_or(0),
        })
    }
    pub(crate) fn reclaim(&self, tenant: Option<&str>) {
        // Reclamation is best effort; admission still fails closed on a poisoned lock.
        let Ok(state) = self.state.try_lock() else { return };
        let owners = state
            .owners
            .iter()
            .filter(|(id, _)| tenant.is_none_or(|tenant| id.as_ref() == tenant))
            .filter_map(|(_, owner)| owner.upgrade())
            .collect::<Vec<_>>();
        drop(state);
        for owner in owners {
            owner.reclaim();
        }
    }
}

impl Admission {
    pub(crate) fn reserve(&self, bytes: usize) -> Result<bool> {
        let mut state = self.pool.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
        state.check()?;
        // Optional native retention never scans unrelated weak allocations.
        // Pressure/reporting paths retire those credits; canonical decoding is safe meanwhile.
        if !state.fits(&self.tenant, bytes, &self.pool) {
            return Ok(false);
        }
        state.charge(&self.tenant, bytes)?;
        drop(state);
        Ok(true)
    }
    pub(crate) fn release(&self, bytes: usize) {
        if let Ok(mut state) = self.pool.state.lock() {
            state.release_owned(&self.tenant, bytes);
        }
    }
    pub(crate) fn tenant_id(&self) -> &str {
        &self.tenant
    }
    pub(crate) fn admit_memory(&self, allocations: Vec<ChargedAllocation>) -> Result<()> {
        let requested = requests(allocations);
        let required = requested.values().try_fold(0_usize, |sum, (_, bytes)| {
            sum.checked_add(*bytes).context("candidate capacity overflow")
        })?;
        let limit = self.pool.limit.min(self.pool.tenant_limit);
        if required > limit {
            return Err(MemoryPressure { required, limit, tenant_only: true }.into());
        }
        if let Some(tenant_only) = self.admit_batch(&requested)? {
            return Err(MemoryPressure { required, limit, tenant_only }.into());
        }
        Ok(())
    }
    pub(crate) fn register(&self, owner: &Arc<dyn Reclaim>) -> Result<()> {
        let mut state = self.pool.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
        state.check()?;
        state.owners.retain(|(_, owner)| owner.strong_count() > 0);
        state.owners.push((self.tenant.clone(), Arc::downgrade(owner)));
        drop(state);
        Ok(())
    }

    pub(crate) fn try_admit(&self, allocation: Arc<Allocation>, bytes: usize) -> Result<bool> {
        Ok(self.admit_batch(&requests(vec![(allocation, bytes)]))?.is_none())
    }
    fn admit_batch(&self, requested: &Requested) -> Result<Option<bool>> {
        let mut state = self.pool.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
        state.check()?;
        let extra = extra(&state, requested, &self.tenant)?;
        if !state.fits(&self.tenant, extra, &self.pool) {
            state.sweep()?;
            if !state.fits(&self.tenant, extra, &self.pool) {
                return Ok(Some(state.tenant_full(&self.tenant, extra, &self.pool)));
            }
        }
        state.charge(&self.tenant, extra)?;
        for (key, (allocation, bytes)) in requested {
            let charge = (*bytes).max(state.allocations.get(key).map_or(0, |entry| entry.bytes));
            state.allocations.insert(
                *key,
                Entry {
                    allocation: Arc::downgrade(allocation),
                    tenant: self.tenant.clone(),
                    bytes: charge,
                },
            );
        }
        drop(state);
        Ok(None)
    }
    pub(crate) fn retain(&self, allocation: Arc<Allocation>, bytes: usize) -> Result<bool> {
        if self.try_admit(allocation.clone(), bytes)? {
            return Ok(true);
        }
        let usage = self.pool.stats()?;
        let tenant_pressure =
            usage.tenants.get(self.tenant.as_ref()).copied().unwrap_or(0).saturating_add(bytes)
                > usage.tenant_limit;
        let owners = {
            let mut state =
                self.pool.state.lock().map_err(|_| Invariant("capacity lock poisoned"))?;
            state.owners.retain(|(_, owner)| owner.strong_count() > 0);
            let len = state.owners.len();
            let offset = state.hand.min(len);
            state.owners.rotate_left(offset);
            state.hand = usize::from(len > 1);
            state
                .owners
                .iter()
                .filter(|(tenant, _)| !tenant_pressure || tenant == &self.tenant)
                .filter_map(|(_, owner)| owner.upgrade())
                .collect::<Vec<_>>()
        };
        for owner in owners {
            owner.reclaim();
            if self.try_admit(allocation.clone(), bytes)? {
                return Ok(true);
            }
        }
        drop(allocation);
        Ok(false)
    }
}

fn extra(state: &State, requested: &Requested, tenant: &Arc<str>) -> Result<usize> {
    requested.iter().try_fold(0_usize, |sum, (key, (_, bytes))| {
        let prior = state.allocations.get(key);
        if prior.is_some_and(|entry| &entry.tenant != tenant) {
            return Err(Invariant("allocation belongs to another tenant").into());
        }
        sum.checked_add(bytes.saturating_sub(prior.map_or(0, |entry| entry.bytes)))
            .context("capacity admission overflow")
    })
}

fn requests(allocations: Vec<ChargedAllocation>) -> Requested {
    let mut requested = Requested::new();
    for (allocation, bytes) in allocations {
        let key = Arc::as_ptr(&allocation).cast::<()>().addr();
        let entry = requested.entry(key).or_insert((allocation, bytes));
        entry.1 = entry.1.max(bytes);
    }
    requested
}

#[cfg(test)]
mod tests;
