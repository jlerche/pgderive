//! Conservative running credits; weak-backed credits retire at bounded sweeps.
use super::{Allocation, Invariant, Owner, Pool};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(super) struct Entry {
    pub(super) allocation: Weak<Allocation>,
    pub(super) tenant: Arc<str>,
    pub(super) bytes: usize,
}
#[derive(Debug, Default)]
pub(super) struct State {
    pub(super) allocations: BTreeMap<usize, Entry>,
    pub(super) owners: Vec<Owner>,
    pub(super) hand: usize,
    pub(super) peak: usize,
    pub(super) total: usize,
    pub(super) tenants: BTreeMap<Arc<str>, usize>,
    broken: bool,
    last_sweep: Option<Instant>,
}
impl State {
    pub(super) fn check(&self) -> Result<()> {
        if self.broken {
            return Err(Invariant("capacity credit accounting broken").into());
        }
        Ok(())
    }
    pub(super) fn fits(&self, tenant: &str, bytes: usize, pool: &Pool) -> bool {
        self.total.checked_add(bytes).is_some_and(|n| n <= pool.limit)
            && !self.tenant_full(tenant, bytes, pool)
    }
    pub(super) fn tenant_full(&self, tenant: &str, bytes: usize, pool: &Pool) -> bool {
        self.tenants
            .get(tenant)
            .copied()
            .unwrap_or(0)
            .checked_add(bytes)
            .is_none_or(|n| n > pool.tenant_limit)
    }
    pub(super) fn charge(&mut self, tenant: &Arc<str>, bytes: usize) -> Result<()> {
        self.check()?;
        let total = self.total.checked_add(bytes).ok_or(Invariant("capacity credit overflow"))?;
        let charged = self
            .tenants
            .get(tenant)
            .copied()
            .unwrap_or(0)
            .checked_add(bytes)
            .ok_or(Invariant("tenant credit overflow"))?;
        self.total = total;
        self.tenants.insert(tenant.clone(), charged);
        self.peak = self.peak.max(total);
        Ok(())
    }
    fn release(&mut self, tenant: &str, bytes: usize) -> Result<()> {
        let total = self.total.checked_sub(bytes).ok_or(Invariant("capacity credit underflow"))?;
        let charged = self
            .tenants
            .get(tenant)
            .copied()
            .unwrap_or(0)
            .checked_sub(bytes)
            .ok_or(Invariant("tenant credit underflow"))?;
        self.total = total;
        if charged == 0 {
            self.tenants.remove(tenant);
        } else {
            *self.tenants.get_mut(tenant).ok_or(Invariant("tenant credit missing"))? = charged;
        }
        Ok(())
    }
    pub(super) fn sweep(&mut self) -> Result<()> {
        self.check()?;
        let dead = self
            .allocations
            .iter()
            .filter(|(_, entry)| entry.allocation.strong_count() == 0)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in dead {
            if let Some(entry) = self.allocations.remove(&key) {
                self.release(&entry.tenant, entry.bytes).inspect_err(|_| self.broken = true)?;
            }
        }
        self.last_sweep = Some(Instant::now());
        Ok(())
    }
    pub(super) fn release_owned(&mut self, tenant: &str, bytes: usize) {
        if self.release(tenant, bytes).is_err() {
            self.broken = true;
        }
    }
    pub(super) fn observe(&mut self) -> Result<()> {
        self.check()?;
        if self.last_sweep.is_none_or(|at| at.elapsed() >= Duration::from_secs(1)) {
            self.sweep()?;
        }
        Ok(())
    }
}
