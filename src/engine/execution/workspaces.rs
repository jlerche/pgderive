//! Shared active-phase admission; permits never charge retained cursor pages.
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Query-wide concurrency budget shared by foreground and maintenance phases.
/// This bounds active operations, not bytes retained by their completed outputs.
#[derive(Debug)]
pub struct Workspaces {
    slots: Arc<Semaphore>,
    background: Arc<Semaphore>,
    capacity: usize,
    peak: AtomicUsize,
    waits: AtomicU64,
    background_waits: AtomicU64,
    foreground: AtomicUsize,
}
/// Current admission usage; independent of cache retention and process RSS.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct WorkspaceStats {
    /// Maximum simultaneous active phases across the query and its collector.
    pub capacity: usize,
    /// Currently held permits; queued operations do not own a workspace.
    pub active: usize,
    /// Highest observed simultaneous permit ownership.
    pub peak: usize,
    /// Acquisitions that encountered a fully occupied pool.
    pub waits: u64,
    /// Maintenance quota, leaving foreground headroom unless capacity is one.
    pub background_capacity: usize,
    /// Held maintenance quota, including callers waiting for global admission.
    pub background_active: usize,
    /// Acquisitions that encountered a fully occupied maintenance quota.
    pub background_waits: u64,
    /// Admitted epochs and foreground phases active or waiting for admission.
    pub foreground_demand: usize,
}
impl Workspaces {
    /// Create one pool. Share its Arc rather than constructing a pool per task.
    ///
    /// # Errors
    /// Rejects zero or excessive parallelism.
    pub fn new(capacity: usize) -> Result<Self> {
        ensure!((1..=256).contains(&capacity), "invalid shared workspace slot limit");
        Ok(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            background: Arc::new(Semaphore::new(capacity.saturating_sub(1).max(1))),
            capacity,
            peak: AtomicUsize::new(0),
            waits: AtomicU64::new(0),
            background_waits: AtomicU64::new(0),
            foreground: AtomicUsize::new(0),
        })
    }
    /// Maximum local polling width; actual admission is shared by every caller.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
    /// Reserve one complete active phase before allocating its private workspace.
    /// Never call while already holding a permit from this pool.
    ///
    /// # Errors
    /// Returns an error if the pool unexpectedly closes.
    pub async fn acquire(self: &Arc<Self>) -> Result<WorkspacePermit> {
        let demand = self.demand();
        let (global, waited) = self.reserve().await?;
        Ok(WorkspacePermit { _global: global, _background: None, _demand: Some(demand), waited })
    }
    async fn reserve(&self) -> Result<(OwnedSemaphorePermit, std::time::Duration)> {
        let (permit, waited) = reserve(&self.slots, &self.waits).await?;
        self.peak.fetch_max(self.capacity - self.slots.available_permits(), Ordering::Relaxed);
        Ok((permit, waited))
    }
    /// Admit maintenance with foreground headroom. The shared FIFO queue prevents
    /// new foreground arrivals from overtaking queued maintenance. Never nest
    /// admission on the same pool; request and execution pools are distinct.
    ///
    /// # Errors
    /// Returns an error if an admission semaphore unexpectedly closes.
    pub async fn background(self: &Arc<Self>) -> Result<WorkspacePermit> {
        let (background, first) = reserve(&self.background, &self.background_waits).await?;
        let (global, second) = self.reserve().await?;
        Ok(WorkspacePermit {
            _global: global,
            _background: Some(background),
            _demand: None,
            waited: first.saturating_add(second),
        })
    }
    /// Mark latency-sensitive work without reserving an execution workspace.
    pub(crate) fn demand(self: &Arc<Self>) -> Demand {
        self.foreground.fetch_add(1, Ordering::Relaxed);
        Demand(self.clone())
    }
    pub(crate) fn has_foreground_demand(&self) -> bool {
        self.foreground.load(Ordering::Relaxed) != 0
    }
    /// Snapshot admission counters without taking ownership of any resource.
    #[must_use]
    pub fn stats(&self) -> WorkspaceStats {
        WorkspaceStats {
            capacity: self.capacity,
            active: self.capacity - self.slots.available_permits(),
            peak: self.peak.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            background_capacity: self.capacity.saturating_sub(1).max(1),
            background_active: self.capacity.saturating_sub(1).max(1)
                - self.background.available_permits(),
            background_waits: self.background_waits.load(Ordering::Relaxed),
            foreground_demand: self.foreground.load(Ordering::Relaxed),
        }
    }
}
async fn reserve(
    slots: &Arc<Semaphore>,
    waits: &AtomicU64,
) -> Result<(OwnedSemaphorePermit, std::time::Duration)> {
    match slots.clone().try_acquire_owned() {
        Ok(permit) => Ok((permit, std::time::Duration::ZERO)),
        Err(TryAcquireError::NoPermits) => {
            let _prior = waits.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_add(1))
            });
            let started = tokio::time::Instant::now();
            let permit =
                slots.clone().acquire_owned().await.context("shared workspace pool closed")?;
            Ok((permit, started.elapsed()))
        }
        Err(TryAcquireError::Closed) => anyhow::bail!("shared workspace pool closed"),
    }
}
#[cfg(test)]
mod tests;

/// Owns active admission and releases foreground demand on completion or cancellation.
pub struct WorkspacePermit {
    _global: OwnedSemaphorePermit,
    _background: Option<OwnedSemaphorePermit>,
    _demand: Option<Demand>,
    waited: std::time::Duration,
}
pub struct Demand(Arc<Workspaces>);
impl Drop for Demand {
    fn drop(&mut self) {
        self.0.foreground.fetch_sub(1, Ordering::Relaxed);
    }
}

impl WorkspacePermit {
    pub(crate) const fn waited(&self) -> std::time::Duration {
        self.waited
    }
}
