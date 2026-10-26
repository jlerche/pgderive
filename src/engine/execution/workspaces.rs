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
    pub async fn acquire(self: &Arc<Self>) -> Result<OwnedSemaphorePermit> {
        let permit = reserve(&self.slots, &self.waits).await?;
        self.peak.fetch_max(self.capacity - self.slots.available_permits(), Ordering::Relaxed);
        Ok(permit)
    }
    /// Admit maintenance without occupying the final foreground slot when the
    /// capacity is greater than one. Never nest with this pool's other permits.
    ///
    /// # Errors
    /// Returns an error if an admission semaphore unexpectedly closes.
    pub async fn background(
        self: &Arc<Self>,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let background = reserve(&self.background, &self.background_waits).await?;
        Ok((self.acquire().await?, background))
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
        }
    }
}
async fn reserve(slots: &Arc<Semaphore>, waits: &AtomicU64) -> Result<OwnedSemaphorePermit> {
    match slots.clone().try_acquire_owned() {
        Ok(permit) => Ok(permit),
        Err(TryAcquireError::NoPermits) => {
            let _prior = waits.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_add(1))
            });
            slots.clone().acquire_owned().await.context("shared workspace pool closed")
        }
        Err(TryAcquireError::Closed) => anyhow::bail!("shared workspace pool closed"),
    }
}
#[cfg(test)]
mod tests;
