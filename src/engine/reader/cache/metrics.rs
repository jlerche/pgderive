//! Read-origin accounting and witnessed compaction sizes, never durable progress.
use anyhow::Result;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative compressed-page read work; cached requests do not issue GETs.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ReadStats {
    /// Requested compressed page bytes, including range-cache hits.
    pub requested_bytes: u64,
    /// Attempted range GETs, including failed or canceled requests.
    pub gets: u64,
    /// Successfully returned range bytes.
    pub fetched_bytes: u64,
}
#[derive(Debug, Default)]
struct Reads {
    requested: AtomicU64,
    gets: AtomicU64,
    fetched: AtomicU64,
}
impl Reads {
    fn snapshot(&self) -> ReadStats {
        ReadStats {
            requested_bytes: self.requested.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            fetched_bytes: self.fetched.load(Ordering::Relaxed),
        }
    }
}
#[derive(Debug)]
pub(in crate::engine::reader) struct Metrics {
    foreground: Reads,
    maintenance: Reads,
    merges: [AtomicU64; 64],
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            foreground: Reads::default(),
            maintenance: Reads::default(),
            merges: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}
impl Metrics {
    const fn reads(&self, scan: bool) -> &Reads {
        if scan { &self.maintenance } else { &self.foreground }
    }
    pub(in crate::engine::reader) fn requested(&self, scan: bool, bytes: u64) {
        self.reads(scan).requested.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(in crate::engine::reader) fn fetching(&self, scan: bool) {
        self.reads(scan).gets.fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::engine::reader) fn fetched(&self, scan: bool, bytes: u64) {
        self.reads(scan).fetched.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(super) fn merged(&self, rows: usize) -> Result<()> {
        self.merges[usize::try_from(rows.max(1).ilog2())?].fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    pub(in crate::engine::reader) fn total_io(&self) -> ReadStats {
        let foreground = self.foreground.snapshot();
        let maintenance = self.maintenance.snapshot();
        ReadStats {
            requested_bytes: foreground.requested_bytes.saturating_add(maintenance.requested_bytes),
            gets: foreground.gets.saturating_add(maintenance.gets),
            fetched_bytes: foreground.fetched_bytes.saturating_add(maintenance.fetched_bytes),
        }
    }
    pub(in crate::engine::reader) fn snapshot(&self) -> (ReadStats, ReadStats, Vec<u64>) {
        (
            self.foreground.snapshot(),
            self.maintenance.snapshot(),
            self.merges.iter().map(|n| n.load(Ordering::Relaxed)).collect(),
        )
    }
}
