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
    #[cfg(test)]
    exact_merges: std::sync::Mutex<Option<MergeProbes>>,
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            foreground: Reads::default(),
            maintenance: Reads::default(),
            merges: std::array::from_fn(|_| AtomicU64::new(0)),
            #[cfg(test)]
            exact_merges: std::sync::Mutex::new(None),
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
        #[cfg(test)]
        {
            let mut probes = self
                .exact_merges
                .lock()
                .map_err(|_| anyhow::anyhow!("merge probe lock poisoned"))?;
            if let Some(events) = probes.as_mut() {
                events.overflowed |= events.rows.len() >= 4096;
                anyhow::ensure!(!events.overflowed, "merge probe event limit exceeded");
                events.rows.push(rows);
            }
        }
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

#[cfg(test)]
#[derive(Debug, Default)]
struct MergeProbes {
    rows: Vec<usize>,
    overflowed: bool,
}
#[cfg(test)]
impl Metrics {
    pub(super) fn enable_merge_probes(&self) -> Result<()> {
        let mut probes =
            self.exact_merges.lock().map_err(|_| anyhow::anyhow!("merge probe lock poisoned"))?;
        if probes.is_none() {
            *probes = Some(MergeProbes::default());
        }
        drop(probes);
        Ok(())
    }
    pub(super) fn take_merge_rows(&self) -> Result<Vec<usize>> {
        let mut probes =
            self.exact_merges.lock().map_err(|_| anyhow::anyhow!("merge probe lock poisoned"))?;
        let events = probes.as_mut().ok_or_else(|| anyhow::anyhow!("merge probes not enabled"))?;
        anyhow::ensure!(!events.overflowed, "merge probe event limit exceeded");
        let rows = std::mem::take(&mut events.rows);
        drop(probes);
        Ok(rows)
    }
}
#[cfg(test)]
mod tests;
