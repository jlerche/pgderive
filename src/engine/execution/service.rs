//! Physical maintenance service; never a progress, durability or retained-memory budget.
use super::Workspaces;
use serde::Serialize;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const IDLE_QUANTUM: u64 = 128;
const CONTENDED_QUANTUM: u64 = 32;
const BYTE_UNIT: usize = 4096;
tokio::task_local! { static CURRENT: Context; }
struct Context {
    pool: Arc<Workspaces>,
    ledger: Arc<Ledger>,
}
#[derive(Debug, Default)]
pub struct Ledger {
    work_units: AtomicU64,
    work_bytes: AtomicU64,
    yields: AtomicU64,
    requests: AtomicU64,
    read_requested_bytes: AtomicU64,
    read_bytes: AtomicU64,
    write_requested_bytes: AtomicU64,
    written_bytes: AtomicU64,
    overflow_compactions: AtomicU64,
    root_limit_compactions: AtomicU64,
    selected_bytes: AtomicU64,
    selected_runs: AtomicU64,
    oversized_selections: AtomicU64,
    pressure_selections: AtomicU64,
    retained_bytes: AtomicU64,
    referenced_pack_bytes: AtomicU64,
    sparse_pack_slack_bytes: AtomicU64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub work_units: u64,
    pub work_bytes: u64,
    pub yields: u64,
    pub requests: u64,
    pub read_requested_bytes: u64,
    pub read_bytes: u64,
    pub write_requested_bytes: u64,
    pub written_bytes: u64,
    /// Physical selection charge or explicit progress exception count.
    pub overflow_compactions: u64,
    /// Physical selection charge or explicit progress exception count.
    pub root_limit_compactions: u64,
    /// Physical selection charge or explicit progress exception count.
    pub selected_bytes: u64,
    /// Physical selection charge or explicit progress exception count.
    pub selected_runs: u64,
    /// Physical selection charge or explicit progress exception count.
    pub oversized_selections: u64,
    /// Physical selection charge or explicit progress exception count.
    pub pressure_selections: u64,
    pub retained_bytes: u64,
    pub referenced_pack_bytes: u64,
    pub sparse_pack_slack_bytes: u64,
}
impl Ledger {
    pub fn snapshot(&self) -> Report {
        Report {
            work_units: self.work_units.load(Ordering::Relaxed),
            work_bytes: self.work_bytes.load(Ordering::Relaxed),
            yields: self.yields.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            read_requested_bytes: self.read_requested_bytes.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_requested_bytes: self.write_requested_bytes.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
            overflow_compactions: self.overflow_compactions.load(Ordering::Relaxed),
            root_limit_compactions: self.root_limit_compactions.load(Ordering::Relaxed),
            selected_bytes: self.selected_bytes.load(Ordering::Relaxed),
            selected_runs: self.selected_runs.load(Ordering::Relaxed),
            oversized_selections: self.oversized_selections.load(Ordering::Relaxed),
            pressure_selections: self.pressure_selections.load(Ordering::Relaxed),
            retained_bytes: self.retained_bytes.load(Ordering::Relaxed),
            referenced_pack_bytes: self.referenced_pack_bytes.load(Ordering::Relaxed),
            sparse_pack_slack_bytes: self.sparse_pack_slack_bytes.load(Ordering::Relaxed),
        }
    }
    pub fn requested(&self) {
        add(&self.requests, 1);
    }
    pub fn reading(&self, bytes: u64) {
        add(&self.read_requested_bytes, bytes);
    }
    pub fn read(&self, bytes: u64) {
        add(&self.read_bytes, bytes);
    }
    pub fn writing(&self, bytes: u64) {
        add(&self.write_requested_bytes, bytes);
    }
    pub fn written(&self, bytes: u64) {
        add(&self.written_bytes, bytes);
    }
}
fn add(value: &AtomicU64, count: u64) {
    let _prior =
        value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_add(count)));
}
pub fn current() -> Option<Arc<Ledger>> {
    CURRENT.try_with(|c| c.ledger.clone()).ok()
}
pub async fn scope<T>(pool: Arc<Workspaces>, future: impl Future<Output = T>) -> (T, Report) {
    let ledger = Arc::new(Ledger::default());
    let output = CURRENT.scope(Context { pool, ledger: ledger.clone() }, future).await;
    (output, ledger.snapshot())
}
/// Account existing physical byte/operation charges. A contended job receives a
/// positive quantum before yielding; it never waits for foreground demand to end.
/// Indivisible codecs remain bounded by their existing block/index limits.
pub async fn spend(bytes: usize) {
    let yielded = CURRENT
        .try_with(|c| {
            let quantum =
                if c.pool.foreground_waiting() { CONTENDED_QUANTUM } else { IDLE_QUANTUM };
            let units = 1 + u64::try_from(bytes.div_ceil(BYTE_UNIT)).unwrap_or(u64::MAX - 1);
            add(&c.ledger.work_bytes, u64::try_from(bytes).unwrap_or(u64::MAX));
            let before = c.ledger.work_units.load(Ordering::Relaxed);
            add(&c.ledger.work_units, units);
            let yield_now = before / quantum != before.saturating_add(units) / quantum;
            if yield_now {
                add(&c.ledger.yields, 1);
            }
            yield_now
        })
        .unwrap_or(false);
    if yielded {
        tokio::task::yield_now().await;
    }
}
pub fn retained(bytes: u64) {
    if let Some(ledger) = current() {
        add(&ledger.retained_bytes, bytes);
    }
}
/// Observed output-root pack occurrences, not a global physical GC inventory.
/// Shared packs can occur in multiple roots; do not sum this as global debt.
pub fn coverage(physical: u64, live: u64) {
    if let Some(ledger) = current() {
        add(&ledger.referenced_pack_bytes, physical);
        if live < physical.div_ceil(2) {
            add(&ledger.sparse_pack_slack_bytes, physical.saturating_sub(live));
        }
    }
}
#[cfg(test)]
mod tests;

/// Record referenced payload/input bytes and physical fan-in, not transferred I/O.
pub fn selection(bytes: u64, runs: u64, oversized: bool, pressure: bool) {
    let _current = CURRENT.try_with(|c| {
        let ledger = &c.ledger;
        add(&ledger.selected_bytes, bytes);
        add(&ledger.selected_runs, runs);
        add(&ledger.oversized_selections, u64::from(oversized));
        add(&ledger.pressure_selections, u64::from(pressure));
    });
}

/// Count whole-state correctness/capacity escalation outside ordinary selection.
pub fn escalation(overflow: bool) {
    if let Some(ledger) = current() {
        add(
            if overflow { &ledger.overflow_compactions } else { &ledger.root_limit_compactions },
            1,
        );
    }
}
