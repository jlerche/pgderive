//! Complete committed source prefixes; scheduling never changes source identity.
pub(super) mod collect;
mod count;
mod model;
mod policy;
use crate::{catalog::Progress, engine::execution::Limits, transaction::Transaction};
use anyhow::{Context, Result, ensure};
pub(super) use collect::{Bounds, Collector};
pub(super) use count::Count;
pub(super) use policy::Controller;
pub use policy::Policy;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(super) struct Epoch {
    transaction: Transaction,
    transactions: u64,
    decode_us: u128,
    opened: Instant,
    source_age: Duration,
}
impl Epoch {
    pub(super) fn new(transaction: Transaction, decode_us: u128) -> Result<Self> {
        Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        let source_age = source_age(transaction.commit_time_micros);
        Ok(Self { transaction, transactions: 1, decode_us, opened: Instant::now(), source_age })
    }
    pub(super) fn push(
        &mut self,
        transaction: Transaction,
        decode_us: u128,
        limits: Limits,
    ) -> Result<()> {
        let prior = self.progress()?;
        let next = Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        ensure!(next.end > prior.end, "epoch source end did not advance");
        ensure!(next.commit >= prior.end, "epoch source order overlaps");
        let bytes =
            self.bytes().checked_add(transaction.bytes).context("epoch byte count overflow")?;
        ensure!(bytes <= limits.output_bytes, "epoch source byte limit exceeded");
        let entries = self
            .transaction
            .batch
            .updates
            .len()
            .checked_add(transaction.batch.updates.len())
            .context("epoch contribution count overflow")?;
        ensure!(entries <= limits.contributions, "epoch contribution limit exceeded");
        self.transactions =
            self.transactions.checked_add(1).context("epoch source count overflow")?;
        self.decode_us = self.decode_us.saturating_add(decode_us);
        self.transaction.bytes = bytes;
        self.transaction.xid = transaction.xid;
        self.transaction.commit_lsn = transaction.commit_lsn;
        self.transaction.end_lsn = transaction.end_lsn;
        self.transaction.commit_time_micros = transaction.commit_time_micros;
        self.transaction.changes.extend(transaction.changes);
        // Each constituent was validated and consolidated by the source decoder.
        // relational::inputs consolidates this full-identity union once, before
        // any nonlinear operator runs; never repeatedly rebuild a growing batch.
        self.transaction.batch.updates.extend(transaction.batch.updates);
        Ok(())
    }
    pub(super) const fn transaction(&self) -> &Transaction {
        &self.transaction
    }
    pub(super) const fn transactions(&self) -> u64 {
        self.transactions
    }
    pub(super) const fn bytes(&self) -> u64 {
        self.transaction.bytes
    }
    pub(super) const fn decode_us(&self) -> u128 {
        self.decode_us
    }
    pub(super) fn age(&self) -> Duration {
        self.source_age.saturating_add(self.opened.elapsed())
    }
    pub(super) fn progress(&self) -> Result<Progress> {
        Progress::new(self.transaction.xid, &self.transaction.commit_lsn, &self.transaction.end_lsn)
    }
    pub(super) fn work(&self) -> f64 {
        let changes = u32::try_from(self.transaction.changes.len()).unwrap_or(u32::MAX);
        let bytes = u32::try_from(self.bytes()).unwrap_or(u32::MAX);
        f64::from(changes).max(f64::from(bytes) / 256.0).max(1.0)
    }
}
fn source_age(commit: Option<i64>) -> Duration {
    let Some(commit) = commit.and_then(|value| u64::try_from(value).ok()) else {
        return Duration::ZERO;
    };
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return Duration::ZERO;
    };
    let committed = Duration::from_hours(262_968).saturating_add(Duration::from_micros(commit));
    now.saturating_sub(committed)
}
#[cfg(test)]
mod tests;
