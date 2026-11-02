use super::{Controller, Epoch, Policy, policy::Decision};
use crate::{acknowledged::Stream, engine::execution::Limits, transaction::Transaction};
use anyhow::{Context, Result};
use futures_util::FutureExt;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::Instant;

pub(in crate::worker) struct Input {
    pub(in crate::worker) transaction: Transaction,
    pub(in crate::worker) decode_us: u128,
}
pub(in crate::worker) struct Collector<C = WallTime> {
    held: Option<Input>,
    blocked: Option<Box<Blocked>>,
    retry: Retry,
    clock: C,
    plan: std::sync::Arc<super::features::Keys>,
}
/// A blocked epoch held several transactions; replay them singly to publish the valid prefix.
#[derive(Debug)]
pub(in crate::worker) struct NarrowEpochs;
impl std::fmt::Display for NarrowEpochs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("replaying a blocked epoch one transaction at a time")
    }
}
impl std::error::Error for NarrowEpochs {}
struct Blocked {
    epoch: Epoch,
    error: anyhow::Error,
    retry_at: Instant,
    merged: bool,
}
#[derive(Default)]
pub(in crate::worker) struct Retry {
    failures: u32,
    origin: Option<Origin>,
}
#[derive(Clone, serde::Serialize)]
pub(in crate::worker) struct Origin {
    end_lsn: String,
    relations: std::collections::BTreeSet<String>,
}
const FIRST_RETRY: Duration = Duration::from_millis(100);
const LAST_RETRY: Duration = Duration::from_secs(30);
const CEILING: crate::engine::execution::CapacityExceeded =
    crate::engine::execution::CapacityExceeded(
        "blocked epoch reached its resource ceiling; repair the row and rebuild, or raise the epoch limits",
    );
/// Wall time converts `PostgreSQL` commit timestamps to age; monotonic waits use Tokio.
pub(in crate::worker) trait Clock: Send + Sync {
    fn wall(&self) -> SystemTime;
}
#[derive(Default)]
pub(in crate::worker) struct WallTime;
impl Clock for WallTime {
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
}
/// Complete committed inputs. Adapters must preserve cancellation-safe reception.
pub(in crate::worker) trait InputStream: Send {
    fn input(&mut self) -> impl std::future::Future<Output = Result<Input>> + Send;
}
impl InputStream for Stream {
    async fn input(&mut self) -> Result<Input> {
        let transaction = self.recv().await?;
        Ok(Input { transaction, decode_us: self.decode_us() })
    }
}
pub(in crate::worker) struct Bounds<'a> {
    pub(in crate::worker) limits: Limits,
    pub(in crate::worker) policy: &'a Policy,
    pub(in crate::worker) remaining: u64,
    pub(in crate::worker) source_changes: usize,
    pub(in crate::worker) single: bool,
    pub(in crate::worker) stop: &'a mut watch::Receiver<bool>,
}
pub(in crate::worker) struct Collected {
    pub(in crate::worker) epoch: Epoch,
    pub(in crate::worker) reason: &'static str,
    pub(in crate::worker) predicted: Duration,
}
impl Collector {
    pub(in crate::worker) fn new(plan: std::sync::Arc<super::features::Keys>) -> Self {
        Self { held: None, blocked: None, retry: Retry::default(), clock: WallTime, plan }
    }
}
impl<C: Clock> Collector<C> {
    /// Hold a failed epoch until later input merges into it and its backoff passes.
    pub(in crate::worker) fn block(&mut self, epoch: Epoch, error: anyhow::Error) -> Origin {
        let origin = self
            .retry
            .origin
            .get_or_insert_with(|| Origin {
                end_lsn: epoch.transaction().end_lsn.clone(),
                relations: epoch
                    .transaction()
                    .batch
                    .updates
                    .iter()
                    .map(|update| format!("{}.{}", update.tuple.schema, update.tuple.table))
                    .collect(),
            })
            .clone();
        let doubled =
            FIRST_RETRY.saturating_mul(1_u32.checked_shl(self.retry.failures).unwrap_or(u32::MAX));
        self.retry.failures = self.retry.failures.saturating_add(1);
        let retry_at = Instant::now() + doubled.min(LAST_RETRY);
        self.blocked = Some(Box::new(Blocked { epoch, error, retry_at, merged: false }));
        origin
    }
    pub(in crate::worker) const fn blocked(&self) -> bool {
        self.blocked.is_some()
    }
    pub(in crate::worker) fn published(&mut self) {
        self.retry = Retry::default();
    }
    /// When to retry input already merged into the blocked epoch; unchanged input cannot pass.
    pub(in crate::worker) fn retry_at(&self) -> Option<Instant> {
        self.blocked.as_ref().filter(|blocked| blocked.merged).map(|blocked| blocked.retry_at)
    }
    /// Merge input while the backoff runs, or return it for an immediate attempt.
    pub(in crate::worker) fn absorb(
        &mut self,
        input: Input,
        scheduler: &mut Controller,
        bounds: &Bounds<'_>,
    ) -> Result<Option<Input>> {
        if self.blocked.as_ref().is_none_or(|blocked| Instant::now() >= blocked.retry_at) {
            return Ok(Some(input));
        }
        scheduler.observe(input.transaction.commit_time_micros);
        self.blocked = Some(self.merge(input, bounds)?);
        Ok(None)
    }
    fn merge(&mut self, input: Input, bounds: &Bounds<'_>) -> Result<Box<Blocked>> {
        let mut blocked = self.blocked.take().context("no blocked epoch to merge into")?;
        if !fits(&blocked.epoch, &input.transaction, bounds) {
            return Err(blocked.error.context(CEILING));
        }
        blocked.epoch.push(input.transaction, input.decode_us, bounds.limits)?;
        blocked.merged = true;
        Ok(blocked)
    }
    fn open(&mut self, first: Option<Input>, bounds: &Bounds<'_>) -> Result<Epoch> {
        match (first, self.blocked.is_some()) {
            (Some(first), false) => Epoch::at(
                first.transaction,
                first.decode_us,
                Instant::now(),
                self.clock.wall(),
                self.plan.clone(),
            ),
            (Some(first), true) => Ok(self.merge(first, bounds)?.epoch),
            (None, _) => Ok(self.blocked.take().context("no blocked epoch to retry")?.epoch),
        }
    }
    pub(in crate::worker) async fn recv(&mut self, stream: &mut impl InputStream) -> Result<Input> {
        if let Some(input) = self.held.take() {
            return Ok(input);
        }
        stream.input().await
    }
    pub(in crate::worker) async fn gather(
        &mut self,
        first: Option<Input>,
        stream: &mut impl InputStream,
        scheduler: &mut Controller,
        bounds: Bounds<'_>,
    ) -> Result<Collected> {
        if let Some(first) = &first {
            scheduler.observe(first.transaction.commit_time_micros);
        }
        let mut epoch = self.open(first, &bounds)?;
        loop {
            let decision = scheduler.decision(&epoch);
            let reason = ceiling(&epoch, &bounds)
                .or_else(|| (decision.reason == "bounded_exploration").then_some(decision.reason));
            if let Some(reason) = reason {
                return Ok(Collected { epoch, reason, predicted: decision.predicted });
            }
            // Missing a freshness objective forbids intentional waiting; it must
            // not prevent coalescing work that is already queued. Otherwise fixed
            // cost or prediction uncertainty can force single-transaction epochs
            // and amplify publication/checkpoint stalls under backlog.
            let Some(input) = receive(stream, bounds.stop, decision).await? else {
                return Ok(Collected {
                    epoch,
                    reason: decision.reason,
                    predicted: decision.predicted,
                });
            };
            if !fits(&epoch, &input.transaction, &bounds) {
                self.held = Some(input);
                return Ok(Collected {
                    epoch,
                    reason: "resource_ceiling",
                    predicted: decision.predicted,
                });
            }
            scheduler.observe(input.transaction.commit_time_micros);
            epoch.push(input.transaction, input.decode_us, bounds.limits)?;
        }
    }
}
fn ceiling(epoch: &Epoch, bounds: &Bounds<'_>) -> Option<&'static str> {
    if !bounds.policy.enabled || bounds.single {
        Some("single_transaction")
    } else if epoch.transactions() >= bounds.remaining.min(bounds.policy.max_transactions) {
        Some("transaction_ceiling")
    } else if epoch.bytes() >= bounds.policy.max_bytes.min(bounds.limits.output_bytes) {
        Some("byte_ceiling")
    } else if *bounds.stop.borrow() {
        Some("shutdown")
    } else {
        None
    }
}
fn fits(epoch: &Epoch, next: &Transaction, bounds: &Bounds<'_>) -> bool {
    epoch
        .bytes()
        .checked_add(next.bytes)
        .is_some_and(|n| n <= bounds.policy.max_bytes.min(bounds.limits.output_bytes))
        && epoch
            .transaction()
            .changes
            .len()
            .checked_add(next.changes.len())
            .is_some_and(|n| n <= bounds.source_changes)
        && epoch
            .transaction()
            .batch
            .updates
            .len()
            .checked_add(next.batch.updates.len())
            .is_some_and(|n| n <= bounds.limits.output_entries.min(bounds.limits.contributions))
}
async fn receive(
    stream: &mut impl InputStream,
    stop: &mut watch::Receiver<bool>,
    decision: Decision,
) -> Result<Option<Input>> {
    let input = if let Some(ready) = stream.input().now_or_never() {
        Some(ready?)
    } else if decision.wait.is_zero() {
        None
    } else {
        tokio::select! {
            signal=stop.changed()=>{signal?;None}
            ()=tokio::time::sleep(decision.wait)=>None,
            transaction=stream.input()=>Some(transaction?),
        }
    };
    Ok(input)
}

#[cfg(test)]
mod tests;
