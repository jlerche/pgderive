use super::{Controller, Epoch, Policy, policy::Decision};
use crate::{acknowledged::Stream, engine::execution::Limits, transaction::Transaction};
use anyhow::Result;
use futures_util::FutureExt;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::Instant;

pub(in crate::worker) struct Input {
    pub(in crate::worker) transaction: Transaction,
    pub(in crate::worker) decode_us: u128,
}
#[derive(Default)]
pub(in crate::worker) struct Collector<C = WallTime> {
    held: Option<Input>,
    clock: C,
}
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
    pub(in crate::worker) stop: &'a mut watch::Receiver<bool>,
}
pub(in crate::worker) struct Collected {
    pub(in crate::worker) epoch: Epoch,
    pub(in crate::worker) reason: &'static str,
    pub(in crate::worker) predicted: Duration,
}
impl<C: Clock> Collector<C> {
    pub(in crate::worker) async fn recv(&mut self, stream: &mut impl InputStream) -> Result<Input> {
        if let Some(input) = self.held.take() {
            return Ok(input);
        }
        stream.input().await
    }
    pub(in crate::worker) async fn gather(
        &mut self,
        first: Input,
        stream: &mut impl InputStream,
        scheduler: &mut Controller,
        bounds: Bounds<'_>,
    ) -> Result<Collected> {
        scheduler.observe(first.transaction.commit_time_micros);
        let mut epoch =
            Epoch::at(first.transaction, first.decode_us, Instant::now(), self.clock.wall())?;
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
    if !bounds.policy.enabled {
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
