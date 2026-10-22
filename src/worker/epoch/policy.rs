use super::{Epoch, model::Model};
use anyhow::{Result, ensure};
use serde::Deserialize;
use std::time::Duration;
/// Adaptive publication goals and defensive queue limits, not periodic triggers.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    /// Disable coalescing for a single-transaction diagnostic control.
    pub enabled: bool,
    /// Desired source-commit-to-publication freshness; not a buffering interval.
    pub freshness_target_ms: u64,
    /// Maximum charged source bytes in a group; one complete source transaction
    /// can exceed this target but remains subject to source/execution limits.
    pub max_bytes: u64,
    /// Defensive maximum complete source transactions in one epoch.
    pub max_transactions: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: true,
            freshness_target_ms: 50,
            max_bytes: 8 * 1024 * 1024,
            max_transactions: 4096,
        }
    }
}
impl Policy {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=60_000).contains(&self.freshness_target_ms),
            "invalid epoch freshness objective"
        );
        ensure!((1..=1024 * 1024 * 1024).contains(&self.max_bytes), "invalid epoch byte ceiling");
        ensure!((1..=65_536).contains(&self.max_transactions), "invalid epoch transaction ceiling");
        Ok(())
    }
}
pub(in crate::worker) struct Controller {
    policy: Policy,
    model: Model,
    arrival: Option<f64>,
    last_commit: Option<i64>,
}
#[derive(Clone, Copy)]
pub(in crate::worker) struct Decision {
    pub(in crate::worker) wait: Duration,
    pub(in crate::worker) reason: &'static str,
    pub(in crate::worker) predicted: Duration,
}
impl Controller {
    pub(in crate::worker) fn new(policy: Policy) -> Self {
        Self { policy, model: Model::default(), arrival: None, last_commit: None }
    }
    pub(in crate::worker) fn observe(&mut self, commit: Option<i64>) {
        if let Some(commit) = commit {
            if let Some(previous) = self.last_commit {
                let delta = commit.saturating_sub(previous);
                if let Ok(delta) = u32::try_from(delta)
                    && delta > 0
                {
                    let gap = f64::from(delta) / 1_000_000.0;
                    self.arrival =
                        Some(self.arrival.map_or(gap, |prior| prior.mul_add(0.8, gap * 0.2)));
                }
            }
            self.last_commit = Some(commit);
        }
    }
    pub(in crate::worker) fn record(&mut self, work: f64, duration: Duration) {
        self.model.record(work, duration);
    }
    pub(in crate::worker) fn decision(&self, epoch: &Epoch) -> Decision {
        let seal = |reason, predicted| Decision { wait: Duration::ZERO, reason, predicted };
        if !self.policy.enabled {
            return seal("single_transaction", Duration::ZERO);
        }
        let Some(model) = self.model.estimate() else {
            return seal("cold_start", Duration::ZERO);
        };
        let work = epoch.work();
        let predicted = Duration::from_secs_f64(model.seconds(work));
        let Some(arrival) = self.arrival else {
            return seal("unknown_arrival", predicted);
        };
        let target = Duration::from_millis(self.policy.freshness_target_ms).as_secs_f64();
        let slack = target - epoch.age().as_secs_f64() - predicted.as_secs_f64();
        if slack <= 0.0 {
            return seal("freshness_pressure", predicted);
        }
        if arrival >= model.fixed || model.fixed <= 0.0 {
            return seal("sparse_input", predicted);
        }
        if work >= model.largest * 2.0 {
            return seal("bounded_exploration", predicted);
        }
        if model.fixed / model.seconds(work).max(1e-9) <= 0.15 {
            return seal("amortized", predicted);
        }
        // Waiting is useful only while another expected arrival plus its predicted
        // marginal work leaves freshness slack. Re-evaluate after every arrival.
        let marginal = model.variable
            * (work / f64::from(u32::try_from(epoch.transactions()).unwrap_or(u32::MAX)));
        if slack < arrival.mul_add(1.25, marginal) {
            return seal("freshness_budget", predicted);
        }
        Decision {
            wait: Duration::from_secs_f64((arrival * 1.25).min(slack - marginal)),
            reason: "predicted_arrival",
            predicted,
        }
    }
}
