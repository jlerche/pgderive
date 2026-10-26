//! Forecast admission pressure without changing immutable checkpoint cadence.
use std::time::Duration;
use tokio::time::Instant;

/// Charged memory, total roots, unsealed roots, and epochs since durable state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::worker) struct Load(pub(in crate::worker) [u64; 4]);
#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    load: Load,
}
#[derive(Default)]
pub(in crate::worker) struct Controller {
    service: Duration,
    captured: Option<Sample>,
    previous: Load,
    burst: [u64; 4],
}
impl Controller {
    pub(in crate::worker) const fn started(&mut self, at: Instant, load: Load) {
        self.captured = Some(Sample { at, load });
        self.previous = load;
        self.burst = [0; 4];
    }
    pub(in crate::worker) fn completed(&mut self, elapsed: Duration) {
        // Follow slow service immediately; retire old spikes gradually.
        self.service = elapsed.max(self.service.mul_f64(0.875));
        self.captured = None;
    }
    pub(in crate::worker) fn pressure(
        &mut self,
        now: Instant,
        elapsed: Duration,
        load: Load,
        limits: Load,
    ) -> bool {
        let Some(captured) = self.captured else { return false };
        let previous = std::mem::replace(&mut self.previous, load);
        if previous == load {
            return load.0.into_iter().zip(limits.0).any(|(current, limit)| current >= limit);
        }
        // An overdue or first upload still has uncertain work remaining. Never
        // interpret exceeding the estimate as zero remaining service.
        let remaining = self.service.saturating_sub(elapsed).max(elapsed);
        // Use growth across the captured suffix, not one run-allocation spike
        // divided by one fast epoch. Cyclic run consolidation is not new debt.
        let interval = now.saturating_duration_since(captured.at).as_nanos();
        load.0.into_iter().zip(previous.0).zip(limits.0).enumerate().any(
            |(index, ((current, before), limit))| {
                let growth = current.saturating_sub(before);
                self.burst[index] = self.burst[index].max(growth);
                let forecast = u128::from(current.saturating_sub(captured.load.0[index]))
                    .saturating_mul(remaining.as_nanos())
                    .checked_div(interval)
                    .unwrap_or(0);
                u128::from(current)
                    .saturating_add(forecast)
                    .saturating_add(u128::from(self.burst[index]))
                    >= u128::from(limit)
            },
        )
    }
}
#[cfg(test)]
mod tests;
