//! Opt-in boundary timings; disabled paths never sample a clock.
use std::time::Instant;
#[derive(Default)]
pub struct Clock(Option<Instant>);
impl Clock {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(enabled.then(Instant::now))
    }
    pub(crate) fn lap(&mut self) -> u128 {
        let Some(prior) = self.0.as_mut() else {
            return 0;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(*prior).as_micros();
        *prior = now;
        elapsed
    }
}

/// Per-preparation attribution; disabled profiles never sample a clock.
pub struct Profile {
    clock: Clock,
    values: Option<std::collections::BTreeMap<String, u128>>,
    admission: std::time::Duration,
}
impl Profile {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            clock: Clock::new(enabled),
            values: enabled.then(std::collections::BTreeMap::new),
            admission: std::time::Duration::ZERO,
        }
    }
    pub(crate) const fn admitted(&mut self, permit: &crate::engine::execution::WorkspacePermit) {
        self.admission = self.admission.saturating_add(permit.waited());
    }
    pub(crate) const fn admission(&self) -> std::time::Duration {
        self.admission
    }
    pub(crate) fn reset_clock(&mut self) {
        self.clock = Clock::new(self.values.is_some());
    }
    pub(crate) fn mark(&mut self, name: impl Into<String>) {
        if let Some(values) = &mut self.values {
            let elapsed = self.clock.lap();
            let total = values.entry(name.into()).or_default();
            *total = total.saturating_add(elapsed);
        }
    }
    pub(crate) fn operator(&mut self, node: &str) {
        if self.values.is_some() {
            self.mark(format!("operator:{node}"));
        }
    }
    pub(crate) fn finish(self) -> Option<std::collections::BTreeMap<String, u128>> {
        self.values
    }
}
