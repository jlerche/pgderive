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
