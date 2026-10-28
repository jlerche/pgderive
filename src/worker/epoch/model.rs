//! Bounded nonnegative service estimates. Forecasts are empirical, not upper bounds.
use super::features::Input;
use crate::catalog::Mutations;
use std::{collections::VecDeque, time::Duration};
mod fit;
use fit::Fit;
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(in crate::worker) struct Observation {
    pub preparation: Duration,
    pub admission: Duration,
    pub disruption: Duration,
    pub fixed: Duration,
    pub dml: Duration,
    pub mutations: Mutations,
}
impl Observation {
    pub(in crate::worker) const fn published(
        elapsed: Duration,
        preparation: Duration,
        admission: Duration,
        publication: &crate::catalog::Feedback,
    ) -> Self {
        Self {
            preparation,
            admission,
            disruption: elapsed
                .saturating_sub(preparation)
                .saturating_sub(admission)
                .saturating_sub(publication.fixed)
                .saturating_sub(publication.dml),
            fixed: publication.fixed,
            dml: publication.dml,
            mutations: publication.mutations,
        }
    }
}
#[derive(Default)]
pub(super) struct Model {
    samples: VecDeque<(Input, Observation)>,
    estimate: Option<Estimate>,
}
#[derive(Clone, Copy, Default)]
pub(super) struct Estimate {
    preparation: Fit,
    outputs: [Fit; 3],
    dml: Fit,
    pub(super) fixed: f64,
    overhead: f64,
    margin: f64,
    largest: [f64; 3],
}
fn mutations(value: Mutations) -> [f64; 3] {
    [value.inserts, value.updates, value.deletes]
        .map(|n| f64::from(u32::try_from(n).unwrap_or(u32::MAX)))
}
impl Model {
    pub(super) fn record(&mut self, input: Input, observation: Observation) {
        if self.samples.len() == 32 {
            self.samples.pop_front();
        }
        self.samples.push_back((input, observation));
        let samples = self.samples.iter().copied().collect::<Vec<_>>();
        self.estimate = Some(Estimate::learn(&samples));
    }
    pub(super) const fn estimate(&self) -> Option<Estimate> {
        self.estimate
    }
}
impl Estimate {
    fn learn(samples: &[(Input, Observation)]) -> Self {
        let inputs = samples.iter().map(|(input, _)| input.values()).collect::<Vec<_>>();
        let outputs = samples.iter().map(|(_, o)| mutations(o.mutations)).collect::<Vec<_>>();
        let target =
            |f: fn(&Observation) -> f64| samples.iter().map(|(_, o)| f(o)).collect::<Vec<_>>();
        let n = f64::from(u32::try_from(samples.len()).unwrap_or(32)).max(1.0);
        let mut result = Self {
            preparation: Fit::learn(&inputs, &target(|o| o.preparation.as_secs_f64())),
            outputs: std::array::from_fn(|i| {
                Fit::learn(&inputs, &outputs.iter().map(|v| v[i]).collect::<Vec<_>>())
            }),
            dml: Fit::learn(&outputs, &target(|o| o.dml.as_secs_f64())),
            fixed: target(|o| o.fixed.as_secs_f64()).iter().sum::<f64>() / n,
            overhead: target(|o| o.admission.saturating_add(o.disruption).as_secs_f64())
                .iter()
                .sum::<f64>()
                / n,
            margin: 0.0,
            largest: std::array::from_fn(|i| inputs.iter().map(|v| v[i]).fold(0.0, f64::max)),
        };
        result.margin = samples
            .iter()
            .map(|(input, o)| {
                let actual = o
                    .preparation
                    .saturating_add(o.admission)
                    .saturating_add(o.disruption)
                    .saturating_add(o.fixed)
                    .saturating_add(o.dml)
                    .as_secs_f64();
                (actual - result.seconds(*input)).abs()
            })
            .sum::<f64>()
            * 2.0
            / n;
        result
    }
    pub(super) fn seconds(self, input: Input) -> f64 {
        let input = input.values();
        let outputs = self.outputs.map(|fit| fit.predict(input));
        self.fixed
            + self.overhead
            + self.preparation.predict(input)
            + self.dml.predict(outputs)
            + self.margin
    }
    pub(super) fn exploring(self, input: Input) -> bool {
        input
            .values()
            .into_iter()
            .zip(self.largest)
            .any(|(value, largest)| value >= largest.max(1.0) * 2.0)
    }
}
#[cfg(test)]
mod tests;
