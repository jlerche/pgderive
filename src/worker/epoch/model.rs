//! Bounded online fixed-plus-variable cost model, with residual uncertainty.
use std::{collections::VecDeque, time::Duration};
#[derive(Default)]
pub(super) struct Model {
    samples: VecDeque<(f64, f64)>,
}
#[derive(Clone, Copy)]
pub(super) struct Estimate {
    pub(super) fixed: f64,
    pub(super) variable: f64,
    pub(super) margin: f64,
    pub(super) largest: f64,
}
impl Model {
    pub(super) fn record(&mut self, work: f64, elapsed: Duration) {
        if self.samples.len() == 32 {
            self.samples.pop_front();
        }
        self.samples.push_back((work.max(1.0), elapsed.as_secs_f64()));
    }
    pub(super) fn estimate(&self) -> Option<Estimate> {
        let n = f64::from(u32::try_from(self.samples.len()).ok()?);
        if n == 0.0 {
            return None;
        }
        let mean_x = self.samples.iter().map(|(x, _)| x).sum::<f64>() / n;
        let mean_y = self.samples.iter().map(|(_, y)| y).sum::<f64>() / n;
        let variance = self.samples.iter().map(|(x, _)| (x - mean_x).powi(2)).sum::<f64>();
        let covariance = self.samples.iter().map(|(x, y)| (x - mean_x) * (y - mean_y)).sum::<f64>();
        let variable = if variance > 1e-9 { (covariance / variance).max(0.0) } else { 0.0 };
        let fixed = (-variable).mul_add(mean_x, mean_y).max(0.0);
        let residual =
            self.samples.iter().map(|(x, y)| (y - variable.mul_add(*x, fixed)).abs()).sum::<f64>()
                / n;
        let largest = self.samples.iter().map(|(x, _)| *x).fold(0.0, f64::max);
        Some(Estimate { fixed, variable, margin: residual * 2.0, largest })
    }
}
impl Estimate {
    pub(super) fn seconds(self, work: f64) -> f64 {
        self.variable.mul_add(work, self.fixed) + self.margin
    }
}
