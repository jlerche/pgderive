//! Normalized nonnegative least squares with bounded coordinate descent.
//! No unconstrained intercept: `PostgreSQL`'s fixed phase is measured separately.
#[derive(Clone, Copy, Default)]
pub(super) struct Fit {
    weights: [f64; 3],
    scales: [f64; 3],
}
impl Fit {
    pub(super) fn learn(inputs: &[[f64; 3]], targets: &[f64]) -> Self {
        let scales = std::array::from_fn(|i| inputs.iter().map(|v| v[i]).fold(1.0, f64::max));
        let xs = inputs
            .iter()
            .map(|v| std::array::from_fn::<_, 3, _>(|i| v[i] / scales[i]))
            .collect::<Vec<_>>();
        let mut weights = [0.0; 3];
        for _ in 0..24 {
            for i in 0..3 {
                let mut numerator = 0.0;
                let mut denominator = 1e-12;
                for (x, y) in xs.iter().zip(targets) {
                    let other = (0..3).filter(|j| *j != i).map(|j| x[j] * weights[j]).sum::<f64>();
                    numerator += x[i] * (y - other);
                    denominator += x[i] * x[i];
                }
                weights[i] = (numerator / denominator).max(0.0);
            }
        }
        Self { weights, scales }
    }
    pub(super) fn predict(self, input: [f64; 3]) -> f64 {
        (0..3).map(|i| input[i] / self.scales[i].max(1.0) * self.weights[i]).sum()
    }
}
