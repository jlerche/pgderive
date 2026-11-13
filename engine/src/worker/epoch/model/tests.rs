use super::*;
use crate::worker::epoch::tests::{input_work, observation};
#[test]
fn independent_input_dimensions_and_actual_dml_change_forecasts() {
    let mut model = Model::default();
    for _ in 0..3 {
        for input in [
            Input { rows: 1, bytes: 0, keys: 0 },
            Input { rows: 0, bytes: 1000, keys: 0 },
            Input { rows: 0, bytes: 0, keys: 1 },
        ] {
            let [rows, bytes, keys] = input.values();
            model.record(
                input,
                Observation {
                    preparation: Duration::from_secs_f64(
                        keys.mul_add(0.003, rows.mul_add(0.001, bytes * 0.000_002)),
                    ),
                    fixed: Duration::from_millis(2),
                    mutations: Mutations { updates: input.keys, ..Mutations::default() },
                    dml: Duration::from_millis(input.keys),
                    ..Observation::default()
                },
            );
        }
    }
    let estimate = model.estimate().into_iter().collect::<Vec<_>>();
    assert_eq!(estimate.len(), 1);
    let estimate = estimate[0];
    let base = Input::default();
    assert!(estimate.seconds(Input { rows: 1, ..base }) > estimate.seconds(base) + 0.0009);
    assert!(estimate.seconds(Input { bytes: 1000, ..base }) > estimate.seconds(base) + 0.0019);
    assert!(estimate.seconds(Input { keys: 1, ..base }) > estimate.seconds(base) + 0.0039);
}
#[test]
fn collinear_noisy_samples_remain_finite_and_bounded() {
    let mut model = Model::default();
    for n in 1..=100 {
        model.record(input_work(n), observation(n * 10 + (n % 3) * 20, 2000));
    }
    assert_eq!(model.samples.len(), 32);
    assert!(model.estimate().is_some());
    if let Some(estimate) = model.estimate() {
        for n in [0, 1, 100, 1000] {
            assert!(estimate.seconds(input_work(n)).is_finite());
            assert!(estimate.seconds(input_work(n)) >= 0.002);
        }
        assert!(estimate.exploring(Input { bytes: 3201, ..Input::default() }));
        assert!(estimate.exploring(Input { keys: 201, ..Input::default() }));
        assert!(!estimate.exploring(input_work(100)));
    }
}
#[test]
fn admission_and_maintenance_are_not_amortizable_fixed_publication() {
    let mut model = Model::default();
    let mut sample = observation(100, 2000);
    sample.admission = Duration::from_millis(20);
    sample.disruption = Duration::from_millis(50);
    model.record(input_work(1), sample);
    assert!(model.estimate().is_some());
    if let Some(estimate) = model.estimate() {
        assert!((estimate.fixed - 0.002).abs() < 1e-9);
        assert!(estimate.seconds(input_work(1)) >= 0.072);
        assert!(estimate.seconds(input_work(2)) < 0.073);
    }
}
