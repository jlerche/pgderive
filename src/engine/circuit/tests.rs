use super::Circuit;
use crate::engine::{IncrementalJoin, ZSet};
use anyhow::{Result, anyhow};

type Input = (ZSet<(i32, i32)>, ZSet<(i32, i32)>);

#[derive(Clone, Default)]
struct Graph {
    join: IncrementalJoin<i32, i32, i32>,
    output: ZSet<i32>,
}

fn evaluate(state: &mut Graph, input: &Input) -> Result<ZSet<i32>> {
    let joined = state.join.step(&input.0, &input.1)?;
    let delta =
        joined.delta.try_filter(|(_, left, _)| Ok(*left >= 0))?.try_map(|(key, _, _)| Ok(*key))?;
    state.output.apply(&delta)?;
    Ok(delta)
}

#[test]
fn downstream_failure_rolls_back_entire_graph_and_retry() -> Result<()> {
    let mut circuit = Circuit::<Graph>::default();
    let input = (ZSet::from_updates([((1, 1), 1)])?, ZSet::from_updates([((1, 2), 1)])?);
    assert!(
        circuit
            .step(&input, |state, input| {
                evaluate(state, input)?;
                Err::<ZSet<i32>, _>(anyhow!("failure after all nodes"))
            })
            .is_err()
    );
    assert_eq!(circuit.state().output, ZSet::default());
    assert_eq!(circuit.time, 0);
    let first = circuit.step(&input, evaluate)?;
    assert_eq!(first.time, 1);
    assert_eq!(first.output, ZSet::from_updates([(1, 1)])?);
    let empty = circuit.step(&(ZSet::default(), ZSet::default()), evaluate)?;
    assert_eq!(empty.time, 2);
    assert_eq!(empty.output, ZSet::default());
    let delete = circuit.step(&(ZSet::from_updates([((1, 1), -1)])?, ZSet::default()), evaluate)?;
    assert_eq!(delete.output, ZSet::from_updates([(1, -1)])?);
    Ok(())
}

#[test]
fn projected_overflow_does_not_advance_join_or_output() -> Result<()> {
    let mut circuit = Circuit::<Graph>::default();
    let input = (
        ZSet::from_updates([((1, 1), i64::MAX), ((1, 2), 1)])?,
        ZSet::from_updates([((1, 1), 1)])?,
    );
    assert!(circuit.step(&input, evaluate).is_err());
    assert_eq!(circuit.time, 0);
    assert_eq!(circuit.state().output, ZSet::default());
    let retry = (ZSet::from_updates([((1, 1), 1)])?, ZSet::from_updates([((1, 1), 1)])?);
    assert_eq!(circuit.step(&retry, evaluate)?.output, ZSet::from_updates([(1, 1)])?);
    Ok(())
}

#[test]
fn tick_overflow_prevents_evaluation() {
    let mut circuit = Circuit::new(0);
    circuit.time = u64::MAX;
    assert!(
        circuit
            .step(&(), |state, ()| {
                *state = 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(*circuit.state(), 0);
    assert_eq!(circuit.time, u64::MAX);
}
