use super::IncrementalJoin;
use crate::engine::ZSet;
use std::collections::BTreeMap;

type Input = ((i64, i64), i64);
type Output = BTreeMap<(i64, i64, i64), i64>;

// Recompute from raw histories rather than using the operator or ZSet algebra.
// This follows the independent nested-loop contract in the accepted PoC tests.
fn oracle(left: &[Input], right: &[Input]) -> Output {
    let mut output = Output::new();
    for ((key, left), lw) in left {
        for ((other, right), rw) in right {
            if key == other {
                *output.entry((*key, *left, *right)).or_default() += lw * rw;
            }
        }
    }
    output.retain(|_, weight| *weight != 0);
    output
}

#[test]
fn simultaneous_signed_inputs_match_independent_oracle() -> anyhow::Result<()> {
    for seed in 1..=128_i64 {
        let mut engine = IncrementalJoin::default();
        let mut left_history = Vec::new();
        let mut right_history = Vec::new();
        let mut output = Output::new();
        for tick in 1..=16_i64 {
            let left = [((tick % 3, seed % 5), (tick + seed) % 5 - 2)];
            let right = [(((tick + seed) % 3, tick % 4), (tick * seed) % 5 - 2)];
            let step = engine.step(&ZSet::from_updates(left)?, &ZSet::from_updates(right)?)?;
            assert_eq!(step.time, u64::try_from(tick)?);
            for (tuple, weight) in step.delta.iter() {
                *output.entry(*tuple).or_default() += weight;
            }
            output.retain(|_, weight| *weight != 0);
            left_history.extend(left);
            right_history.extend(right);
            assert_eq!(output, oracle(&left_history, &right_history), "seed={seed}, tick={tick}");
        }
    }
    Ok(())
}

#[test]
fn multiplicity_cross_term_and_full_tuple_retraction() -> anyhow::Result<()> {
    let mut engine = IncrementalJoin::default();
    let left = ZSet::from_updates([((1, "old"), 3)])?;
    let right = ZSet::from_updates([((1, "right"), 2)])?;
    let first = engine.step(&left, &right)?;
    assert_eq!(first.delta, ZSet::from_updates([((1, "old", "right"), 6)])?);
    let second = engine.step(
        &ZSet::from_updates([((1, "old"), -3), ((1, "new"), 3)])?,
        &ZSet::from_updates([((1, "right"), -2), ((1, "other"), 2)])?,
    )?;
    assert_eq!(
        second.delta,
        ZSet::from_updates([((1, "old", "right"), -6), ((1, "new", "other"), 6)])?
    );
    Ok(())
}

#[test]
fn overflow_keeps_state_and_time_unchanged() -> anyhow::Result<()> {
    let mut engine = IncrementalJoin::default();
    let left = ZSet::from_updates([((1, 1), i64::MAX)])?;
    engine.step(&left, &ZSet::<(i32, i32)>::default())?;
    let before_left = engine.left.clone();
    let before_right = engine.right.clone();
    assert!(engine.step(&ZSet::default(), &ZSet::from_updates([((1, 1), 2)])?).is_err());
    assert!(engine.step(&ZSet::from_updates([((1, 1), 1)])?, &ZSet::default()).is_err());
    assert_eq!(engine.left, before_left);
    assert_eq!(engine.right, before_right);
    assert_eq!(engine.time, 1);
    let empty = engine.step(&ZSet::default(), &ZSet::default())?;
    assert_eq!(empty.time, 2);
    engine.time = u64::MAX;
    assert!(engine.step(&ZSet::default(), &ZSet::default()).is_err());
    assert_eq!(engine.time, u64::MAX);
    Ok(())
}

#[test]
fn integration_overflow_is_atomic() -> anyhow::Result<()> {
    let mut state = ZSet::from_updates([(1, i64::MAX)])?;
    let before = state.clone();
    assert!(state.apply(&ZSet::from_updates([(0, 1), (1, 1)])?).is_err());
    assert_eq!(state, before);
    state.apply(&ZSet::from_updates([(1, -i64::MAX)])?)?;
    assert_eq!(state, ZSet::default());
    Ok(())
}

#[test]
fn failed_right_integration_does_not_commit_staged_left() -> anyhow::Result<()> {
    let mut engine = IncrementalJoin::default();
    engine.step(&ZSet::<(i32, i32)>::default(), &ZSet::from_updates([((1, 1), i64::MAX)])?)?;
    let before_left = engine.left.clone();
    let before_right = engine.right.clone();
    // The left key is unmatched, so multiplication succeeds and failure occurs
    // only while integrating the right delta, after left staging succeeded.
    assert!(
        engine
            .step(&ZSet::from_updates([((2, 2), 1)])?, &ZSet::from_updates([((1, 1), 1)])?)
            .is_err()
    );
    assert_eq!(engine.left, before_left);
    assert_eq!(engine.right, before_right);
    assert_eq!(engine.time, 1);
    let recovered = engine.step(&ZSet::from_updates([((1, 3), 1)])?, &ZSet::default())?;
    assert_eq!(recovered.time, 2);
    assert_eq!(recovered.delta, ZSet::from_updates([((1, 3, 1), i64::MAX)])?);
    Ok(())
}
