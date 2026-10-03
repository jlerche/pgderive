use super::GroupedCount;
use crate::engine::{Circuit, ZSet};
use anyhow::Result;
use std::collections::BTreeMap;

#[test]
fn groups_are_replaced_and_removed_with_signed_weights() -> Result<()> {
    let mut count = GroupedCount::default();
    let first = count.step(&ZSet::from_updates([((Some(1), "a"), 3), ((None, "b"), 2)])?)?;
    assert_eq!(first, ZSet::from_updates([((Some(1), 3), 1), ((None, 2), 1)])?);
    let second = count.step(&ZSet::from_updates([((Some(1), "a"), -3), ((None, "b"), -1)])?)?;
    assert_eq!(second, ZSet::from_updates([((Some(1), 3), -1), ((None, 2), -1), ((None, 1), 1)])?);
    let unchanged = count.step(&ZSet::from_updates([((None, "x"), 1), ((None, "y"), -1)])?)?;
    assert_eq!(unchanged, ZSet::default());
    let signed = count.step(&ZSet::from_updates([((Some(2), "c"), -2)])?)?;
    assert_eq!(signed, ZSet::from_updates([((Some(2), -2), 1)])?);
    Ok(())
}

#[test]
fn signed_histories_match_independent_group_oracle() -> Result<()> {
    for seed in 1..=128_i64 {
        let mut count = GroupedCount::default();
        let mut output = ZSet::default();
        let mut totals = BTreeMap::<i64, i64>::new();
        for tick in 0..16_i64 {
            let raw = (0..8)
                .map(|n| (((n + tick) % 5, n), (seed + n + tick) % 5 - 2))
                .collect::<Vec<_>>();
            for ((key, _), weight) in &raw {
                *totals.entry(*key).or_default() += weight;
            }
            let delta = count.step(&ZSet::from_updates(raw)?)?;
            output.apply(&delta)?;
            let expected = ZSet::from_updates(
                totals
                    .iter()
                    .filter(|(_, count)| **count != 0)
                    .map(|(key, count)| ((*key, *count), 1)),
            )?;
            assert_eq!(output, expected, "seed={seed}, tick={tick}");
        }
    }
    Ok(())
}

#[test]
fn group_overflow_and_circuit_failure_can_be_retried() -> Result<()> {
    let mut count = GroupedCount::default();
    count.step(&ZSet::from_updates([((1, 1), i64::MAX)])?)?;
    let before = count.counts.clone();
    assert!(count.step(&ZSet::from_updates([((0, 0), 1), ((1, 1), 1)])?).is_err());
    assert_eq!(count.counts, before);
    assert!(count.step(&ZSet::from_updates([((2, 0), i64::MAX), ((2, 1), 1)])?).is_err());
    assert_eq!(count.counts, before);
    let mut circuit = Circuit::new(count);
    let input = ZSet::from_updates([((1, 1), -1)])?;
    assert!(
        circuit
            .step(&input, |count, input| {
                count.step(input)?;
                Err::<(), _>(anyhow::anyhow!("downstream failure"))
            })
            .is_err()
    );
    assert_eq!(circuit.state().counts, before);
    let retried = circuit.step(&input, GroupedCount::step)?;
    assert_eq!(retried.time, 1);
    assert_eq!(retried.output, ZSet::from_updates([((1, i64::MAX), -1), ((1, i64::MAX - 1), 1)])?);
    Ok(())
}

mod recorded;
