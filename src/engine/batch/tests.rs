use crate::engine::{Batch, BatchBuilder, GroupedCount, IncrementalJoin, ZSet};
use anyhow::Result;
use num_bigint::BigInt;
use std::collections::BTreeMap;

#[test]
fn boundary_cancellation_and_final_overflow() -> Result<()> {
    for weights in [[i64::MAX, 1, -1], [1, -1, i64::MAX], [-1, i64::MAX, 1]] {
        assert_eq!(
            ZSet::from_updates(weights.map(|weight| (0, weight)))?,
            ZSet::from_updates([(0, i64::MAX)])?
        );
    }
    assert_eq!(
        ZSet::from_updates([(0, i64::MIN), (0, -1), (0, 1)])?,
        ZSet::from_updates([(0, i64::MIN)])?
    );
    assert!(ZSet::from_updates([(0, i64::MAX), (0, 1)]).is_err());
    assert!(ZSet::from_updates([(0, i64::MIN), (0, -1)]).is_err());
    let mapped = ZSet::from_updates([(0, i64::MAX), (1, 1), (2, -1)])?.try_map(|_| Ok(0))?;
    assert_eq!(mapped, ZSet::from_updates([(0, i64::MAX)])?);
    Ok(())
}

#[test]
fn count_narrows_after_combining_prior_state_and_delta() -> Result<()> {
    let mut count = GroupedCount::default();
    count.step(&ZSet::from_updates([((1, 0), i64::MIN)])?)?;
    // Group delta exceeds i64, but the final group count is zero.
    let output = count.step(&ZSet::from_updates([((1, 1), i64::MAX), ((1, 2), 1)])?)?;
    assert_eq!(output, ZSet::from_updates([((1, i64::MIN), -1)])?);
    Ok(())
}

#[test]
fn join_narrows_after_all_cross_terms() -> Result<()> {
    let mut join = IncrementalJoin::<i32, i32, i32>::default();
    join.step(&ZSet::from_updates([((1, 1), i64::MAX)])?, &ZSet::default())?;
    let step = join.step(
        &ZSet::from_updates([((1, 1), 1 - i64::MAX)])?,
        &ZSet::from_updates([((1, 1), 2)])?,
    )?;
    assert_eq!(step.delta, ZSet::from_updates([((1, 1, 1), 2)])?);
    Ok(())
}

#[test]
fn exact_merge_trees_preserve_identity_and_physical_grouping() -> Result<()> {
    for seed in 1..=128_i64 {
        let mut raw = vec![((0, 0), i64::MAX), ((0, 0), 1), ((0, 0), -1)];
        raw.extend((0..64).map(|n| (((n % 7) + 1, n % 3), (seed + n) % 5 - 2)));
        let mut oracle = BTreeMap::<(i64, i64), BigInt>::new();
        for (tuple, weight) in &raw {
            *oracle.entry(*tuple).or_default() += *weight;
        }
        oracle.retain(|_, weight| *weight != BigInt::default());
        let expected = oracle
            .into_iter()
            .map(|(tuple, weight)| Ok((tuple, i64::try_from(weight)?)))
            .collect::<Result<Vec<_>>>()?;
        raw.rotate_left(usize::try_from(seed)? % 67);
        if seed % 2 == 0 {
            raw.reverse();
        }
        let mut partials = raw
            .chunks(3)
            .map(|chunk| {
                let mut builder = BatchBuilder::default();
                for ((key, value), weight) in chunk {
                    builder.push(*key, *value, *weight);
                }
                builder
            })
            .collect::<Vec<_>>();
        while partials.len() > 1 {
            let other = partials.pop().ok_or_else(|| anyhow::anyhow!("missing partial"))?;
            partials[0].merge(other);
            partials.rotate_left(1);
        }
        let batch = partials.pop().ok_or_else(|| anyhow::anyhow!("missing batch"))?.finish()?;
        assert_eq!(
            batch.iter().map(|(tuple, weight)| (*tuple, *weight)).collect::<Vec<_>>(),
            expected
        );
        let mut copy = BatchBuilder::default();
        copy.extend(&batch);
        assert_eq!(copy.finish()?, batch);
        assert_eq!(Batch::from_updates(raw)?, batch);
    }
    Ok(())
}
