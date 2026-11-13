use super::{Project, TimedBatch};
use crate::engine::{Batch, ZSet, reader::BatchData};
use anyhow::{Result, anyhow};
fn transform<T: BatchData, U: BatchData>(
    input: &ZSet<T>,
    project: impl Fn(&T) -> Result<Option<U>> + Send + Sync + 'static,
) -> Result<ZSet<U>> {
    let batch =
        Batch::from_updates(input.iter().map(|(value, weight)| (((), value.clone()), *weight)))?;
    let result =
        Project::new(move |(): &(), value: &T| Ok(project(value)?.map(|value| ((), value))))
            .evaluate(&TimedBatch { time: 1, batch })?;
    ZSet::from_updates(result.batch.iter().map(|(((), value), weight)| (value.clone(), *weight)))
}
#[test]
fn projection_consolidates_signed_collisions() -> Result<()> {
    let source = ZSet::from_updates([
        ((1, "a".to_owned()), 3),
        ((1, "b".into()), -2),
        ((2, "a".into()), -1),
        ((2, "b".into()), 1),
    ])?;
    assert_eq!(transform(&source, |(key, _)| Ok(Some(*key)))?, ZSet::from_updates([(1, 1)])?);
    Ok(())
}
#[test]
fn filter_and_map_are_linear_against_raw_updates() -> Result<()> {
    for seed in 1..=128_i64 {
        let a = ZSet::from_updates((0..16).map(|n| ((n % 5, n % 3), (seed + n) % 5 - 2)))?;
        let b = ZSet::from_updates((0..16).map(|n| ((n % 5, n % 3), (seed * n) % 5 - 2)))?;
        let project = |input: &ZSet<(i64, i64)>| {
            transform(input, |(key, value)| Ok((*key >= 2).then_some(*value)))
        };
        let mut sum = a.clone();
        sum.apply(&b)?;
        let mut separately = project(&a)?;
        separately.apply(&project(&b)?)?;
        assert_eq!(project(&sum)?, separately);
        let expected = ZSet::from_updates(
            (0..16)
                .filter(|n| n % 5 >= 2)
                .flat_map(|n| [(n % 3, (seed + n) % 5 - 2), (n % 3, (seed * n) % 5 - 2)]),
        )?;
        assert_eq!(separately, expected);
    }
    Ok(())
}
#[test]
fn null_filter_and_failures_preserve_input() -> Result<()> {
    let input = ZSet::from_updates([(None, 1), (Some(1), 2), (Some(2), -1)])?;
    let before = input.clone();
    assert_eq!(
        transform(&input, |value| Ok(value.filter(|n| *n >= 2).map(Some)))?,
        ZSet::from_updates([(Some(2), -1)])?
    );
    assert!(transform::<_, i32>(&input, |_| Err(anyhow!("predicate failed"))).is_err());
    assert!(transform::<_, i32>(&input, |_| Err(anyhow!("projection failed"))).is_err());
    assert_eq!(input, before);
    let large = ZSet::from_updates([(1, i64::MAX), (2, 1)])?;
    assert!(transform(&large, |_| Ok(Some(0))).is_err());
    Ok(())
}
#[test]
fn boundary_projection_narrows_after_cancellation() -> Result<()> {
    let source = ZSet::from_updates([(0, i64::MAX), (1, 1), (2, -1)])?;
    assert_eq!(transform(&source, |_| Ok(Some(0)))?, ZSet::from_updates([(0, i64::MAX)])?);
    Ok(())
}
