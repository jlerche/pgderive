use crate::engine::ZSet;
use anyhow::{Result, anyhow};

#[test]
fn projection_consolidates_signed_collisions() -> Result<()> {
    let source =
        ZSet::from_updates([((1, "a"), 3), ((1, "b"), -2), ((2, "a"), -1), ((2, "b"), 1)])?;
    assert_eq!(source.try_map(|(key, _)| Ok(*key))?, ZSet::from_updates([(1, 1)])?);
    Ok(())
}

#[test]
fn filter_and_map_are_linear_against_raw_updates() -> Result<()> {
    for seed in 1..=128_i64 {
        let a = ZSet::from_updates((0..16).map(|n| ((n % 5, n % 3), (seed + n) % 5 - 2)))?;
        let b = ZSet::from_updates((0..16).map(|n| ((n % 5, n % 3), (seed * n) % 5 - 2)))?;
        let transform = |input: &ZSet<(i64, i64)>| {
            input.try_filter(|(key, _)| Ok(*key >= 2))?.try_map(|(_, value)| Ok(*value))
        };
        let mut sum = a.clone();
        sum.apply(&b)?;
        let mut separately = transform(&a)?;
        separately.apply(&transform(&b)?)?;
        assert_eq!(transform(&sum)?, separately);
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
        input.try_filter(|value| Ok(value.is_some_and(|n| n >= 2)))?,
        ZSet::from_updates([(Some(2), -1)])?
    );
    assert!(input.try_filter(|_| Err(anyhow!("predicate failed"))).is_err());
    assert!(input.try_map::<i32>(|_| Err(anyhow!("projection failed"))).is_err());
    assert_eq!(input, before);
    let large = ZSet::from_updates([(1, i64::MAX), (2, 1)])?;
    assert!(large.try_map(|_| Ok(0)).is_err());
    Ok(())
}
