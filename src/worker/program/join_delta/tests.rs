use super::*;
use crate::compiler::ColumnRef;
fn spec() -> Spec {
    Spec { right: vec![ColumnRef { right: true, name: "r".into(), oid: 23, nullable: true }] }
}
fn bag(left: i64, right: i64) -> Result<Batch<(), Row>> {
    Batch::from_updates([
        (((), [(SIDE.into(), Some("left".into())), ("l".into(), Some("1".into()))].into()), left),
        (((), [(SIDE.into(), Some("right".into())), ("r".into(), Some("2".into()))].into()), right),
    ])
}
fn expected(left: i64, right: i64) -> Result<Batch<(), Row>> {
    let row =
        [("l".into(), Some("1".into())), ("r".into(), (right > 0).then(|| "2".into()))].into();
    Batch::from_updates([(((), row), left * right.max(1))])
}
#[test]
fn weighted_transitions_include_cross_term_and_null_extension() -> Result<()> {
    for old in [(0, 0), (0, 3), (2, 0), (2, 1), (2, 3)] {
        for new in [(0, 0), (0, 3), (2, 0), (2, 1), (2, 3)] {
            let delta = bag(new.0 - old.0, new.1 - old.1)?;
            let actual = evaluate(
                (&spec(), Limits::default()),
                &bag(old.0, old.1)?,
                &bag(new.0, new.1)?,
                &delta,
                &mut PartitionWork::for_test(1000),
            )?;
            let old_output = expected(old.0, old.1)?;
            let new_output = expected(new.0, new.1)?;
            let expected = Batch::from_updates(
                old_output
                    .iter()
                    .map(|(row, w)| (row.clone(), -*w))
                    .chain(new_output.iter().map(|(row, w)| (row.clone(), *w))),
            )?;
            assert_eq!(actual, expected, "{old:?} -> {new:?}");
        }
    }
    Ok(())
}
#[test]
fn hot_key_computation_is_linear_in_inputs_plus_actual_pairs() -> Result<()> {
    for size in [8, 32, 128] {
        let before = Batch::from_updates((0..size).flat_map(|id| {
            [
                (
                    (
                        (),
                        [(SIDE.into(), Some("left".into())), ("l".into(), Some(id.to_string()))]
                            .into(),
                    ),
                    1,
                ),
                (
                    (
                        (),
                        [(SIDE.into(), Some("right".into())), ("r".into(), Some(id.to_string()))]
                            .into(),
                    ),
                    1,
                ),
            ]
        }))?;
        let delta = Batch::from_updates([(
            ((), [(SIDE.into(), Some("right".into())), ("r".into(), Some("new".into()))].into()),
            1,
        )])?;
        let after = Batch::from_updates(
            before.iter().chain(delta.iter()).map(|(row, w)| (row.clone(), *w)),
        )?;
        let budget = usize::try_from(5 * size + 2)?;
        let mut work = PartitionWork::for_test(budget);
        let output = evaluate((&spec(), Limits::default()), &before, &after, &delta, &mut work)?;
        assert_eq!(output.iter().count(), usize::try_from(size)?);
        assert_eq!(work.remaining_for_test(), 0);
    }
    Ok(())
}
