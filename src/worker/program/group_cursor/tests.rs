use super::*;
use crate::engine::{
    dataflow::{Arrangement, TimedBatch},
    execution::Limits,
};
use object_store::memory::InMemory;
use std::sync::Arc;
fn tagged(left: bool, id: i64) -> Row {
    [
        (SIDE.into(), Some(if left { "left" } else { "right" }.into())),
        (if left { "l" } else { "r" }.into(), Some(id.to_string())),
    ]
    .into()
}
#[tokio::test]
async fn cursor_left_join_does_only_linear_visits_and_new_matching_pairs() -> Result<()> {
    let spec =
        Spec { right: vec![ColumnRef { right: true, name: "r".into(), oid: 23, nullable: true }] };
    for size in [8_i64, 32, 128] {
        let writer =
            Arrangement::<Key, Row>::new(Arc::new(InMemory::new()), "cursor-join-v1".into(), 8)?;
        let initial = TimedBatch {
            time: 1,
            batch: Batch::from_updates((0..size).flat_map(|id| {
                [
                    ((vec!["key".into()], tagged(true, id)), 1),
                    ((vec!["key".into()], tagged(false, id)), 1),
                ]
            }))?,
        };
        let prior = writer.stage(&writer.empty(), &initial).await?;
        let prior = writer.reopen(prior.manifest()?).await?;
        let delta = TimedBatch {
            time: 2,
            batch: Batch::from_updates([((vec!["key".into()], tagged(false, size)), 1)])?,
        };
        let budget = usize::try_from(5 * size + 2)?;
        let output = left_join(&spec)
            .evaluate(&delta, &prior, Limits { contributions: budget, ..Limits::default() })
            .await?;
        assert_eq!(
            output.batch,
            Batch::from_updates((0..size).map(|id| {
                (
                    (
                        vec!["key".into()],
                        [("l".into(), Some(id.to_string())), ("r".into(), Some(size.to_string()))]
                            .into(),
                    ),
                    1,
                )
            }))?
        );
        assert!(
            left_join(&spec)
                .evaluate(&delta, &prior, Limits { contributions: budget - 1, ..Limits::default() })
                .await
                .is_err()
        );
        assert_eq!(prior.time(), 1);
    }
    Ok(())
}
