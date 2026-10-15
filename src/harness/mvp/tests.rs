use super::{Batch, Query, Row, operators, registered_plan, visible_row};
use crate::engine::{dataflow::TimedBatch, execution::Limits, plan::Settings};
use anyhow::Result;
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::test]
async fn relational_fixture_preserves_simultaneous_changes_and_cold_restart() -> Result<()> {
    let settings = Settings {
        store: Arc::new(InMemory::new()),
        block_rows: 1,
        limits: Limits::default(),
        encoded_cache: None,
    };
    let mut graph = Query::new(registered_plan()?, operators(), &settings)?;
    let auction = Row::from([("category".into(), Some("7".into()))]);
    let bid = Row::from([("id".into(), Some("9".into())), ("price".into(), Some("205".into()))]);
    let first = graph
        .prepare(TimedBatch {
            time: 1,
            batch: vec![
                Batch::from_updates([(("1".into(), auction.clone()), 1)])?,
                Batch::from_updates([(("1".into(), bid.clone()), 1)])?,
            ],
        })
        .await?;
    assert_eq!(
        first.output().batch,
        Batch::from_updates([((String::new(), visible_row(Some("7".into()), 1, Some(205))), 1)])?
    );
    graph.commit(first)?;
    let checkpoint = graph.checkpoint()?;
    let mut reopened =
        Query::reopen(registered_plan()?, operators(), settings, checkpoint.clone()).await?;
    assert_eq!(reopened.checkpoint()?, checkpoint);
    let pinned = reopened.snapshot();
    let null_group = Row::from([("category".into(), None)]);
    let null_price = Row::from([("id".into(), Some("9".into())), ("price".into(), None)]);
    let second = reopened
        .prepare(TimedBatch {
            time: 2,
            batch: vec![
                Batch::from_updates([(("1".into(), auction), -1), (("1".into(), null_group), 1)])?,
                Batch::from_updates([(("1".into(), bid), -1), (("1".into(), null_price), 1)])?,
            ],
        })
        .await?;
    assert_eq!(
        second.output().batch,
        Batch::from_updates([
            ((String::new(), visible_row(Some("7".into()), 1, Some(205))), -1),
            ((String::new(), visible_row(None, 1, None)), 1)
        ])?
    );
    reopened.commit(second)?;
    assert_eq!(pinned.trace("output")?.time(), 1);
    assert_eq!(
        reopened.snapshot().trace("output")?.materialize().await?,
        Batch::from_updates([((String::new(), visible_row(None, 1, None)), 1)])?
    );
    Ok(())
}
