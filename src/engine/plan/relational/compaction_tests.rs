use super::{Entry, compact_entry};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
};
use anyhow::Result;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;
#[tokio::test]
async fn selective_compaction_preserves_single_roots_and_only_writes_multirun_state() -> Result<()>
{
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "selective-compaction-v1".into(), 1)?;
    let singleton = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((9, 100), 1)])? },
        )
        .await?;
    let first = writer
        .stage(&writer.empty(), &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 2)])? })
        .await?;
    let multiple = writer
        .stage(
            &first,
            &TimedBatch { time: 2, batch: Batch::from_updates([((0, 1), -1), ((0, 2), 1)])? },
        )
        .await?;
    let one = Entry { schema: "selective-compaction-v1".into(), trace: singleton };
    let many = Entry { schema: "selective-compaction-v1".into(), trace: multiple };
    let before = store.list(None).try_collect::<Vec<_>>().await?;
    let scoped = writer.clone().with_namespace("pgderive/upload-v1/selective-compaction-test");
    let unchanged = compact_entry(&one, &scoped).await?;
    let compacted = compact_entry(&many, &scoped).await?;
    assert_eq!(unchanged.trace.manifest()?, one.trace.manifest()?);
    assert_eq!(compacted.trace.time(), many.trace.time());
    assert_eq!(compacted.trace.run_count(), 1);
    assert_eq!(
        compacted.trace.materialize().await?,
        Batch::from_updates([((0, 1), 1), ((0, 2), 1)])?
    );
    let after = store.list(None).try_collect::<Vec<_>>().await?;
    let added = after
        .iter()
        .filter(|object| !before.iter().any(|prior| prior.location == object.location))
        .count();
    assert_eq!(added, 3, "only two consolidated blocks and one root should be written");
    let reopened = scoped.reopen(compacted.trace.manifest()?).await?;
    assert_eq!(reopened.materialize().await?, many.trace.materialize().await?);
    let empty = Entry { schema: one.schema.clone(), trace: writer.empty() };
    assert_eq!(compact_entry(&empty, &scoped).await?.trace.manifest()?, empty.trace.manifest()?);
    Ok(())
}
