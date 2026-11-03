//! Shared payload addressing preserves independent integrity and bounded output.
use super::*;
use crate::engine::{
    Batch,
    tests::store::{Store, corrupt_block},
};
use anyhow::{Context as _, Result};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use object_store::ObjectStore as _;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test(flavor = "current_thread")]
async fn shared_pack_damage_is_confined_to_the_authenticated_arrangement_range() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batches =
        [Batch::from_updates([((1_i64, 2_i64), -3)])?, Batch::from_updates([((7, 8), 4)])?];
    let roots =
        packing::scope(store.clone(), None, 2, async {
            stream::iter(batches.iter().enumerate().map(|(index, batch)| {
            let store = store.clone();
            async move { ObjectBatch::write(store, batch, &format!("schema-{index}"), 16).await }
        })).buffered(2).collect::<Vec<_>>().await.into_iter().collect::<Result<Vec<_>>>()
        })
        .await?;
    assert_eq!(store.puts.load(Ordering::SeqCst), 3);
    let first = ObjectBatch::<i64, i64>::open(store.clone(), roots[0].clone(), "schema-0").await?;
    let second = ObjectBatch::<i64, i64>::open(store.clone(), roots[1].clone(), "schema-1").await?;
    let first_range = first.block_range(0).await?.context("missing first range")?;
    let second_range = second.block_range(0).await?.context("missing second range")?;
    assert_eq!(first_range.0, second_range.0);
    assert!(first_range.1.end <= second_range.1.start);
    corrupt_block(store.as_ref(), &first, 0).await?;
    assert!(first.cursor().await.is_err());
    assert_eq!(second.cursor().await?.current(), Some((&7, &8, 4)));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shared_large_writers_split_payloads_without_losing_rows() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batches = (0..2)
        .map(|side| {
            Batch::from_updates((0..10_000).map(|key| {
                let value = (0..16)
                    .map(|part| format::hash(format!("{side}-{key}-{part}").as_bytes()))
                    .collect::<String>();
                ((key, value), if key % 2 == 0 { 2 } else { -1 })
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let roots = packing::scope(store.clone(), None, 2, async {
        let work = batches.iter().map(|batch| {
            let store = store.clone();
            async move {
                ObjectBatch::write_stream(
                    store,
                    &mut batch.cursor().await?,
                    "large-shared",
                    WriteLimits::default(),
                )
                .await
            }
        });
        stream::iter(work)
            .buffered(2)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
    })
    .await?;
    for (batch, references) in batches.iter().zip(roots) {
        let mut actual = Vec::new();
        for reference in references {
            let object =
                ObjectBatch::<i32, String>::open(store.clone(), reference, "large-shared").await?;
            let mut cursor = object.cursor().await?;
            while let Some((key, value, weight)) = cursor.current() {
                actual.push(((*key, value.clone()), weight));
                cursor.advance().await?;
            }
        }
        assert_eq!(Batch::from_updates(actual)?, *batch);
    }
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    let packs = objects
        .iter()
        .filter(|object| object.location.as_ref().starts_with("pgderive/pack/"))
        .collect::<Vec<_>>();
    assert!(packs.len() > 2);
    let limit = u64::try_from(format::TARGET_OBJECT_BYTES)?;
    assert!(packs.iter().all(|object| object.size <= limit));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shared_upload_budget_exhaustion_survives_restart_without_more_requests() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let ledger = directory.path().join("class-a.json");
    let batch = Batch::from_updates([((1_i64, 2_i64), 1)])?;
    for attempt in 0..2 {
        let store = crate::storage::budgeted_test_store(ledger.clone(), 1)?;
        let objects: Arc<dyn object_store::ObjectStore> = store.clone();
        let result = packing::scope(objects.clone(), None, 2, async {
            let (left, right) = tokio::join!(
                ObjectBatch::write(objects.clone(), &batch, "budget-left", 16),
                ObjectBatch::write(objects.clone(), &batch, "budget-right", 16),
            );
            Ok((left?, right?))
        })
        .await;
        let error = result.err().context("expected shared upload budget exhaustion")?;
        assert!(crate::storage::operation_budget_exhausted(&error), "attempt {attempt}: {error:#}");
        let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(&ledger)?)?;
        assert_eq!(saved["used"], 1);
        assert_eq!(saved["remaining"], 0);
        assert_eq!(saved["puts"], 1);
        assert_eq!(saved["listing_pages"], 0);
        let service = store.service_stats().context("missing request instrumentation")?;
        if attempt == 0 {
            assert!(service.foreground.write_requested_bytes > 0);
        } else {
            assert_eq!(service.foreground.write_requested_bytes, 0);
            assert_eq!(service.foreground.written_bytes, 0);
        }
        let retained = store.page("", None, 100).await;
        // Even inventory is blocked before a remote request can exceed capacity.
        let error = retained.err().context("expected blocked listing")?;
        assert!(crate::storage::operation_budget_exhausted(&error.into()));
    }
    Ok(())
}
