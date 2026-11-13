use super::{Arrangement, CompactionJob, CompactionResult};
use crate::engine::{Batch, dataflow::TimedBatch, reader::BatchReader as _};
use anyhow::Result;
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::test]
async fn detached_roundtrip_rejects_stale_corrupt_and_inequivalent_results() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, String>::new(store.clone(), "tuple-v1".into(), 2)?;
    let first = writer
        .stage(
            &writer.empty(),
            &TimedBatch {
                time: 1,
                batch: Batch::from_updates([((1, "a".into()), 3), ((1, "b".into()), 1)])?,
            },
        )
        .await?;
    let second = writer
        .stage(
            &first,
            &TimedBatch {
                time: 2,
                batch: Batch::from_updates([((1, "a".into()), -1), ((2, "c".into()), 5)])?,
            },
        )
        .await?;
    let mut pinned = second.cursor().await?;
    let job = writer.make_job(&second, "pgderive/upload-v1/detached-test")?;
    assert_eq!(job.input(), &second.manifest()?);
    // The executor receives only serialized work plus object-store access.
    let detached: CompactionJob = serde_json::from_slice(&serde_json::to_vec(&job)?)?;
    let output = detached.execute::<i64, String>(store.clone()).await?;
    let result: CompactionResult = serde_json::from_slice(&serde_json::to_vec(&output)?)?;
    assert!(writer.adopt_compaction(&first, &job, result.clone()).await.is_err());
    let compacted = writer.adopt_compaction(&second, &job, result.clone()).await?;
    assert_eq!(compacted.time(), 2);
    assert_eq!(compacted.generation(), second.generation() + 1);
    assert_eq!(compacted.run_count(), 1);
    assert_eq!(pinned.current(), Some((&1, &"a".to_owned(), 2)));
    pinned.advance().await?;
    assert_eq!(pinned.current(), Some((&1, &"b".to_owned(), 1)));
    let wrong = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((9, "wrong".into()), 1)])? },
        )
        .await?;
    let wrong_job = writer.make_job(&wrong, "pgderive/upload-v1/detached-test")?;
    let wrong_output = wrong_job.execute::<i64, String>(store.clone()).await?;
    let mut changed = result.clone();
    changed.output.objects = wrong_output.output.objects;
    assert!(writer.adopt_compaction(&second, &job, changed).await.is_err());
    let mut foreign = result.clone();
    foreign.job.namespace = "pgderive/upload-v1/foreign".into();
    assert!(writer.adopt_compaction(&second, &job, foreign).await.is_err());
    let mut invalid = detached;
    invalid.version = 999;
    assert!(invalid.execute::<i64, String>(store.clone()).await.is_err());
    let erased: Arc<dyn object_store::ObjectStore> = store.clone();
    let block = result.output.objects[0].dependencies(&erased).await?[0].clone();
    object_store::ObjectStoreExt::delete(store.as_ref(), &object_store::path::Path::from(block))
        .await?;
    assert!(writer.adopt_compaction(&second, &job, result).await.is_err());
    Ok(())
}

#[tokio::test]
async fn detached_executor() -> Result<()> {
    let Some(directory) = std::env::var_os("PGDERIVE_COMPACTION_FIXTURE") else {
        return Ok(());
    };
    let directory = std::path::PathBuf::from(directory);
    let job: CompactionJob = serde_json::from_slice(&std::fs::read(directory.join("job.json"))?)?;
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.join("objects"))?);
    let result = job.execute::<i64, String>(store).await?;
    std::fs::write(directory.join("result.json"), serde_json::to_vec(&result)?)?;
    Ok(())
}

#[tokio::test]
async fn separate_process_can_execute_serialized_job() -> Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::create_dir(directory.path().join("objects"))?;
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        directory.path().join("objects"),
    )?);
    let writer = Arrangement::<i64, String>::new(store, "tuple-v1".into(), 2)?;
    let input = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((1, "one".into()), 7)])? },
        )
        .await?;
    let job = writer.make_job(&input, "pgderive/upload-v1/subprocess-test")?;
    std::fs::write(directory.path().join("job.json"), serde_json::to_vec(&job)?)?;
    let child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "engine::dataflow::compaction::tests::detached_executor", "--nocapture"])
        .env("PGDERIVE_COMPACTION_FIXTURE", directory.path())
        .kill_on_drop(true)
        .output()
        .await?;
    anyhow::ensure!(
        child.status.success(),
        "executor failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let result: CompactionResult =
        serde_json::from_slice(&std::fs::read(directory.path().join("result.json"))?)?;
    let output = writer.adopt_compaction(&input, &job, result).await?;
    assert_eq!(output.time(), 1);
    assert_eq!(output.generation(), 2);
    Ok(())
}

#[tokio::test]
async fn detached_adoption_rejects_equal_foreign_data_in_fresh_root() -> Result<()> {
    use crate::engine::{
        reader::{BlockCache, ObjectBatch, WriteLimits},
        trace::Run,
    };
    let store = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, String>::new(store.clone(), "tuple-v1".into(), 2)?;
    let input = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((1, "same".into()), 7)])? },
        )
        .await?;
    let job = writer.make_job(&input, "pgderive/upload-v1/provenance-job")?;
    let foreign = ObjectBatch::write_resident(
        store.clone(),
        &Batch::from_updates([((1_i64, "same".to_owned()), 7)])?,
        "tuple-v1",
        (Some("pgderive/upload-v1/unpinned-foreign"), 2),
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    let objects = ObjectBatch::compact_resident(
        store,
        &[Run::Object(foreign)],
        "tuple-v1",
        (Some(&job.namespace), WriteLimits { block_rows: 1, ..WriteLimits::default() }),
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    let result = CompactionResult {
        job: job.clone(),
        output: crate::engine::trace::Manifest {
            version: 2,
            time: input.time(),
            generation: input.generation() + 1,
            cohorts: vec![objects.len()],
            objects: objects.into_iter().map(|object| object.reference().clone()).collect(),
        },
    };
    assert!(
        result
            .output
            .objects
            .iter()
            .all(|object| { object.namespace() == Some(job.namespace.as_str()) })
    );
    let error = writer
        .adopt_compaction(&input, &job, result)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("adopted unpinned foreign dependency"))?;
    assert!(error.to_string().contains("unpinned foreign data"), "{error}");
    Ok(())
}
