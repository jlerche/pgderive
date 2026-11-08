use crate::engine::{
    Batch,
    dataflow::{Project, TimedBatch},
    execution::{Consolidator, Limits, Sealed},
    reader::{BatchCursor as _, BatchReader as _},
};
use anyhow::{Result, bail};

fn limits() -> Limits {
    Limits { resident_entries: 2, spill_runs: 2, contributions: 1000, ..Limits::default() }
}
fn input() -> Result<Batch<i64, i64>> {
    Batch::from_updates(
        (0..32_i64).map(|key| ((key / 4, key), if key % 2 == 0 { i64::MIN } else { i64::MAX })),
    )
}
fn predicate(key: i64, value: i64) -> Result<bool> {
    if value < 0 {
        bail!("negative fixture")
    }
    Ok((key + value) % 3 != 0)
}
async fn collect(sealed: &Sealed<i64, i64>) -> Result<Batch<i64, i64>> {
    let mut rows = Vec::new();
    let mut cursor = sealed.cursor().await?;
    while let Some((key, value, weight)) = cursor.current() {
        rows.push(((*key, *value), weight));
        cursor.advance().await?;
    }
    Batch::from_updates(rows)
}
#[tokio::test]
async fn unchanged_filter_preserves_complete_signed_identities_in_all_storage_modes() -> Result<()>
{
    let batch = input()?;
    let general = Project::new(|key: &i64, value: &i64| {
        Ok(predicate(*key, *value)?.then_some((*key, *value)))
    });
    let ordered = Project::filter(|key: &i64, value: &i64| predicate(*key, *value));
    let expected =
        general.evaluate_with_limits(&TimedBatch { time: 7, batch: &batch }, limits())?;
    let mut scratch = Consolidator::new(limits())?;
    for (tuple, weight) in batch.iter() {
        scratch.add(*tuple, *weight)?;
    }
    for source in [Sealed::from(batch.clone()), scratch.seal()?] {
        for resident_entries in [2, 64] {
            let limits = Limits { resident_entries, ..limits() };
            assert_eq!(
                ordered.evaluate_with_limits(&TimedBatch { time: 7, batch: &batch }, limits)?.batch,
                expected.batch
            );
            let mut contributions = 3;
            let filtered =
                ordered.evaluate_sealed_counted(&source, limits, &mut contributions).await?;
            assert_eq!(collect(&filtered).await?, expected.batch);
            assert_eq!(contributions, 3 + expected.batch.iter().count());
            let size = filtered
                .materialize(limits)
                .await?
                .encoded_size(limits.record_bytes, limits.output_bytes)?;
            let bytes =
                expected.batch.iter().try_fold(0_u64, |sum, (tuple, weight)| -> Result<_> {
                    Ok(sum + u64::try_from(serde_json::to_vec(&(tuple, weight))?.len())?)
                })?;
            assert_eq!(size.bytes, bytes);
        }
    }
    Ok(())
}
#[tokio::test]
async fn filter_seals_large_edges_with_one_run_and_enforces_materialization_limits() -> Result<()> {
    let source = Sealed::from(input()?);
    let limits = Limits { output_entries: 2, spill_runs: 1, ..limits() };
    let filtered =
        Project::filter(|_: &i64, _: &i64| Ok(true)).evaluate_sealed(&source, limits).await?;
    assert_eq!(filtered.len(), 32);
    assert!(filtered.materialize(limits).await.is_err());
    assert_eq!(collect(&filtered).await?, input()?);
    Ok(())
}
#[tokio::test]
async fn predicate_error_precedes_deferred_output_limit_and_leaves_input_unchanged() -> Result<()> {
    let batch = Batch::from_updates([((0, 0), 1), ((1, 1), -1)])?;
    let source = Sealed::from(batch.clone());
    let filter = Project::filter(|_: &i32, value: &i32| {
        if *value == 1 {
            bail!("predicate failure")
        }
        Ok(true)
    });
    let limits = Limits { output_entries: 1, ..limits() };
    let error = filter
        .evaluate_sealed(&source, limits)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("accepted predicate failure"))?;
    assert!(format!("{error:#}").contains("predicate failure"));
    assert_eq!(source.materialize(Limits::default()).await?, batch);
    Ok(())
}
#[tokio::test]
async fn filter_contribution_limit_counts_emissions_and_empty_output_has_no_rows() -> Result<()> {
    let source = Sealed::from(input()?);
    let keep = Project::filter(|_: &i64, _: &i64| Ok(true));
    let limits = Limits { contributions: 1, ..limits() };
    let mut count = 0;
    assert!(keep.evaluate_sealed_counted(&source, limits, &mut count).await.is_err());
    assert_eq!(count, 1);
    let drop = Project::filter(|_: &i64, _: &i64| Ok(false));
    let filtered = drop.evaluate_sealed_counted(&source, limits, &mut count).await?;
    assert!(filtered.is_empty());
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn predicate_failure_after_spill_retains_diagnostic_run() -> Result<()> {
    use anyhow::Context as _;
    let source = Sealed::from(input()?);
    let filter = Project::filter(|_: &i64, value: &i64| {
        if *value == 5 {
            bail!("late predicate failure")
        }
        Ok(true)
    });
    let error =
        filter.evaluate_sealed(&source, limits()).await.err().context("accepted failure")?;
    let path = error
        .chain()
        .find_map(|cause| {
            cause
                .to_string()
                .strip_prefix("scratch retained on failure: ")
                .map(std::path::PathBuf::from)
        })
        .context("missing scratch diagnostic")?;
    assert!(format!("{error:#}").contains("late predicate failure"));
    assert!(path.join("sealed").exists());
    assert_eq!(collect(&source).await?, input()?);
    std::fs::remove_dir_all(path)?;
    Ok(())
}
