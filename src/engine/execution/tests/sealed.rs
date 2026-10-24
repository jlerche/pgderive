use super::*;
use crate::engine::execution::OpenInput;
fn limits() -> Limits {
    Limits {
        resident_entries: 2,
        spill_runs: 2,
        contributions: 10_000,
        output_entries: 2,
        ..Limits::default()
    }
}
#[tokio::test]
async fn input_spills_merge_with_bounded_fanin_and_finalize_only_after_seal() -> Result<()> {
    let mut input = OpenInput::new(2, limits())?;
    for key in 0..100 {
        input.push(0, key, "value".to_owned(), i64::MAX)?;
        input.push(0, key, "value".to_owned(), 1)?;
    }
    for key in 0..100 {
        input.push(0, key, "value".to_owned(), -i64::MAX)?;
    }
    let sealed = input.seal(7)?;
    assert_eq!(sealed.time, 7);
    assert_eq!(sealed.batch[0].len(), 100);
    assert!(sealed.batch[1].is_empty());
    assert!(sealed.batch[0].materialize(limits()).await.is_err());
    let mut cursor = sealed.batch[0].cursor().await?;
    for key in 0..100 {
        assert_eq!(cursor.current().map(|(found, _, weight)| (*found, weight)), Some((key, 1)));
        cursor.advance().await?;
    }
    assert!(cursor.current().is_none());
    Ok(())
}
#[tokio::test]
async fn resident_and_spilled_cursors_seek_backwards_and_pin_owned_scratch() -> Result<()> {
    let batch = Batch::from_updates((0..10).flat_map(|key| [((key, 0), 1), ((key, 1), 1)]))?;
    let mut builder = Consolidator::new(limits())?;
    for (tuple, weight) in batch.iter() {
        builder.add(*tuple, *weight)?;
    }
    let scratch = builder.seal()?;
    let path = match &scratch.storage {
        Storage::Scratch(owner) => owner.run.path.clone(),
        Storage::Resident(_) => anyhow::bail!("fixture did not spill"),
    };
    for sealed in [Sealed::from(batch), scratch.clone()] {
        let mut cursor = sealed.cursor().await?;
        cursor.seek_key(&3).await?;
        cursor.advance().await?;
        assert_eq!(cursor.current().map(|(key, value, _)| (*key, *value)), Some((3, 1)));
        cursor.seek_key(&3).await?;
        assert_eq!(cursor.current().map(|(key, value, _)| (*key, *value)), Some((3, 0)));
        for target in [7, 1, 12, 0, 9] {
            cursor.seek_key(&target).await?;
            assert_eq!(cursor.current().map(|(key, _, _)| *key), (target < 10).then_some(target));
        }
    }
    let cursor = scratch.cursor().await?;
    drop(scratch);
    assert!(path.exists());
    drop(cursor);
    assert!(!path.exists());
    Ok(())
}
#[tokio::test]
async fn corrupt_scratch_advance_preserves_position_on_repeated_failure() -> Result<()> {
    use std::io::{Read as _, Seek as _, Write as _};
    let mut builder = Consolidator::new(limits())?;
    for key in 0..10 {
        builder.add((key, key), 1)?;
    }
    let sealed = builder.seal()?;
    let path = match &sealed.storage {
        Storage::Scratch(owner) => owner.run.path.clone(),
        Storage::Resident(_) => anyhow::bail!("fixture did not spill"),
    };
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
    let mut length = [0; 4];
    file.read_exact(&mut length)?;
    let second = 36 + u64::from(u32::from_le_bytes(length));
    file.seek(std::io::SeekFrom::Start(second + 4))?;
    file.write_all(&[0; 32])?;
    file.flush()?;
    let mut cursor = sealed.cursor().await?;
    assert!(cursor.advance().await.is_err());
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current().map(|(key, _, _)| *key), Some(0));
    drop(cursor);
    drop(sealed);
    assert!(path.exists());
    std::fs::remove_dir_all(path.parent().context("scratch parent missing")?)?;
    Ok(())
}
#[test]
fn permanent_capacity_causes_survive_codec_and_context_wrapping() -> Result<()> {
    let error = super::super::record::size_value(&"oversized row", 2)
        .err()
        .context("accepted oversized record")?;
    assert!(error.is::<super::super::CapacityExceeded>(), "{error:#}");
    let wrapped = error.context("source ingestion");
    assert!(wrapped.is::<super::super::CapacityExceeded>());
    Ok(())
}
#[tokio::test]
async fn exact_cancellation_can_seal_an_empty_disk_input() -> Result<()> {
    let mut input = OpenInput::new(1, limits())?;
    for key in 0..20 {
        input.push(0, key, key, 1)?;
    }
    for key in 0..20 {
        input.push(0, key, key, -1)?;
    }
    let sealed = input.seal(1)?;
    assert!(sealed.batch[0].is_empty());
    assert!(sealed.batch[0].materialize(limits()).await?.iter().next().is_none());
    Ok(())
}

#[tokio::test]
async fn sealed_statistics_finalize_once_after_all_group_contributions() -> Result<()> {
    use crate::engine::dataflow::{Arrangement, Statistics};
    use num_bigint::BigInt;
    let operator = Statistics::new(
        1,
        |row: &Vec<i64>| Ok(vec![BigInt::from(row[1])]),
        |row: &Vec<i64>| Ok(vec![BigInt::from(row[0])]),
        |_: &i64, values: &[BigInt]| Ok(Some(vec![i64::try_from(values[0].clone())?])),
    );
    let mut input = OpenInput::new(1, limits())?;
    for (id, value) in [(1, i64::MAX), (2, 1), (3, -1)] {
        input.push(0, 7, vec![id, value], 1)?;
    }
    let sealed = input.seal(1)?;
    let writer = Arrangement::new(
        Arc::new(object_store::memory::InMemory::new()),
        "statistics-sealed".into(),
        1,
    )?;
    let work = Limits { output_entries: 64, ..limits() };
    let result = operator.evaluate_sealed(&sealed.batch[0], &writer.empty(), work).await?;
    assert_eq!(result.materialize(work).await?, Batch::from_updates([((7, vec![i64::MAX]), 1)])?);
    Ok(())
}

#[tokio::test]
async fn corrupt_scratch_open_and_seek_preserve_failure_evidence() -> Result<()> {
    use std::io::{Seek as _, Write as _};
    for seek in [false, true] {
        let mut builder = Consolidator::new(limits())?;
        for key in 0..10 {
            builder.add((key, key), 1)?;
        }
        let sealed = builder.seal()?;
        let path = match &sealed.storage {
            Storage::Scratch(owner) => owner.run.path.clone(),
            Storage::Resident(_) => anyhow::bail!("fixture did not spill"),
        };
        let mut cursor = if seek { Some(sealed.cursor().await?) } else { None };
        let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
        file.seek(std::io::SeekFrom::Start(4))?;
        file.write_all(&[0; 32])?;
        file.flush()?;
        if let Some(cursor) = &mut cursor {
            assert!(cursor.seek_key(&9).await.is_err());
            assert_eq!(cursor.current().map(|(key, _, _)| *key), Some(0));
        } else {
            assert!(sealed.cursor().await.is_err());
        }
        drop(cursor);
        drop(sealed);
        assert!(path.exists());
        std::fs::remove_dir_all(path.parent().context("scratch parent missing")?)?;
    }
    Ok(())
}

#[tokio::test]
async fn whole_record_truncation_is_not_clean_eof() -> Result<()> {
    use std::io::Read as _;
    for empty in [false, true] {
        let mut builder = Consolidator::new(limits())?;
        for key in 0..10 {
            builder.add((key, key), 1)?;
        }
        let sealed = builder.seal()?;
        let path = match &sealed.storage {
            Storage::Scratch(owner) => owner.run.path.clone(),
            Storage::Resident(_) => anyhow::bail!("fixture did not spill"),
        };
        let mut length = [0; 4];
        File::open(&path)?.read_exact(&mut length)?;
        let boundary = if empty { 0 } else { 36 + u64::from(u32::from_le_bytes(length)) };
        std::fs::OpenOptions::new().write(true).open(&path)?.set_len(boundary)?;
        if empty {
            assert!(sealed.cursor().await.is_err());
        } else {
            let mut cursor = sealed.cursor().await?;
            assert!(cursor.seek_key(&10).await.is_err());
            assert!(cursor.advance().await.is_err());
            assert!(cursor.advance().await.is_err());
            assert_eq!(cursor.current().map(|(key, _, _)| *key), Some(0));
        }
        drop(sealed);
        assert!(path.exists());
        std::fs::remove_dir_all(path.parent().context("scratch parent missing")?)?;
    }
    Ok(())
}
