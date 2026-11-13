use super::*;
use crate::engine::reader::{BatchCursor as _, BatchReader as _};
fn limits() -> Limits {
    Limits { resident_entries: 2, spill_runs: 1, ..Limits::default() }
}
fn builder() -> Result<Ordered<i64, i64>> {
    Ordered::new(limits())
}
#[test]
fn ordered_builder_rejects_duplicate_descending_and_zero_weights_across_chunks() -> Result<()> {
    for (tuple, weight) in [((0, 1), 1), ((0, -1), 1), ((1, 1), 0)] {
        let mut output = builder()?;
        output.add((0, 0), 1)?;
        output.add((0, 1), 1)?;
        assert!(output.add(tuple, weight).is_err());
    }
    let mut output = builder()?;
    for value in 0..3 {
        output.add((0, value), 1)?;
    }
    assert!(output.add((0, 1), 1).is_err());
    Ok(())
}
#[tokio::test]
async fn one_run_is_written_once_and_lives_until_last_cursor_drops() -> Result<()> {
    let mut output = builder()?;
    for value in 0..9 {
        output.add((0, value), i64::MIN)?;
    }
    let expected_bytes = output.bytes + 9 * 36;
    let directory = output.disk.as_ref().context("did not spill")?.directory.path().to_owned();
    let sealed = output.seal()?;
    let path = directory.join("sealed");
    assert_eq!(std::fs::metadata(&path)?.len(), expected_bytes);
    assert_eq!(std::fs::read_dir(&directory)?.count(), 1);
    let mut cursor = sealed.cursor().await?;
    drop(sealed);
    for value in 0..9 {
        assert_eq!(cursor.current(), Some((&0, &value, i64::MIN)));
        cursor.advance().await?;
    }
    assert!(path.exists());
    drop(cursor);
    assert!(!directory.exists());
    Ok(())
}
#[test]
fn record_resident_scratch_and_output_limits_use_existing_wire_accounting() -> Result<()> {
    let mut normal = builder()?;
    for value in 0..4 {
        normal.add((0, value), 1)?;
    }
    let wire_bytes = normal.bytes;
    let disk_bytes = wire_bytes + 4 * 36;
    let mut exact = Ordered::new(Limits { scratch_bytes: disk_bytes, ..limits() })?;
    for value in 0..4 {
        exact.add((0, value), 1)?;
    }
    assert_eq!(exact.seal()?.len(), 4);
    let mut short = Ordered::new(Limits { scratch_bytes: disk_bytes - 1, ..limits() })?;
    for value in 0..4 {
        short.add((0, value), 1)?;
    }
    let directory = short.disk.as_ref().context("did not spill")?.directory.path().to_owned();
    assert!(short.seal().is_err());
    assert!(directory.exists());
    std::fs::remove_dir_all(directory)?;
    for output_bytes in [wire_bytes - 1, wire_bytes] {
        let mut output = Ordered::new(Limits { output_bytes, ..limits() })?;
        for value in 0..4 {
            output.add((0, value), 1)?;
        }
        let directory = output.disk.as_ref().context("did not spill")?.directory.path().to_owned();
        assert_eq!(output.finish_batch().is_ok(), output_bytes == wire_bytes);
        if directory.exists() {
            std::fs::remove_dir_all(directory)?;
        }
    }
    let mut small = Ordered::new(Limits { resident_bytes: 16, record_bytes: 16, ..limits() })?;
    assert!(small.add((0, 0), i64::MIN).is_err());
    let mut bytes = Ordered::new(Limits {
        resident_bytes: 16,
        record_bytes: 16,
        resident_entries: 64,
        ..limits()
    })?;
    for value in 0..4 {
        bytes.add((0, value), 1)?;
    }
    assert!(bytes.disk.is_some());
    Ok(())
}
#[test]
fn consumer_failure_retains_partial_output_and_successful_batch_removes_scratch() -> Result<()> {
    let mut output = builder()?;
    for value in 0..4 {
        output.add((0, value), 1)?;
    }
    let directory = output.disk.as_ref().context("did not spill")?.directory.path().to_owned();
    let error = output.failure(anyhow::anyhow!("consumer failed"));
    assert!(format!("{error:#}").contains("consumer failed"));
    assert!(directory.exists());
    std::fs::remove_dir_all(directory)?;
    let mut output = builder()?;
    for value in 0..4 {
        output.add((0, value), 1)?;
    }
    let directory = output.disk.as_ref().context("did not spill")?.directory.path().to_owned();
    assert_eq!(output.finish_batch()?.iter().count(), 4);
    assert!(!directory.exists());
    Ok(())
}

#[test]
fn corrupt_materialization_retains_evidence_and_dropped_unsealed_output_cleans_up() -> Result<()> {
    use std::io::{Read as _, Seek as _, Write as _};
    let mut output = builder()?;
    for value in 0..4 {
        output.add((0, value), 1)?;
    }
    let disk = output.disk.as_deref_mut().context("did not spill")?;
    disk.writer.flush()?;
    let directory = disk.directory.path().to_owned();
    let mut file =
        std::fs::OpenOptions::new().read(true).write(true).open(directory.join("sealed"))?;
    file.seek(std::io::SeekFrom::Start(8))?;
    let mut byte = [0_u8];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0x80;
    file.seek(std::io::SeekFrom::Start(8))?;
    file.write_all(&byte)?;
    drop(file);
    let error = output.finish_batch().err().context("accepted corrupt output")?;
    assert!(format!("{error:#}").contains("scratch checksum mismatch"));
    assert!(directory.exists());
    std::fs::remove_dir_all(directory)?;
    let mut output = builder()?;
    for value in 0..4 {
        output.add((0, value), 1)?;
    }
    let directory = output.disk.as_ref().context("did not spill")?.directory.path().to_owned();
    drop(output);
    assert!(!directory.exists());
    Ok(())
}
