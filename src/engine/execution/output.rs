//! Bounded output builders; only unchanged-identity filters certify ordering.
use super::{CapacityExceeded, Limits, Sealed, charge_contribution, record};
use crate::engine::{
    Batch,
    batch_size::BatchSize,
    reader::{BatchData, Entry},
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write as _},
};

// Each identity arrives once with its finalized coefficient. Keep one bounded
// resident chunk, then append canonical records to a single checksummed run.
pub struct Ordered<K: BatchData, V: BatchData> {
    limits: Limits,
    rows: Vec<Entry<K, V>>,
    resident_bytes: usize,
    contributions: usize,
    bytes: u64,
    sizing: BatchSize,
    disk: Option<Box<Disk>>,
}
struct Disk {
    directory: tempfile::TempDir,
    writer: BufWriter<File>,
    written: u64,
    buffer: Vec<u8>,
}
impl<K: BatchData, V: BatchData> Ordered<K, V> {
    pub(crate) fn new(limits: Limits) -> Result<Self> {
        Ok(Self {
            limits: limits.validate()?,
            rows: Vec::new(),
            resident_bytes: 0,
            contributions: 0,
            bytes: 0,
            sizing: BatchSize { entries: 0, bytes: 0, largest: 0 },
            disk: None,
        })
    }
    pub(crate) fn add(&mut self, tuple: (K, V), weight: i64) -> Result<()> {
        charge_contribution(&mut self.contributions, self.limits.contributions)?;
        ensure!(weight != 0, "zero ordered output coefficient");
        ensure!(self.rows.last().is_none_or(|last| last.0 < tuple), "noncanonical ordered output");
        // The existing scratch format quotes decimal coefficients. Integer JSON
        // has identical digits, so sizing needs no temporary BigInt or string.
        let native_bytes =
            record::size_value(&(&tuple, weight), self.limits.record_bytes.saturating_sub(2))?;
        let bytes = native_bytes.checked_add(2).context("ordered record size overflow")?;
        if self.rows.len() == self.limits.resident_entries
            || bytes > self.limits.resident_bytes.saturating_sub(self.resident_bytes)
        {
            self.spill()?;
        }
        // Check ordering before spilling: the incoming row remains as the next
        // chunk's last identity, so no extra representative clone is retained.
        if self.rows.len() == self.rows.capacity() {
            let capacity =
                self.rows.capacity().max(1).saturating_mul(2).min(self.limits.resident_entries);
            self.rows.reserve_exact(capacity - self.rows.len());
        }
        self.rows.push((tuple, weight));
        self.resident_bytes += bytes;
        self.bytes =
            self.bytes.checked_add(u64::try_from(bytes)?).context("ordered byte count overflow")?;
        self.sizing.entries += 1;
        self.sizing.bytes = self
            .sizing
            .bytes
            .checked_add(u64::try_from(native_bytes)?)
            .context("batch size overflow")?;
        self.sizing.largest = self.sizing.largest.max(native_bytes);
        Ok(())
    }
    fn spill(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        if self.disk.is_none() {
            let directory = tempfile::Builder::new().prefix("pgderive-filter-").tempdir()?;
            let writer = BufWriter::new(File::create_new(directory.path().join("sealed"))?);
            self.disk = Some(Box::new(Disk { directory, writer, written: 0, buffer: Vec::new() }));
        }
        let disk = self.disk.as_deref_mut().context("missing ordered scratch")?;
        for (tuple, weight) in self.rows.drain(..) {
            record::encode(
                &tuple,
                &BigInt::from(weight),
                self.limits.record_bytes,
                &mut disk.buffer,
            )?;
            disk.written = record::write(
                &mut disk.writer,
                &disk.buffer,
                disk.written,
                self.limits.scratch_bytes,
            )?;
        }
        self.resident_bytes = 0;
        Ok(())
    }
    pub(crate) fn failure(mut self, error: anyhow::Error) -> anyhow::Error {
        error.context(self.failure_context())
    }
    fn failure_context(&mut self) -> String {
        self.disk.as_mut().map_or_else(
            || "ordered output failed before creating scratch".into(),
            |disk| {
                disk.directory.disable_cleanup(true);
                format!("scratch retained on failure: {}", disk.directory.path().display())
            },
        )
    }
    pub(crate) fn finish_batch(mut self) -> Result<Batch<K, V>> {
        let result = self.collect();
        result.with_context(|| self.failure_context())
    }
    fn collect(&mut self) -> Result<Batch<K, V>> {
        ensure!(
            self.sizing.entries <= self.limits.output_entries,
            CapacityExceeded("output entry limit exceeded")
        );
        ensure!(
            self.bytes <= self.limits.output_bytes,
            CapacityExceeded("output byte limit exceeded")
        );
        if self.disk.is_some() {
            self.spill()?;
            let disk = self.disk.as_deref_mut().context("missing ordered scratch")?;
            disk.writer.flush()?;
            let mut reader = BufReader::new(File::open(disk.directory.path().join("sealed"))?);
            let mut sizing = BatchSize { entries: 0, bytes: 0, largest: 0 };
            let mut bytes = 0_u64;
            while let Some((tuple, weight)) = record::read_complete(
                &mut reader,
                self.limits.record_bytes,
                disk.written,
                &mut disk.buffer,
            )? {
                ensure!(
                    self.rows.len() < self.sizing.entries,
                    "ordered scratch entry count mismatch"
                );
                let weight = i64::try_from(weight)?;
                let native_bytes = record::size_value(
                    &(&tuple, weight),
                    self.limits.record_bytes.saturating_sub(2),
                )?;
                bytes = bytes
                    .checked_add(u64::try_from(
                        native_bytes.checked_add(2).context("ordered record size overflow")?,
                    )?)
                    .context("ordered byte count overflow")?;
                ensure!(
                    bytes <= self.limits.output_bytes,
                    CapacityExceeded("output byte limit exceeded")
                );
                sizing.entries += 1;
                sizing.bytes = sizing
                    .bytes
                    .checked_add(u64::try_from(native_bytes)?)
                    .context("batch size overflow")?;
                sizing.largest = sizing.largest.max(native_bytes);
                self.rows.push((tuple, weight));
            }
            ensure!(self.rows.len() == self.sizing.entries, "ordered scratch entry count mismatch");
            self.sizing = sizing;
        }
        let batch = Batch::from_canonical(std::mem::take(&mut self.rows))?;
        let _ = batch.sizing.set(self.sizing);
        Ok(batch)
    }
    pub(crate) fn seal(mut self) -> Result<Sealed<K, V>> {
        if self.disk.is_none() {
            return Ok(self.finish_batch()?.into());
        }
        let result = self.spill().and_then(|()| {
            self.disk.as_deref_mut().context("missing ordered scratch")?.writer.flush()?;
            Ok(())
        });
        result.with_context(|| self.failure_context())?;
        let disk = self.disk.take().context("missing ordered scratch")?;
        let Disk { directory, writer, written, .. } = *disk;
        drop(writer);
        let path = directory.path().join("sealed");
        Ok(Sealed::scratch(
            directory,
            record::Run { path, bytes: written },
            self.sizing.entries,
            self.limits.record_bytes,
        ))
    }
}

#[cfg(test)]
#[path = "tests/output.rs"]
mod tests;
