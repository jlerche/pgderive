//! Bounded scratch consolidation; physical spill boundaries never narrow weights.
mod capacity;
mod input;
mod levels;
mod merge;
pub use input::{OpenInput, SealedInput};
mod sealed;
pub use capacity::CapacityExceeded;
pub(crate) use sealed::group;
pub use sealed::{Sealed, SealedCursor};
mod record;

use crate::engine::{Batch, reader::BatchData};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{collections::BTreeMap, fs::File, io::BufWriter};
use tempfile::TempDir;

/// Explicit per-operator work limits. Byte budgets measure serialized records;
/// entry limits also bound container overhead. Scratch is disposable local state.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Separate cap for retained native values, including decoded handles.
    /// Zero disables native retention; transient decoding is outside this cap.
    pub native_cache_bytes: usize,
    /// Shared query cache cap for decoded structural-plus-payload charges.
    /// Active pins, indexes and allocator overhead are outside this cap; not RSS.
    pub cache_bytes: usize,
    /// Shared query cache entry cap; zero disables retention.
    pub cache_entries: usize,
    /// Maximum encoded bytes retained in the consolidation map.
    pub resident_bytes: usize,
    /// Maximum distinct identities retained before spilling.
    pub resident_entries: usize,
    /// Maximum bytes for one scratch record, including its exact coefficient.
    pub record_bytes: usize,
    /// Maximum input contributions, including join fanout.
    pub contributions: usize,
    /// Maximum simultaneously opened spill runs.
    pub spill_runs: usize,
    /// Maximum total scratch bytes written. Merge reads runs without another copy.
    pub scratch_bytes: u64,
    /// Maximum encoded bytes in the finalized logical batch.
    pub output_bytes: u64,
    /// Maximum identities in the finalized logical batch.
    pub output_entries: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            native_cache_bytes: 4 * 1024 * 1024,
            cache_bytes: 16 * 1024 * 1024,
            cache_entries: 256,
            resident_bytes: 4 * 1024 * 1024,
            resident_entries: 4096,
            record_bytes: 64 * 1024,
            contributions: 10_000_000,
            spill_runs: 128,
            scratch_bytes: 1024 * 1024 * 1024,
            output_bytes: 64 * 1024 * 1024,
            output_entries: 250_000,
        }
    }
}
impl Limits {
    /// Check a finalized incoming edge before invoking operator callbacks.
    ///
    /// # Errors
    /// Rejects incoming records, entry counts, or byte sizes outside the budget.
    pub fn check_batch<K: BatchData, V: BatchData>(self, batch: &Batch<K, V>) -> Result<()> {
        self.validate()?;
        let entries = batch.iter().count();
        ensure!(
            entries <= self.output_entries && entries <= self.contributions,
            CapacityExceeded("input entry limit exceeded")
        );
        batch.encoded_size(self.record_bytes, self.output_bytes)?;
        Ok(())
    }
    /// Validate limits before allocating scratch or evaluating callbacks.
    ///
    /// # Errors
    /// Rejects zero limits or records larger than the resident byte budget.
    pub fn validate(self) -> Result<Self> {
        ensure!(
            self.record_bytes > 0
                && self.record_bytes <= self.resident_bytes
                && self.resident_entries > 0
                && self.contributions > 0
                && self.spill_runs > 0
                && self.scratch_bytes > 0
                && self.output_bytes > 0
                && self.output_entries > 0,
            "invalid execution limits"
        );
        Ok(self)
    }
}

pub(crate) fn record_size(value: &impl serde::Serialize, limit: usize) -> Result<usize> {
    record::size_value(value, limit)
}

pub(crate) fn weight_size(
    tuple: &impl serde::Serialize,
    weight: &BigInt,
    limit: usize,
) -> Result<usize> {
    record::size(tuple, weight, limit)
}

struct Resident {
    weight: BigInt,
    tuple_bytes: usize,
    bytes: usize,
}

/// Exact, spillable full-identity consolidation for one logical batch.
/// No i64 representability check occurs until the complete merged result exists.
pub struct Consolidator<T: BatchData> {
    limits: Limits,
    directory: TempDir,
    entries: BTreeMap<T, Resident>,
    bytes: usize,
    contributions: usize,
    runs: Vec<record::Run>,
    levels: Vec<usize>,
    next_run: u64,
    written: u64,
    failed: bool,
}
impl<T: BatchData> Consolidator<T> {
    /// Create isolated disposable scratch for one operator evaluation.
    ///
    /// # Errors
    /// Returns invalid budget or scratch filesystem errors.
    pub fn new(limits: Limits) -> Result<Self> {
        let limits = limits.validate()?;
        let mut directory = tempfile::Builder::new().prefix("pgderive-spill-").tempdir()?;
        directory.disable_cleanup(true);
        Ok(Self {
            limits,
            directory,
            entries: BTreeMap::new(),
            bytes: 0,
            contributions: 0,
            runs: Vec::new(),
            levels: Vec::new(),
            next_run: 0,
            written: 0,
            failed: false,
        })
    }
    /// Add an exact contribution, spilling before exceeding resident limits.
    ///
    /// # Errors
    /// Returns resource, serialization, or scratch I/O failures.
    pub fn add(&mut self, tuple: T, weight: impl Into<BigInt>) -> Result<()> {
        ensure!(!self.failed, "failed consolidator cannot be reused");
        let result = self.add_inner(tuple, weight.into());
        self.failed = result.is_err();
        result.with_context(|| {
            format!("scratch retained on failure: {}", self.directory.path().display())
        })
    }
    fn add_inner(&mut self, tuple: T, weight: BigInt) -> Result<()> {
        ensure!(
            self.contributions < self.limits.contributions,
            CapacityExceeded("contribution limit exceeded")
        );
        self.contributions += 1;
        let previous = self.entries.get(&tuple);
        let next = previous.map_or_else(|| weight.clone(), |old| &old.weight + &weight);
        let tuple_bytes = previous.map_or_else(
            || record::size_value(&tuple, self.limits.record_bytes),
            |old| Ok(old.tuple_bytes),
        )?;
        let bytes = record::weighted_size(tuple_bytes, &next, self.limits.record_bytes)?;
        let old_bytes = previous.map_or(0, |old| old.bytes);
        let required = self
            .bytes
            .checked_sub(old_bytes)
            .and_then(|retained| retained.checked_add(bytes))
            .context("resident byte count overflow")?;
        if required > self.limits.resident_bytes
            || (previous.is_none() && self.entries.len() == self.limits.resident_entries)
        {
            self.spill()?;
            // BTreeMap keeps its original key on an occupied insert. After a
            // spill this incoming representative becomes the retained key, so
            // charge its own serialization even if ordering compares equal.
            let tuple_bytes = record::size_value(&tuple, self.limits.record_bytes)?;
            let bytes = record::weighted_size(tuple_bytes, &weight, self.limits.record_bytes)?;
            self.bytes += bytes;
            self.entries.insert(tuple, Resident { weight, tuple_bytes, bytes });
        } else {
            self.bytes = required;
            self.entries.insert(tuple, Resident { weight: next, tuple_bytes, bytes });
        }
        Ok(())
    }
    fn spill(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let path = self.next_path()?;
        let before = self.written;
        let mut writer = BufWriter::new(File::create_new(&path)?);
        for (tuple, resident) in &self.entries {
            let bytes = record::encode(tuple, &resident.weight, self.limits.record_bytes)?;
            self.written =
                record::write(&mut writer, &bytes, self.written, self.limits.scratch_bytes)?;
        }
        std::io::Write::flush(&mut writer)?;
        drop(writer);
        self.runs.push(record::Run {
            path,
            bytes: self.written.checked_sub(before).context("scratch byte count decreased")?,
        });
        self.levels.push(0);
        if self.runs.len() > self.limits.spill_runs {
            self.merge_level()?;
        }
        self.entries.clear();
        self.bytes = 0;
        Ok(())
    }
    /// Merge physical runs exactly and visit canonical nonzero identities.
    ///
    /// # Errors
    /// Returns scratch, integrity, budget, or consumer failures. The consumer must
    /// stage unpublished work; earlier visits do not authorize publication.
    pub fn finish(self, mut visit: impl FnMut(T, BigInt) -> Result<()>) -> Result<()> {
        self.finish_sized(|tuple, weight, _| visit(tuple, weight))
    }
    fn finish_sized(mut self, visit: impl FnMut(T, BigInt, usize) -> Result<()>) -> Result<()> {
        let result = self.finish_inner(visit);
        match result {
            Ok(()) => self.directory.close().context("removing successful operator scratch"),
            Err(error) => Err(error).with_context(|| {
                format!("scratch retained on failure: {}", self.directory.path().display())
            }),
        }
    }
    fn finish_inner(&mut self, visit: impl FnMut(T, BigInt, usize) -> Result<()>) -> Result<()> {
        ensure!(!self.failed, "failed consolidator cannot be finalized");
        if self.runs.is_empty() {
            let rows = std::mem::take(&mut self.entries)
                .into_iter()
                .filter(|(_, resident)| resident.weight != BigInt::default())
                .map(|(tuple, resident)| Ok((tuple, resident.weight, resident.bytes)));
            return finish_rows(self.limits, rows, visit);
        }
        self.spill()?;
        let mut merge = merge::Merge::open(&self.runs, self.limits.record_bytes)?;
        let rows = std::iter::from_fn(|| merge.next().transpose()).map(|row| {
            let (tuple, weight) = row?;
            let bytes = record::size(&tuple, &weight, self.limits.record_bytes)?;
            Ok((tuple, weight, bytes))
        });
        finish_rows(self.limits, rows, visit)
    }
}
fn finish_rows<T: BatchData>(
    limits: Limits,
    rows: impl Iterator<Item = Result<(T, BigInt, usize)>>,
    mut visit: impl FnMut(T, BigInt, usize) -> Result<()>,
) -> Result<()> {
    let mut bytes = 0_u64;
    let mut count = 0_usize;
    for row in rows {
        let (tuple, weight, record_bytes) = row?;
        count += 1;
        ensure!(count <= limits.output_entries, CapacityExceeded("output entry limit exceeded"));
        bytes = bytes
            .checked_add(u64::try_from(record_bytes)?)
            .context("output byte count overflow")?;
        ensure!(bytes <= limits.output_bytes, CapacityExceeded("output byte limit exceeded"));
        visit(tuple, weight, record_bytes)?;
    }
    Ok(())
}
impl<K: BatchData, V: BatchData> Consolidator<(K, V)> {
    /// Finalize coefficients into an explicitly bounded in-memory logical batch.
    ///
    /// # Errors
    /// Returns merge/resource errors or final i64 coefficient overflow.
    pub fn finish_batch(self) -> Result<Batch<K, V>> {
        let mut rows = Vec::new();
        let mut size = super::batch_size::BatchSize { entries: 0, bytes: 0, largest: 0 };
        self.finish_sized(|tuple, weight, bytes| {
            // Scratch weights are quoted decimal strings; canonical i64 JSON
            // has exactly the same digits without the two quote characters.
            let bytes = bytes.checked_sub(2).context("invalid weighted record size")?;
            size.entries += 1;
            size.bytes =
                size.bytes.checked_add(u64::try_from(bytes)?).context("batch size overflow")?;
            size.largest = size.largest.max(bytes);
            rows.push((
                tuple,
                i64::try_from(weight).context("finalized weight exceeds i64 domain")?,
            ));
            Ok(())
        })?;
        let batch = Batch::from_canonical(rows)?;
        let _ = batch.sizing.set(size);
        Ok(batch)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

impl<K: BatchData, V: BatchData> Consolidator<(K, V)> {
    /// Seal a complete logical edge without materializing its total payload.
    ///
    /// # Errors
    /// Returns scratch, integrity, capacity or final coefficient errors.
    pub fn seal(mut self) -> Result<Sealed<K, V>> {
        if self.runs.is_empty() {
            return Ok(self.finish_batch()?.into());
        }
        let result = self.seal_disk();
        result.with_context(|| {
            format!("scratch retained on failure: {}", self.directory.path().display())
        })
    }
    fn seal_disk(&mut self) -> Result<Sealed<K, V>> {
        self.spill()?;
        let mut merge = merge::Merge::<(K, V)>::open(&self.runs, self.limits.record_bytes)?;
        let path = self.directory.path().join("sealed");
        let before = self.written;
        let mut writer = BufWriter::new(File::create_new(&path)?);
        let mut entries = 0_usize;
        while let Some((tuple, weight)) = merge.next()? {
            let checked = i64::try_from(weight)?;
            let bytes = record::encode(&tuple, &BigInt::from(checked), self.limits.record_bytes)?;
            self.written =
                record::write(&mut writer, &bytes, self.written, self.limits.scratch_bytes)?;
            entries = entries.checked_add(1).context("sealed entry count overflow")?;
        }
        std::io::Write::flush(&mut writer)?;
        for run in &self.runs {
            std::fs::remove_file(&run.path)?;
        }
        if entries == 0 {
            self.directory.disable_cleanup(false);
            return Ok(Batch::default().into());
        }
        let bytes = self.written.checked_sub(before).context("scratch byte count decreased")?;
        let replacement = tempfile::Builder::new().prefix("pgderive-sealed-move-").tempdir()?;
        let directory = std::mem::replace(&mut self.directory, replacement);
        Ok(Sealed::scratch(
            directory,
            record::Run { path, bytes },
            entries,
            self.limits.record_bytes,
        ))
    }
}
