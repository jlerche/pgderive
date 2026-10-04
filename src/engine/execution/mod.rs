//! Bounded scratch consolidation; physical spill boundaries never narrow weights.
mod merge;
mod record;

use crate::engine::{Batch, reader::BatchData};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{collections::BTreeMap, fs::File, io::BufWriter, path::PathBuf};
use tempfile::TempDir;

/// Explicit per-operator work limits. Byte budgets measure serialized records;
/// entry limits also bound container overhead. Scratch is disposable local state.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
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
        let mut bytes = 0_u64;
        for (ordinal, (tuple, weight)) in batch.iter().enumerate() {
            ensure!(
                ordinal < self.output_entries && ordinal < self.contributions,
                "input entry limit exceeded"
            );
            bytes = bytes
                .checked_add(u64::try_from(record_size(&(tuple, weight), self.record_bytes)?)?)
                .context("input byte count overflow")?;
            ensure!(bytes <= self.output_bytes, "input byte limit exceeded");
        }
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
    Ok(record::encode_value(value, limit)?.len())
}

/// Exact, spillable full-identity consolidation for one logical batch.
/// No i64 representability check occurs until the complete merged result exists.
pub struct Consolidator<T: BatchData> {
    limits: Limits,
    directory: TempDir,
    entries: BTreeMap<T, (BigInt, usize)>,
    bytes: usize,
    contributions: usize,
    runs: Vec<PathBuf>,
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
        ensure!(self.contributions < self.limits.contributions, "contribution limit exceeded");
        self.contributions += 1;
        let previous = self.entries.get(&tuple);
        let next = previous.map_or_else(|| weight.clone(), |(old, _)| old + &weight);
        let bytes = record::encode(&tuple, &next, self.limits.record_bytes)?.len();
        let old_bytes = previous.map_or(0, |(_, bytes)| *bytes);
        let required = self
            .bytes
            .checked_sub(old_bytes)
            .and_then(|retained| retained.checked_add(bytes))
            .context("resident byte count overflow")?;
        if required > self.limits.resident_bytes
            || (previous.is_none() && self.entries.len() == self.limits.resident_entries)
        {
            self.spill()?;
            let bytes = record::encode(&tuple, &weight, self.limits.record_bytes)?.len();
            self.bytes += bytes;
            self.entries.insert(tuple, (weight, bytes));
        } else {
            self.bytes = required;
            self.entries.insert(tuple, (next, bytes));
        }
        Ok(())
    }
    fn spill(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        ensure!(self.runs.len() < self.limits.spill_runs, "spill run limit exceeded");
        let path = self.directory.path().join(format!("run-{}", self.runs.len()));
        let mut writer = BufWriter::new(File::create_new(&path)?);
        for (tuple, (weight, _)) in &self.entries {
            let bytes = record::encode(tuple, weight, self.limits.record_bytes)?;
            self.written =
                record::write(&mut writer, &bytes, self.written, self.limits.scratch_bytes)?;
        }
        std::io::Write::flush(&mut writer)?;
        self.runs.push(path);
        self.entries.clear();
        self.bytes = 0;
        Ok(())
    }
    /// Merge physical runs exactly and visit canonical nonzero identities.
    ///
    /// # Errors
    /// Returns scratch, integrity, budget, or consumer failures. The consumer must
    /// stage unpublished work; earlier visits do not authorize publication.
    pub fn finish(mut self, visit: impl FnMut(T, BigInt) -> Result<()>) -> Result<()> {
        let result = self.finish_inner(visit);
        match result {
            Ok(()) => self.directory.close().context("removing successful operator scratch"),
            Err(error) => Err(error).with_context(|| {
                format!("scratch retained on failure: {}", self.directory.path().display())
            }),
        }
    }
    fn finish_inner(&mut self, mut visit: impl FnMut(T, BigInt) -> Result<()>) -> Result<()> {
        ensure!(!self.failed, "failed consolidator cannot be finalized");
        self.spill()?;
        let mut merge = merge::Merge::open(&self.runs, self.limits.record_bytes)?;
        let mut bytes = 0_u64;
        let mut count = 0_usize;
        while let Some((tuple, weight)) = merge.next()? {
            count += 1;
            ensure!(count <= self.limits.output_entries, "output entry limit exceeded");
            bytes = bytes
                .checked_add(u64::try_from(
                    record::encode(&tuple, &weight, self.limits.record_bytes)?.len(),
                )?)
                .context("output byte count overflow")?;
            ensure!(bytes <= self.limits.output_bytes, "output byte limit exceeded");
            visit(tuple, weight)?;
        }
        Ok(())
    }
}
impl<K: BatchData, V: BatchData> Consolidator<(K, V)> {
    /// Finalize coefficients into an explicitly bounded in-memory logical batch.
    ///
    /// # Errors
    /// Returns merge/resource errors or final i64 coefficient overflow.
    pub fn finish_batch(self) -> Result<Batch<K, V>> {
        let mut rows = Vec::new();
        self.finish(|tuple, weight| {
            rows.push((
                tuple,
                i64::try_from(weight).context("finalized weight exceeds i64 domain")?,
            ));
            Ok(())
        })?;
        Batch::from_updates(rows)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
