//! Size-tiered exact scratch merges keep open descriptors bounded.
use super::{Consolidator, merge::Merge, record};
use crate::engine::reader::BatchData;
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::BufWriter,
};
impl<T: BatchData> Consolidator<T> {
    pub(super) fn merge_level(&mut self) -> Result<()> {
        let mut counts = BTreeMap::<usize, usize>::new();
        for level in &self.levels {
            *counts.entry(*level).or_default() += 1;
        }
        let (level, count) = counts
            .into_iter()
            .max_by_key(|(level, count)| (*count, std::cmp::Reverse(*level)))
            .context("no scratch levels")?;
        ensure!(self.limits.spill_runs >= 2, super::CapacityExceeded("spill run limit exceeded"));
        let ordinals = if count == 1 {
            let mut ranked = self
                .levels
                .iter()
                .enumerate()
                .map(|(index, level)| (*level, index))
                .collect::<Vec<_>>();
            ranked.sort_unstable();
            ranked.into_iter().take(2).map(|(_, index)| index).collect::<BTreeSet<_>>()
        } else {
            self.levels
                .iter()
                .enumerate()
                .filter(|(_, found)| **found == level)
                .map(|(index, _)| index)
                .take(self.limits.spill_runs)
                .collect()
        };
        let level = ordinals
            .iter()
            .map(|index| self.levels[*index])
            .max()
            .context("no merge input levels")?;
        let paths = ordinals.iter().map(|index| self.runs[*index].clone()).collect::<Vec<_>>();
        let mut merge = Merge::<T>::open(&paths, self.limits.record_bytes)?;
        let path = self.next_path()?;
        let before = self.written;
        let mut output = BufWriter::new(File::create_new(&path)?);
        while let Some((tuple, weight)) = merge.next()? {
            let bytes = record::encode(&tuple, &weight, self.limits.record_bytes)?;
            self.written =
                record::write(&mut output, &bytes, self.written, self.limits.scratch_bytes)?;
        }
        std::io::Write::flush(&mut output)?;
        drop(output);
        let mut retained = Vec::new();
        let mut levels = Vec::new();
        for (index, (path, found)) in self.runs.iter().zip(&self.levels).enumerate() {
            if ordinals.contains(&index) {
                std::fs::remove_file(&path.path)?;
            } else {
                retained.push(path.clone());
                levels.push(*found);
            }
        }
        retained.push(record::Run {
            path,
            bytes: self.written.checked_sub(before).context("scratch byte count decreased")?,
        });
        levels.push(level.checked_add(1).context("scratch level overflow")?);
        self.runs = retained;
        self.levels = levels;
        Ok(())
    }
    pub(super) fn next_path(&mut self) -> Result<std::path::PathBuf> {
        let next = self.next_run;
        self.next_run = self.next_run.checked_add(1).context("scratch run counter overflow")?;
        Ok(self.directory.path().join(format!("run-{next}")))
    }
}
