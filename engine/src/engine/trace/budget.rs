//! Bound profitable physical subsets after extent localization, before uploads.
use super::{Run, ranges::Parts};
use crate::engine::reader::BatchData;
use anyhow::{Context as _, Result, ensure};

const BYTE_BUDGET: u64 = 32 * 1024 * 1024;
const FAN_IN: usize = 16;
const ROOT_PRESSURE: usize = 96;
#[derive(Clone, Copy, Default)]
struct Cost {
    rows: usize,
    bytes: u64,
    runs: usize,
}
impl Cost {
    fn add(self, other: Self) -> Result<Self> {
        Ok(Self {
            rows: self.rows.checked_add(other.rows).context("selection rows overflow")?,
            bytes: self.bytes.checked_add(other.bytes).context("selection bytes overflow")?,
            runs: self.runs.checked_add(other.runs).context("selection fan-in overflow")?,
        })
    }
    const fn fits(self) -> bool {
        self.bytes <= BYTE_BUDGET && self.runs <= FAN_IN
    }
}
struct Candidate {
    cost: Cost,
    cohort: usize,
    memory: bool,
}
fn cost<K: BatchData, V: BatchData>(run: &Run<K, V>) -> Result<Cost> {
    let bytes = match run {
        Run::Object(object) => object.payload_bytes(),
        Run::Memory(batch) => batch.encoded_size(usize::MAX, u64::MAX)?.bytes,
    };
    Ok(Cost { rows: run.row_count(), bytes, runs: 1 })
}
fn total(candidates: &[Option<Candidate>], flags: &[bool]) -> Result<Cost> {
    candidates
        .iter()
        .zip(flags)
        .filter_map(|(candidate, flag)| flag.then_some(candidate.as_ref()).flatten())
        .try_fold(Cost::default(), |sum, candidate| sum.add(candidate.cost))
}
fn balanced(candidates: &[Option<Candidate>], flags: &mut [bool]) -> Result<()> {
    loop {
        let sum = total(candidates, flags)?;
        let dominant = candidates
            .iter()
            .zip(flags.iter())
            .enumerate()
            .filter_map(|(i, (candidate, flag))| {
                candidate.as_ref().filter(|c| *flag && !c.memory).map(|c| (i, c))
            })
            .find(|(_, c)| c.cost.rows > sum.rows / 2 || c.cost.bytes > sum.bytes / 2);
        let Some((index, _)) = dominant else { return Ok(()) };
        flags[index] = false;
    }
}
fn participants(candidates: &[Option<Candidate>], flags: &[bool]) -> usize {
    candidates
        .iter()
        .zip(flags)
        .filter_map(|(candidate, flag)| candidate.as_ref().filter(|_| *flag).map(|c| c.cohort))
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}
fn choose(candidates: &[Option<Candidate>], pressure: bool) -> Result<Vec<bool>> {
    let baseline = candidates.iter().map(Option::is_some).collect::<Vec<_>>();
    let complete = total(candidates, &baseline)?;
    let oversized_memory = candidates.iter().flatten().any(|c| c.memory && !c.cost.fits());
    if complete.fits() || pressure || oversized_memory {
        return Ok(baseline);
    }
    let mut flags = vec![false; candidates.len()];
    let mut sum = Cost::default();
    // Keep complete memory input fused when possible, then give old roots first
    // admission. This does not continually prefer the cheapest/newest roots.
    let mut admitted = std::collections::BTreeSet::new();
    for pass in 0..3 {
        for (index, candidate) in
            candidates.iter().enumerate().filter_map(|(i, c)| c.as_ref().map(|c| (i, c)))
        {
            if flags[index]
                || match pass {
                    0 => !candidate.memory,
                    1 => candidate.memory || admitted.contains(&candidate.cohort),
                    _ => candidate.memory,
                }
            {
                continue;
            }
            let next = sum.add(candidate.cost)?;
            if next.fits() {
                flags[index] = true;
                sum = next;
                admitted.insert(candidate.cohort);
            }
        }
    }
    balanced(candidates, &mut flags)?;
    // A cap must not turn unsplittable history into permanent maintenance debt.
    // Retain the complete profitable job when no balanced multi-cohort subset
    // fits. Root pressure and coefficient cancellation have the same explicit
    // progress requirement; hierarchical membership is a separate change.
    if participants(candidates, &flags) < 2 {
        return Ok(baseline);
    }
    Ok(flags)
}
pub(super) fn restrict<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    cohorts: &[usize],
    selected: &mut [bool],
    parts: &mut [Parts<K, V>],
) -> Result<()> {
    ensure!(
        runs.len() == parts.len() && selected.len() == runs.len(),
        "invalid physical selection lengths"
    );
    let mut candidates = Vec::with_capacity(parts.len());
    let mut offset = 0_usize;
    for (cohort, count) in cohorts.iter().enumerate() {
        let end = offset.checked_add(*count).context("physical cohort overflow")?;
        for part in parts.get(offset..end).context("invalid physical cohort")? {
            candidates.push(
                part.merging
                    .as_ref()
                    .map(|run| {
                        Ok::<_, anyhow::Error>(Candidate {
                            cost: cost(run)?,
                            cohort,
                            memory: matches!(run, Run::Memory(_)),
                        })
                    })
                    .transpose()?,
            );
        }
        offset = end;
    }
    ensure!(offset == parts.len(), "invalid physical cohort count");
    let pressure =
        parts.iter().map(|p| p.retained.len() + usize::from(p.merging.is_some())).sum::<usize>()
            >= ROOT_PRESSURE;
    let flags = choose(&candidates, pressure)?;
    let selected_cost = total(&candidates, &flags)?;
    crate::engine::execution::service::selection(
        selected_cost.bytes,
        u64::try_from(selected_cost.runs)?,
        !selected_cost.fits(),
        pressure && !selected_cost.fits(),
    );
    for (index, keep) in flags.into_iter().enumerate() {
        if !keep && parts[index].merging.is_some() {
            parts[index] = Parts { retained: vec![runs[index].clone()], merging: None };
            selected[index] = false;
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod physical_tests;
