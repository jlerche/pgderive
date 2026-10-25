//! Cross-tier consolidation for balanced overlapping logical runs.
use super::Run;
use crate::engine::reader::BatchData;
use anyhow::{Context as _, Result, ensure};

struct Cohort<'a, K> {
    rows: usize,
    bounds: Option<(&'a K, &'a K)>,
}
impl<K: BatchData, V: BatchData> Run<K, V> {
    fn key_bounds(&self) -> Option<(&K, &K)> {
        match self {
            Self::Object(batch) => batch.key_bounds(),
            Self::Memory(batch) => {
                let mut rows = batch.iter();
                let ((first, _), _) = rows.next()?;
                let last = rows.last().map_or(first, |((key, _), _)| key);
                Some((first, last))
            }
        }
    }
}
fn describe<K: BatchData, V: BatchData>(runs: &[Run<K, V>]) -> Result<Cohort<'_, K>> {
    let mut rows = 0_usize;
    let mut bounds: Option<(&K, &K)> = None;
    for run in runs {
        let count = match run {
            Run::Object(batch) => batch.row_count(),
            Run::Memory(batch) => batch.iter().count(),
        };
        rows = rows.checked_add(count).context("overlap cohort row count overflow")?;
        if let Some((first, last)) = run.key_bounds() {
            bounds = Some(
                bounds.map_or((first, last), |(lower, upper)| (lower.min(first), upper.max(last))),
            );
        }
    }
    Ok(Cohort { rows, bounds })
}
fn balanced<K>(cohorts: &[Cohort<'_, K>], mut selected: Vec<usize>) -> Result<Vec<usize>> {
    // A dominant base must not be rewritten for tiny peers. This is a bound on
    // physical row work, not on encoded byte widths or object request counts.
    while selected.len() >= 4 {
        let total = selected.iter().try_fold(0_usize, |total, index| {
            total.checked_add(cohorts[*index].rows).context("overlap row budget overflow")
        })?;
        let largest = selected
            .iter()
            .enumerate()
            .max_by_key(|(_, index)| cohorts[**index].rows)
            .map(|(position, _)| position)
            .context("empty overlap group")?;
        if cohorts[selected[largest]].rows <= total / 2 {
            return Ok(selected);
        }
        selected.remove(largest);
    }
    Ok(Vec::new())
}
pub(super) fn select<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    counts: &[usize],
) -> Result<Option<Vec<bool>>> {
    let mut cohorts = Vec::with_capacity(counts.len());
    let mut offset = 0_usize;
    for count in counts {
        ensure!(*count > 0, "empty overlap cohort");
        let end = offset.checked_add(*count).context("overlap cohort offset overflow")?;
        cohorts.push(describe(runs.get(offset..end).context("invalid overlap cohort")?)?);
        offset = end;
    }
    ensure!(offset == runs.len(), "invalid overlap cohort count");
    let mut best = Vec::new();
    let mut best_rows = usize::MAX;
    for anchor in cohorts.iter().filter_map(|cohort| cohort.bounds.map(|bounds| bounds.0)) {
        let selected = cohorts
            .iter()
            .enumerate()
            .filter(|(_, cohort)| cohort.bounds.is_some_and(|(a, b)| a <= anchor && anchor <= b))
            .map(|(index, _)| index)
            .collect();
        let selected = balanced(&cohorts, selected)?;
        let rows = selected.iter().try_fold(0_usize, |total, index| {
            total.checked_add(cohorts[*index].rows).context("selected overlap rows overflow")
        })?;
        if selected.len() > best.len() || (selected.len() == best.len() && rows < best_rows) {
            best = selected;
            best_rows = rows;
        }
    }
    Ok((!best.is_empty()).then(|| (0..counts.len()).map(|index| best.contains(&index)).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Batch;
    use std::sync::Arc;
    fn run(key: i64, rows: i64) -> Result<Run<i64, i64>> {
        Ok(Run::Memory(Arc::new(Batch::from_updates((0..rows).map(|v| ((key, v), 1)))?)))
    }
    #[test]
    fn cross_tier_history_is_balanced_without_inferring_unique_values() -> Result<()> {
        let runs = [1, 2, 4, 6].map(|rows| run(0, rows)).into_iter().collect::<Result<Vec<_>>>()?;
        assert_eq!(select(&runs, &[1; 4])?, Some(vec![true; 4]));
        let disjoint = (0..4).map(|key| run(key, 2)).collect::<Result<Vec<_>>>()?;
        assert!(select(&disjoint, &[1; 4])?.is_none());
        Ok(())
    }
    #[test]
    fn dominant_base_is_excluded_and_multipack_cohorts_remain_atomic() -> Result<()> {
        let runs =
            [1024, 1, 1, 1, 1].map(|rows| run(0, rows)).into_iter().collect::<Result<Vec<_>>>()?;
        assert_eq!(select(&runs, &[1; 5])?, Some(vec![false, true, true, true, true]));
        assert!(select(&runs[..4], &[1; 4])?.is_none());
        let packs = (0..8).map(|_| run(0, 1)).collect::<Result<Vec<_>>>()?;
        assert!(select(&packs, &[6, 1, 1])?.is_none());
        assert_eq!(select(&packs, &[2, 2, 2, 2])?, Some(vec![true; 4]));
        assert!(select(&packs, &[2, 2]).is_err());
        Ok(())
    }
    #[test]
    fn empty_runs_are_not_overlapping_and_sparse_envelopes_are_conservative() -> Result<()> {
        let empty = (0..4).map(|_| run(0, 0)).collect::<Result<Vec<_>>>()?;
        assert!(select(&empty, &[1; 4])?.is_none());
        let sparse = (0..4)
            .map(|i| {
                Ok(Run::Memory(Arc::new(Batch::from_updates([((i, i), 1), ((100 + i, i), 1)])?)))
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(select(&sparse, &[1; 4])?, Some(vec![true; 4]));
        Ok(())
    }
}
