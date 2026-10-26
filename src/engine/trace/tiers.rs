//! Size-tiered logical runs; a multipack upload remains one run across restart.
use super::{Run, TraceSnapshot};
use crate::engine::reader::BatchData;
use anyhow::{Context, Result, ensure};

const TIER_FAN_IN: usize = 8;

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(crate) fn consolidation_due(&self) -> Result<bool> {
        Ok((self.run_count() >= super::CONSOLIDATION_ROOTS && self.cohorts.len() > 1)
            || select_tier(&self.runs, &self.cohorts)?.contains(&true))
    }
    pub(crate) async fn consolidate_tiers(
        &self,
        writer: &crate::engine::dataflow::Arrangement<K, V>,
    ) -> Result<Self> {
        ensure!(!self.has_memory(), "consolidation requires durable input");
        let runs = &self.runs;
        let selected = if runs.len() >= super::CONSOLIDATION_ROOTS && self.cohorts.len() > 1 {
            vec![true; self.cohorts.len()]
        } else {
            select_tier(runs, &self.cohorts)?
        };
        if !selected.contains(&true) {
            return Ok(self.clone());
        }
        let mut retained = Vec::new();
        let mut retained_cohorts = Vec::new();
        let mut merging = Vec::new();
        let mut offset = 0_usize;
        for (count, selected) in self.cohorts.iter().copied().zip(selected) {
            let cohort = &runs[offset..offset + count];
            if selected {
                merging.extend_from_slice(cohort);
            } else {
                retained_cohorts.push(count);
                retained.extend_from_slice(cohort);
            }
            offset += count;
        }
        if !merging.is_empty() {
            let subset = Self::from_runs(merging, self.generation, self.time)?;
            let compacted = match subset.compact_with(writer).await {
                Ok(compacted) => compacted,
                Err(error) if error.is::<super::merged::CoefficientOverflow>() => {
                    return writer.compact(self).await;
                }
                Err(error) => return Err(error),
            };
            if compacted.run_count() > 0 {
                retained_cohorts.push(compacted.run_count());
                retained.extend(compacted.runs);
            }
        }
        if retained.len() > super::MAX_RUNS {
            return writer.compact(self).await;
        }
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let mut next = Self::from_runs(retained, generation, self.time)?;
        next.cohorts = retained_cohorts;
        next.logical_cache.clone_from(&self.logical_cache);
        Ok(next)
    }
}

fn select_tier<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    cohorts: &[usize],
) -> Result<Vec<bool>> {
    if let Some(selected) = super::overlap::select(runs, cohorts)? {
        return Ok(selected);
    }
    let mut tiers = Vec::with_capacity(cohorts.len());
    let mut counts = vec![0_usize; usize::try_from(usize::BITS)?];
    let mut offset = 0_usize;
    for count in cohorts {
        let end = offset.checked_add(*count).context("trace cohort count overflow")?;
        let cohort = runs.get(offset..end).context("invalid trace run cohorts")?;
        let rows = cohort.iter().try_fold(0_usize, |total, run| {
            let rows = match run {
                Run::Object(object) => object.row_count(),
                Run::Memory(batch) => batch.iter().count(),
            };
            total.checked_add(rows).context("flush row count overflow")
        })?;
        // Empty results have a real immutable root. Charge one scheduling unit
        // rather than rejecting valid empty arrangements or calling ilog2(0).
        let tier = usize::try_from(rows.max(1).ilog2())?;
        counts[tier] += 1;
        tiers.push(tier);
        offset = end;
    }
    ensure!(offset == runs.len(), "invalid trace run cohorts");
    let selected = counts.iter().position(|count| *count >= TIER_FAN_IN);
    Ok(tiers.into_iter().map(|tier| Some(tier) == selected).collect())
}
