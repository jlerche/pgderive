//! Restrict profitable cohort merges to complete roots in one navigation-key window.
use super::Run;
use crate::engine::reader::BatchData;
use anyhow::{Context as _, Result, ensure};

const ROW_BUDGET: usize = 131_072;
const MIN_COHORTS: usize = 2;

struct Root<'a, K> {
    bounds: Option<(&'a K, &'a K)>,
    rows: usize,
    cohort: usize,
    memory: bool,
    selected: bool,
}

pub(super) fn restrict<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    cohorts: &[usize],
    selected: &[bool],
) -> Result<Vec<bool>> {
    ensure!(cohorts.len() == selected.len(), "invalid window selection length");
    let mut roots = Vec::with_capacity(runs.len());
    let mut offset = 0_usize;
    for (cohort, (count, selected)) in cohorts.iter().zip(selected).enumerate() {
        ensure!(*count > 0, "empty window cohort");
        let end = offset.checked_add(*count).context("window cohort offset overflow")?;
        for run in runs.get(offset..end).context("invalid window cohort")? {
            roots.push(Root {
                bounds: run.key_bounds(),
                rows: run.row_count(),
                cohort,
                memory: matches!(run, Run::Memory(_)),
                selected: *selected,
            });
        }
        offset = end;
    }
    ensure!(offset == runs.len(), "invalid window cohort count");
    choose(&roots, cohorts.len())
}

fn choose<K: Ord>(roots: &[Root<'_, K>], cohorts: usize) -> Result<Vec<bool>> {
    let baseline = roots.iter().map(|root| root.selected).collect::<Vec<_>>();
    let baseline_rows = object_rows(roots, &baseline)?;
    let memory_bounds = roots
        .iter()
        .filter(|root| root.selected && root.memory)
        .filter_map(|root| root.bounds)
        .reduce(|(a, b), (c, d)| (a.min(c), b.max(d)));
    let anchors: Vec<(&K, &K)> = memory_bounds.map_or_else(
        || roots.iter().filter(|root| root.selected).filter_map(|root| root.bounds).collect(),
        |bounds| vec![bounds],
    );
    let mut best = baseline;
    let mut best_count = 0;
    let mut best_rows = baseline_rows;
    let mut defer = false;
    for bounds in anchors {
        let mut candidate = roots
            .iter()
            .map(|root| root.selected && (root.memory || overlaps(root.bounds, bounds)))
            .collect::<Vec<_>>();
        if participant_count(roots, &candidate, cohorts) < MIN_COHORTS
            || object_rows(roots, &candidate)? >= baseline_rows
        {
            continue;
        }
        defer |= total_rows(roots, &candidate)? <= ROW_BUDGET;
        balance(roots, &mut candidate)?;
        let Some((count, rows)) = score(roots, &candidate, cohorts)? else {
            continue;
        };
        if count > best_count || (count == best_count && rows < best_rows) {
            best = candidate;
            best_count = count;
            best_rows = rows;
        }
    }
    // A localized but unbalanced window should accumulate comparable peers,
    // rather than repeatedly rewriting its dominant root for tiny updates.
    if best_count == 0 && defer {
        return Ok(vec![false; roots.len()]);
    }
    Ok(best)
}

fn total_rows<K>(roots: &[Root<'_, K>], selected: &[bool]) -> Result<usize> {
    roots
        .iter()
        .zip(selected)
        .filter(|(_, selected)| **selected)
        .try_fold(0_usize, |rows, (root, _)| {
            rows.checked_add(root.rows).context("window row budget overflow")
        })
}

fn balance<K>(roots: &[Root<'_, K>], selected: &mut [bool]) -> Result<()> {
    loop {
        let total = total_rows(roots, selected)?;
        let largest = roots
            .iter()
            .zip(selected.iter())
            .enumerate()
            .filter(|(_, (root, selected))| **selected && !root.memory)
            .max_by_key(|(_, (root, _))| root.rows);
        let Some((index, (root, _))) = largest else {
            return Ok(());
        };
        if root.rows <= total / 2 {
            return Ok(());
        }
        selected[index] = false;
    }
}

fn overlaps<K: Ord>(bounds: Option<(&K, &K)>, (lower, upper): (&K, &K)) -> bool {
    bounds.is_some_and(|(first, last)| first <= upper && lower <= last)
}

fn object_rows<K>(roots: &[Root<'_, K>], selected: &[bool]) -> Result<usize> {
    roots
        .iter()
        .zip(selected)
        .filter(|(root, selected)| **selected && !root.memory)
        .try_fold(0_usize, |rows, (root, _)| {
            rows.checked_add(root.rows).context("window object row count overflow")
        })
}

fn participant_count<K>(roots: &[Root<'_, K>], selected: &[bool], cohorts: usize) -> usize {
    let mut participants = vec![false; cohorts];
    for (root, _) in roots.iter().zip(selected).filter(|(_, selected)| **selected) {
        participants[root.cohort] = true;
    }
    participants.into_iter().filter(|selected| *selected).count()
}

fn score<K>(
    roots: &[Root<'_, K>],
    selected: &[bool],
    cohorts: usize,
) -> Result<Option<(usize, usize)>> {
    if total_rows(roots, selected)? > ROW_BUDGET {
        return Ok(None);
    }
    let count = participant_count(roots, selected, cohorts);
    if count < MIN_COHORTS {
        return Ok(None);
    }
    Ok(Some((count, object_rows(roots, selected)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(bounds: &(i64, i64), rows: usize, cohort: usize, memory: bool) -> Root<'_, i64> {
        Root { bounds: Some((&bounds.0, &bounds.1)), rows, cohort, memory, selected: true }
    }

    #[test]
    fn retains_disjoint_roots_and_whole_selected_memory() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots = [
            root(&cold, 100, 0, false),
            root(&hot, 10, 0, false),
            root(&hot, 10, 1, false),
            root(&hot, 10, 2, false),
            root(&hot, 10, 3, true),
        ];
        assert_eq!(choose(&roots, 4)?, [false, true, true, true, true]);
        Ok(())
    }

    #[test]
    fn root_multiplicity_does_not_invent_independent_cohorts() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots = [
            root(&cold, 100, 0, false),
            root(&hot, 10, 0, false),
            root(&hot, 10, 0, false),
            root(&hot, 10, 0, false),
            root(&hot, 10, 0, false),
        ];
        assert_eq!(choose(&roots, 1)?, [true; 5]);
        Ok(())
    }

    #[test]
    fn oversized_dominant_root_is_excluded_before_checking_budget() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots = [
            root(&cold, 100, 0, false),
            root(&hot, ROW_BUDGET, 0, false),
            root(&hot, 1, 1, false),
            root(&hot, 1, 2, false),
            root(&hot, 1, 3, true),
        ];
        assert_eq!(choose(&roots, 4)?, [false, false, true, true, true]);
        Ok(())
    }

    #[test]
    fn object_only_windows_keep_duplicate_occurrences_and_inclusive_edges() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let edge = (1, 2);
        let roots = [
            root(&cold, 100, 0, false),
            root(&hot, 10, 0, false),
            root(&hot, 10, 1, false),
            root(&hot, 10, 1, false),
            root(&edge, 10, 2, false),
            root(&edge, 10, 3, false),
        ];
        assert_eq!(choose(&roots, 4)?, [false, true, true, true, true, true]);
        Ok(())
    }

    #[test]
    fn fragmented_cold_envelopes_do_not_force_unrelated_roots_into_hot_merges() -> Result<()> {
        let left = (-100, -1);
        let right = (2, 100);
        let hot = (0, 1);
        let roots = [
            root(&left, 100, 0, false),
            root(&right, 100, 0, false),
            root(&left, 100, 1, false),
            root(&right, 100, 1, false),
            root(&left, 100, 2, false),
            root(&right, 100, 2, false),
            root(&left, 100, 3, false),
            root(&right, 100, 3, false),
            root(&hot, 10, 4, false),
            root(&hot, 10, 5, true),
        ];
        assert_eq!(
            choose(&roots, 6)?,
            [false, false, false, false, false, false, false, false, true, true]
        );
        Ok(())
    }

    #[test]
    fn localized_dominant_hot_root_defers_until_comparable_peers_accumulate() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots = [root(&cold, 100, 0, false), root(&hot, 100, 0, false), root(&hot, 1, 1, true)];
        assert_eq!(choose(&roots, 2)?, [false; 3]);
        Ok(())
    }

    #[test]
    fn tiny_peers_merge_without_dominant_root_and_memory_remains_whole() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots = [
            root(&cold, 100, 0, false),
            root(&hot, 100, 0, false),
            root(&hot, 1, 1, false),
            root(&hot, 1, 2, false),
            root(&hot, 1, 3, false),
            root(&hot, 1, 4, true),
        ];
        assert_eq!(choose(&roots, 5)?, [false, false, true, true, true, true]);
        Ok(())
    }

    #[test]
    fn budget_failure_or_no_locality_alone_does_not_defer() -> Result<()> {
        let cold = (100, 200);
        let hot = (0, 1);
        let roots =
            [root(&cold, 100, 0, false), root(&hot, 1, 0, false), root(&hot, ROW_BUDGET, 1, true)];
        assert_eq!(choose(&roots, 2)?, [true; 3]);
        let no_locality = [root(&hot, 100, 0, false), root(&hot, 1, 1, true)];
        assert_eq!(choose(&no_locality, 2)?, [true; 2]);
        Ok(())
    }

    #[test]
    fn isolated_delta_preserves_whole_tier_merging_for_disjoint_cohorts() -> Result<()> {
        let first = (0, 1);
        let second = (2, 3);
        let third = (4, 5);
        let delta = (6, 7);
        let roots = [
            root(&first, 10, 0, false),
            root(&second, 10, 1, false),
            root(&third, 10, 2, false),
            root(&delta, 1, 3, true),
        ];
        assert_eq!(choose(&roots, 4)?, [true; 4]);
        Ok(())
    }

    #[test]
    fn malformed_cohorts_are_rejected_before_planning() -> Result<()> {
        let run = Run::Memory(std::sync::Arc::new(crate::engine::Batch::from_updates([(
            (1_i64, 2_i64),
            1,
        )])?));
        assert!(restrict(std::slice::from_ref(&run), &[1], &[]).is_err());
        assert!(restrict(std::slice::from_ref(&run), &[0], &[true]).is_err());
        assert!(restrict(std::slice::from_ref(&run), &[2], &[true]).is_err());
        assert!(restrict(&[run], &[], &[]).is_err());
        Ok(())
    }

    #[test]
    fn empty_bounds_do_not_drop_memory_or_create_profitable_object_windows() -> Result<()> {
        let bounds = (0, 1);
        let mut roots = (0..4).map(|cohort| root(&bounds, 0, cohort, false)).collect::<Vec<_>>();
        roots[0].bounds = None;
        assert_eq!(choose(&roots, 4)?, [true; 4]);
        Ok(())
    }
}
