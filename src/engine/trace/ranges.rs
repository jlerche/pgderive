//! Select block extents while retaining cold immutable membership without uploads.
use super::Run;
use crate::engine::reader::BatchData;
use anyhow::Result;

// Physical maintenance granularity is independent of index navigation fanout.
const MIN_MERGE_BLOCKS: usize = 32;

pub(super) struct Parts<K: BatchData, V: BatchData> {
    pub(super) retained: Vec<Run<K, V>>,
    pub(super) merging: Option<Run<K, V>>,
}
async fn parts<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    selected: &[bool],
) -> Result<Vec<Parts<K, V>>> {
    let anchor = runs
        .iter()
        .zip(selected)
        .filter(|(_, selected)| **selected)
        .min_by_key(|(run, _)| (usize::from(!matches!(run, Run::Memory(_))), run.row_count()))
        .and_then(|(run, _)| run.key_bounds());
    let mut parts = Vec::with_capacity(runs.len());
    for (run, selected) in runs.iter().zip(selected) {
        let mut part = Parts { retained: Vec::new(), merging: None };
        if !selected {
            part.retained.push(run.clone());
        } else if let (Run::Object(object), Some((lower, upper))) = (run, anchor) {
            // Keep the physical merge quantum stable when navigation geometry
            // changes; smaller fragments create debt in moving-key workloads.
            if object.block_count() <= MIN_MERGE_BLOCKS {
                part.merging = Some(run.clone());
                parts.push(part);
                continue;
            }
            let range = object.key_blocks(lower, upper).await?;
            if range.is_empty() {
                part.retained.push(run.clone());
            } else {
                if range.start > 0 {
                    part.retained.push(Run::Object(object.slice(0..range.start).await?));
                }
                if range.end < object.block_count() {
                    part.retained
                        .push(Run::Object(object.slice(range.end..object.block_count()).await?));
                }
                part.merging = Some(Run::Object(object.slice(range).await?));
            }
        } else {
            part.merging = Some(run.clone());
        }
        parts.push(part);
    }
    // Fragmentation must not force an immediate whole-trace rewrite. Keep the
    // existing whole-run selection when there is no room for retained extents.
    let retained_rows = parts
        .iter()
        .zip(selected)
        .filter(|(_, selected)| **selected)
        .flat_map(|(part, _)| &part.retained)
        .map(Run::row_count)
        .sum::<usize>();
    let selected_rows = parts
        .iter()
        .filter_map(|part| part.merging.as_ref())
        .filter(|run| matches!(run, Run::Object(_)))
        .map(Run::row_count)
        .sum::<usize>();
    let localized = parts
        .iter()
        .zip(selected)
        .any(|(part, selected)| *selected && part.merging.is_some() && !part.retained.is_empty());
    if !localized
        || retained_rows <= selected_rows
        || parts.iter().map(|part| part.retained.len()).sum::<usize>() + runs.len() > 128
    {
        return Ok(runs
            .iter()
            .zip(selected)
            .map(|(run, selected)| Parts {
                retained: if *selected { Vec::new() } else { vec![run.clone()] },
                merging: selected.then(|| run.clone()),
            })
            .collect());
    }
    Ok(parts)
}

pub(super) struct Selection<K: BatchData, V: BatchData> {
    pub(super) retained: Vec<Run<K, V>>,
    pub(super) retained_cohorts: Vec<usize>,
    pub(super) merging: Vec<Run<K, V>>,
}
pub(super) async fn select<K: BatchData, V: BatchData>(
    runs: &[Run<K, V>],
    cohorts: &[usize],
    selected: &[bool],
    writer: &crate::engine::dataflow::Arrangement<K, V>,
) -> Result<Selection<K, V>> {
    let mut parts = parts(runs, selected).await?;
    let mut selected = selected.to_vec();
    super::budget::restrict(runs, cohorts, &mut selected, &mut parts)?;
    for part in &mut parts {
        if let Some(Run::Object(object)) = &mut part.merging {
            object.prepare_extent().await?;
        }
    }
    let mut retained = Vec::new();
    let mut retained_cohorts = Vec::new();
    let mut merging = Vec::new();
    let mut remainders = Vec::new();
    let mut offset = 0_usize;
    for count in cohorts.iter().copied() {
        let mut kept = 0_usize;
        let group = &parts[offset..offset + count];
        merging.extend(group.iter().filter_map(|part| part.merging.clone()));
        let flags = &selected[offset..offset + count];
        remainders.extend(
            group
                .iter()
                .zip(flags)
                .filter(|(_, selected)| **selected)
                .flat_map(|(part, _)| part.retained.iter().cloned()),
        );
        for run in group
            .iter()
            .zip(flags)
            .filter(|(_, selected)| !**selected)
            .flat_map(|(part, _)| &part.retained)
        {
            if let Run::Memory(batch) = run {
                let uploaded = writer.upload_flush(batch).await?;
                kept += uploaded.len();
                retained.extend(uploaded);
            } else {
                kept += 1;
                retained.push(run.clone());
            }
        }
        if kept > 0 {
            retained_cohorts.push(kept);
        }
        offset += count;
    }
    // Untouched extents share one frozen scheduling cohort. They remain
    // separate weighted runs, charged by their real rows and root count;
    // another hot flush must not immediately reconsolidate their cold data.
    if !remainders.is_empty() {
        retained_cohorts.push(remainders.len());
        retained.extend(remainders);
    }
    Ok(Selection { retained, retained_cohorts, merging })
}
