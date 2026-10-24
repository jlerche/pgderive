//! Bounded run-head reuse for affected-key probes at one immutable boundary.
use super::{KeyCursor, MergedCursor, Run, TraceSnapshot};
use crate::engine::reader::{BatchData, Cursor};
use anyhow::{Context as _, Result};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use std::collections::BTreeMap;

// Limit simultaneous selected-run GETs, including oversized single-row pages.
pub(super) const READ_CONCURRENCY: usize = 8;

/// Key probes retaining one reusable verified head per distinct reader.
///
/// Repeated reader occurrences keep their full weighted multiplicity.
/// Current and returned cursors can pin additional blocks independently.
/// Failed probes preserve the reusable heads.
pub struct KeyProbes<K: BatchData, V: BatchData> {
    runs: Vec<Run<K, V>>,
    slots: Vec<usize>,
    heads: Vec<Option<Cursor<K, V>>>,
    current: Option<LastProbe<K, V>>,
}
struct LastProbe<K: BatchData, V: BatchData> {
    key: K,
    cursor: KeyCursor<K, V>,
}
impl<K: BatchData, V: BatchData> KeyProbes<K, V> {
    pub(super) fn new(snapshot: TraceSnapshot<K, V>) -> Self {
        let mut runs: Vec<Run<K, V>> = Vec::new();
        let mut paths = BTreeMap::<String, Vec<usize>>::new();
        let mut slots = Vec::new();
        for run in snapshot.runs {
            let matching = if let Run::Object(batch) = &run {
                paths.get(batch.reference().path()).into_iter().flatten().copied().find(
                    |index| matches!(&runs[*index], Run::Object(prior) if batch.same_reader(prior)),
                )
            } else {
                None
            };
            let index = matching.unwrap_or_else(|| {
                let index = runs.len();
                if let Run::Object(batch) = &run {
                    paths.entry(batch.reference().path().to_owned()).or_default().push(index);
                }
                runs.push(run);
                index
            });
            slots.push(index);
        }
        let heads = vec![None; runs.len()];
        Self { runs, slots, heads, current: None }
    }
    /// Pin a fresh exact-key cursor, reusing verified blocks across affected keys.
    ///
    /// # Errors
    /// Returns selected-block integrity/read or consolidated coefficient errors.
    pub async fn cursor(&mut self, key: &K) -> Result<KeyCursor<K, V>> {
        if let Some(previous) = &self.current
            && &previous.key == key
        {
            return Ok(previous.cursor.clone());
        }
        let requests = self
            .runs
            .iter()
            .zip(&self.heads)
            .map(|(run, head)| (run.clone(), head.clone(), key.clone()))
            .collect::<Vec<_>>();
        let selected = stream::iter(requests)
            .map(|(run, head, key)| async move {
                match run {
                    Run::Memory(batch) => Ok(Some(Cursor::memory_key_reusing(
                        batch.shared_entries(),
                        &key,
                        head.as_ref(),
                    ))),
                    Run::Object(batch) => {
                        Cursor::object_key_reusing(batch, &key, head.as_ref()).await
                    }
                }
            })
            .buffered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut heads = self.heads.clone();
        for (head, selected) in heads.iter_mut().zip(&selected) {
            if let Some(cursor) = selected {
                *head = Some(cursor.clone());
            }
        }
        let mut cursors = Vec::new();
        for index in &self.slots {
            if let Some(cursor) = selected.get(*index).context("missing selected run head")?
                && cursor.current().is_some_and(|(found, _, _)| found == key)
            {
                cursors.push(cursor.clone());
            }
        }
        let mut cursor = MergedCursor { cursors, row: None };
        cursor.find().await?;
        let cursor = KeyCursor { cursor };
        self.heads = heads;
        self.current = Some(LastProbe { key: key.clone(), cursor: cursor.clone() });
        Ok(cursor)
    }
}
