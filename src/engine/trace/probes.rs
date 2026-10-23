//! Bounded run-head reuse for affected-key probes at one immutable boundary.
use super::{KeyCursor, MergedCursor, Run, TraceSnapshot};
use crate::engine::reader::{BatchData, Cursor};
use anyhow::{Context as _, Result};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use std::collections::BTreeMap;

// Limit simultaneous selected-run GETs, including oversized single-row pages.
pub(super) const READ_CONCURRENCY: usize = 8;

/// Key probes retaining one reusable verified head per run across groups.
/// Current and returned cursors can pin additional blocks independently.
/// Failed probes preserve the reusable heads.
pub struct KeyProbes<K: BatchData, V: BatchData> {
    snapshot: TraceSnapshot<K, V>,
    heads: Vec<Option<Cursor<K, V>>>,
    current: Option<LastProbe<K, V>>,
}
struct Request<K: BatchData, V: BatchData> {
    run: Run<K, V>,
    head: Option<Cursor<K, V>>,
    key: K,
}
struct Requests<K: BatchData, V: BatchData> {
    unique: Vec<Request<K, V>>,
    slots: Vec<usize>,
}
struct LastProbe<K: BatchData, V: BatchData> {
    key: K,
    cursor: KeyCursor<K, V>,
}
impl<K: BatchData, V: BatchData> KeyProbes<K, V> {
    pub(super) fn new(snapshot: TraceSnapshot<K, V>) -> Self {
        let heads = vec![None; snapshot.runs.len()];
        Self { snapshot, heads, current: None }
    }
    fn requests(&self, key: &K) -> Requests<K, V> {
        let mut unique: Vec<Request<K, V>> = Vec::new();
        let mut paths = BTreeMap::<&str, Vec<usize>>::new();
        let mut slots = Vec::new();
        for (run, head) in self.snapshot.runs.iter().zip(&self.heads) {
            let matching = if let Run::Object(batch) = run {
                paths.get(batch.reference().path()).into_iter().flatten().copied().find(|index| {
                    matches!(&unique[*index].run, Run::Object(prior) if batch.same_reader(prior))
                })
            } else {
                None
            };
            let index = matching.unwrap_or_else(|| {
                let index = unique.len();
                if let Run::Object(batch) = run {
                    paths.entry(batch.reference().path()).or_default().push(index);
                }
                unique.push(Request { run: run.clone(), head: head.clone(), key: key.clone() });
                index
            });
            slots.push(index);
        }
        Requests { unique, slots }
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
        let Requests { unique, slots } = self.requests(key);
        let selected = stream::iter(unique)
            .map(|Request { run, head, key }| async move {
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
        let mut cursors = Vec::new();
        for (head, index) in heads.iter_mut().zip(slots) {
            if let Some(cursor) = selected.get(index).context("missing selected run head")? {
                *head = Some(cursor.clone());
                if cursor.current().is_some_and(|(found, _, _)| found == key) {
                    cursors.push(cursor.clone());
                }
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
