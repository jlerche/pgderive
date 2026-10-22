//! Bounded run-head reuse for affected-key probes at one immutable boundary.
use super::{KeyCursor, MergedCursor, Run, TraceSnapshot};
use crate::engine::reader::{BatchData, BatchReader, Cursor};
use anyhow::Result;

/// Key probes retaining one reusable verified head per run across groups.
/// Current and returned cursors can pin additional blocks independently.
/// Failed probes preserve the reusable heads.
pub struct KeyProbes<K: BatchData, V: BatchData> {
    snapshot: TraceSnapshot<K, V>,
    heads: Vec<Option<Cursor<K, V>>>,
    current: Option<LastProbe<K, V>>,
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
        let mut heads = self.heads.clone();
        let mut cursors = Vec::new();
        for (run, head) in self.snapshot.runs.iter().zip(&mut heads) {
            let selected = match run {
                Run::Memory(batch) => {
                    let mut cursor = batch.cursor().await?;
                    cursor.seek_key(key).await?;
                    cursor.restrict_to_upper(key);
                    Some(cursor)
                }
                Run::Object(batch) => {
                    Cursor::object_key_reusing(batch.clone(), key, head.as_ref()).await?
                }
            };
            if let Some(cursor) = selected {
                *head = Some(cursor.clone());
                if cursor.current().is_some_and(|(found, _, _)| found == key) {
                    cursors.push(cursor);
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
