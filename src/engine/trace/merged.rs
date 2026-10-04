use super::Run;
use crate::engine::{
    reader::{BatchCursor, BatchData, BatchReader, Cursor, Entry},
    weights::Accumulator,
};
use anyhow::Result;

/// Merge pinned run cursors, summing complete identities before i64 narrowing.
/// Each read operation stages cursor positions, preserving retry on GET errors.
#[derive(Clone)]
pub struct MergedCursor<K: BatchData, V: BatchData> {
    cursors: Vec<Cursor<K, V>>,
    row: Option<Entry<K, V>>,
}

impl<K: BatchData, V: BatchData> MergedCursor<K, V> {
    pub(super) async fn open(runs: &[Run<K, V>]) -> Result<Self> {
        let mut cursors = Vec::new();
        for run in runs {
            cursors.push(run.cursor().await?);
        }
        let mut result = Self { cursors, row: None };
        result.find().await?;
        Ok(result)
    }
    /// Current consolidated full identity, or None when exhausted.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.row.as_ref().map(|((key, val), weight)| (key, val, *weight))
    }
    async fn find(&mut self) -> Result<()> {
        loop {
            let tuple = self
                .cursors
                .iter()
                .filter_map(|cursor| {
                    cursor.current().map(|(key, val, _)| (key.clone(), val.clone()))
                })
                .min();
            let Some(tuple) = tuple else {
                self.row = None;
                return Ok(());
            };
            let mut weights = Accumulator::default();
            for cursor in &self.cursors {
                if let Some((key, val, weight)) = cursor.current()
                    && (key, val) == (&tuple.0, &tuple.1)
                {
                    weights.add(tuple.clone(), weight);
                }
            }
            let rows = weights.finish()?;
            if let Some(weight) = rows.get(&tuple) {
                self.row = Some((tuple, *weight));
                return Ok(());
            }
            self.skip(&tuple).await?;
        }
    }
    async fn skip(&mut self, tuple: &(K, V)) -> Result<()> {
        for cursor in &mut self.cursors {
            if let Some((key, val, _)) = cursor.current()
                && (key, val) == (&tuple.0, &tuple.1)
            {
                cursor.advance().await?;
            }
        }
        Ok(())
    }
    /// Advance atomically across all runs.
    ///
    /// # Errors
    /// Returns read/corruption/final coefficient errors, retaining prior position.
    pub async fn advance(&mut self) -> Result<()> {
        let mut next = self.clone();
        if let Some((tuple, _)) = &self.row {
            next.skip(tuple).await?;
        }
        next.find().await?;
        *self = next;
        Ok(())
    }
    /// Seek all runs, consolidating all values under the target navigation key.
    ///
    /// # Errors
    /// Returns read/corruption/final coefficient errors, retaining prior position.
    pub async fn seek_key(&mut self, key: &K) -> Result<()> {
        let mut next = self.clone();
        for cursor in &mut next.cursors {
            cursor.seek_key(key).await?;
        }
        next.find().await?;
        *self = next;
        Ok(())
    }
}
impl<K: BatchData, V: BatchData> BatchCursor for MergedCursor<K, V> {
    type Key = K;
    type Val = V;
    fn current(&self) -> Option<(&K, &V, i64)> {
        Self::current(self)
    }
    async fn advance(&mut self) -> Result<()> {
        Self::advance(self).await
    }
    async fn seek_key(&mut self, key: &K) -> Result<()> {
        Self::seek_key(self, key).await
    }
}

/// Forward-only exact-key probe; all distinct values under the key remain visible.
/// It cannot be reseeked to another key after excluding irrelevant runs.
#[derive(Clone)]
pub struct KeyCursor<K: BatchData, V: BatchData> {
    cursor: MergedCursor<K, V>,
}
impl<K: BatchData, V: BatchData> KeyCursor<K, V> {
    pub(super) async fn open(runs: &[Run<K, V>], key: &K) -> Result<Self> {
        let mut cursors = Vec::new();
        for run in runs {
            if let Some(cursor) = run.cursor_for_key(key).await? {
                cursors.push(cursor);
            }
        }
        let mut cursor = MergedCursor { cursors, row: None };
        cursor.find().await?;
        Ok(Self { cursor })
    }
    /// Current full weighted identity, restricted to the probed key.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.cursor.current()
    }
    /// Advance atomically without reading blocks beyond the selected key.
    ///
    /// # Errors
    /// Returns selected-block read, integrity, or arithmetic failures and retains
    /// the prior position so the caller may retry the same operation.
    pub async fn advance(&mut self) -> Result<()> {
        self.cursor.advance().await
    }
}

/// Batch of key probes sharing a pinned snapshot and the current key's run heads.
/// Repeated identities under one key reuse decoded heads without another GET.
pub struct KeyProbes<K: BatchData, V: BatchData> {
    snapshot: super::TraceSnapshot<K, V>,
    current: Option<LastProbe<K, V>>,
}
struct LastProbe<K: BatchData, V: BatchData> {
    key: K,
    cursor: KeyCursor<K, V>,
}
impl<K: BatchData, V: BatchData> KeyProbes<K, V> {
    pub(super) const fn new(snapshot: super::TraceSnapshot<K, V>) -> Self {
        Self { snapshot, current: None }
    }
    /// Pin a fresh cursor for one key, reusing the most recent key's run heads.
    ///
    /// # Errors
    /// Returns selected-block read, integrity, or arithmetic errors.
    pub async fn cursor(&mut self, key: &K) -> Result<KeyCursor<K, V>> {
        if let Some(current) = &self.current
            && &current.key == key
        {
            return Ok(current.cursor.clone());
        }
        let cursor = self.snapshot.key_cursor(key).await?;
        self.current = Some(LastProbe { key: key.clone(), cursor: cursor.clone() });
        Ok(cursor)
    }
}
