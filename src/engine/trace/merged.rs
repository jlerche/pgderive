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
