use super::Run;
use crate::engine::reader::{BatchCursor, BatchData, BatchReader, Cursor, Entry, KeyIdentity};
use anyhow::{Context as _, Result};
use num_bigint::BigInt;
use std::sync::Arc;

#[derive(Debug)]
pub struct CoefficientOverflow;
impl std::fmt::Display for CoefficientOverflow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("finalized weight exceeds i64 domain")
    }
}
impl std::error::Error for CoefficientOverflow {}

type UncontendedSpan<'a, K, V> = Option<(usize, &'a [Entry<K, V>])>;

/// Merge pinned run cursors, summing complete identities before i64 narrowing.
///
/// Each read operation stages cursor positions, preserving retry on GET errors.
/// Between operations, private staging retains capacity without decoded page pins.
pub struct MergedCursor<K: BatchData, V: BatchData> {
    position: Position<K, V>,
    workspace: Position<K, V>,
}
#[derive(Clone)]
struct Position<K: BatchData, V: BatchData> {
    cursors: Vec<Cursor<K, V>>,
    // The selected run pins the identity; only its consolidated weight is owned.
    row: Option<(usize, i64)>,
}
impl<K: BatchData, V: BatchData> Clone for MergedCursor<K, V> {
    fn clone(&self) -> Self {
        Self { position: self.position.clone(), workspace: Position::new(Vec::new()) }
    }
}
impl<K: BatchData, V: BatchData> Position<K, V> {
    const fn new(cursors: Vec<Cursor<K, V>>) -> Self {
        Self { cursors, row: None }
    }
    fn current(&self) -> Option<(&K, &V, i64)> {
        self.row.and_then(|(ordinal, weight)| {
            self.cursors[ordinal].current().map(|(key, val, _)| (key, val, weight))
        })
    }
    async fn find(&mut self) -> Result<()> {
        loop {
            crate::engine::reader::work::spend(0).await;
            let tuple = self
                .cursors
                .iter()
                .enumerate()
                .filter_map(|(ordinal, cursor)| {
                    cursor.current().map(|(key, val, _)| (ordinal, key, val))
                })
                .min_by(|(_, key, val), (_, other_key, other_val)| {
                    (key, val).cmp(&(other_key, other_val))
                });
            let Some((ordinal, key, val)) = tuple else {
                self.row = None;
                return Ok(());
            };
            let mut weight = BigInt::default();
            let mut last = ordinal;
            for (index, cursor) in self.cursors.iter().enumerate() {
                if let Some((other_key, other_val, coefficient)) = cursor.current()
                    && (other_key, other_val) == (key, val)
                {
                    weight += coefficient;
                    last = index;
                }
            }
            if weight != BigInt::default() {
                let weight = i64::try_from(weight).map_err(|_| CoefficientOverflow)?;
                self.row = Some((ordinal, weight));
                return Ok(());
            }
            self.skip_cancelled(last).await?;
        }
    }
    async fn skip_cancelled(&mut self, last: usize) -> Result<()> {
        // The last matching head pins the identity until all earlier matches
        // advance. Advancing the witness last preserves the original GET order.
        let (earlier, remaining) = self.cursors.split_at_mut(last);
        let witness = &mut remaining[0];
        let (key, value, _) = witness.current().context("missing canceled identity witness")?;
        Self::skip(earlier, (key, value)).await?;
        witness.advance().await
    }
    async fn skip(cursors: &mut [Cursor<K, V>], tuple: (&K, &V)) -> Result<()> {
        for cursor in cursors {
            if let Some((key, val, _)) = cursor.current()
                && (key, val) == tuple
            {
                cursor.advance().await?;
            }
        }
        Ok(())
    }
    fn uncontended_span(&self) -> UncontendedSpan<'_, K, V> {
        let (ordinal, _) = self.row?;
        let span = self.cursors[ordinal].current_span();
        let other = self
            .cursors
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != ordinal)
            .filter_map(|(_, cursor)| cursor.current().map(|(key, value, _)| (key, value)))
            .min();
        let length = other
            .map_or(span.len(), |bound| span.partition_point(|row| (&row.0.0, &row.0.1) < bound));
        (length > 0).then_some((ordinal, &span[..length]))
    }
}

// Dropping an in-flight operation releases every staged page pin even when its
// future is cancelled. The allocation stays in its owning cursor for the retry.
struct Stage<'a, K: BatchData, V: BatchData> {
    position: &'a mut Position<K, V>,
    workspace: &'a mut Position<K, V>,
}
impl<K: BatchData, V: BatchData> Stage<'_, K, V> {
    fn commit(self) {
        std::mem::swap(self.position, self.workspace);
    }
}
impl<K: BatchData, V: BatchData> Drop for Stage<'_, K, V> {
    fn drop(&mut self) {
        self.workspace.cursors.clear();
        self.workspace.row = None;
    }
}
impl<K: BatchData, V: BatchData> MergedCursor<K, V> {
    pub(super) const fn new(cursors: Vec<Cursor<K, V>>) -> Self {
        Self { position: Position::new(cursors), workspace: Position::new(Vec::new()) }
    }
    pub(super) async fn open(runs: &[Run<K, V>]) -> Result<Self> {
        let mut cursors = Vec::new();
        for run in runs {
            cursors.push(run.cursor().await?);
        }
        let mut result = Self::new(cursors);
        result.find().await?;
        Ok(result)
    }
    /// Current consolidated full identity, or None when exhausted.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.position.current()
    }
    pub(super) async fn find(&mut self) -> Result<()> {
        self.position.find().await
    }
    fn stage(&mut self) -> Stage<'_, K, V> {
        let stage = Stage { position: &mut self.position, workspace: &mut self.workspace };
        stage.workspace.cursors.clone_from(&stage.position.cursors);
        stage
    }
    async fn staged_advance(&mut self) -> Result<Stage<'_, K, V>> {
        let stage = self.stage();
        if let Some((key, value, _)) = stage.position.current() {
            Position::skip(&mut stage.workspace.cursors, (key, value)).await?;
        }
        stage.workspace.find().await?;
        Ok(stage)
    }
    /// Advance atomically across all runs.
    ///
    /// # Errors
    /// Returns read/corruption/final coefficient errors, retaining prior position.
    pub async fn advance(&mut self) -> Result<()> {
        self.staged_advance().await?.commit();
        Ok(())
    }
    /// Seek all runs, consolidating all values under the target navigation key.
    ///
    /// # Errors
    /// Returns read/corruption/final coefficient errors, retaining prior position.
    pub async fn seek_key(&mut self, key: &K) -> Result<()> {
        let stage = self.stage();
        for cursor in &mut stage.workspace.cursors {
            cursor.seek_key(key).await?;
        }
        stage.workspace.find().await?;
        stage.commit();
        Ok(())
    }
}
impl<K: BatchData, V: BatchData> BatchCursor for MergedCursor<K, V> {
    type Key = K;
    type Val = V;
    fn current(&self) -> Option<(&K, &V, i64)> {
        Self::current(self)
    }
    fn current_span(&self) -> crate::engine::reader::EntrySpan<'_, K, V> {
        self.position.uncontended_span().map(|(_, span)| span)
    }
    async fn advance_span(&mut self) -> Result<()> {
        let Some((ordinal, span)) = self.position.uncontended_span() else {
            return self.advance().await;
        };
        let count = span.len();
        let stage = self.stage();
        stage.workspace.cursors[ordinal].advance_rows(count).await?;
        stage.workspace.find().await?;
        stage.commit();
        Ok(())
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
///
/// A live probe optionally retains at most 64 KiB of private collector workspace;
/// callers owning multiple cursors also own their separate bounded workspaces.
pub struct KeyCursor<K: BatchData, V: BatchData> {
    pub(super) cursor: MergedCursor<K, V>,
    collector: Option<Collector<K, V>>,
}
struct Collector<K: BatchData, V: BatchData> {
    ticket: super::cache::Ticket,
    key: Arc<KeyIdentity<K>>,
    rows: Vec<Entry<K, V>>,
    charge: usize,
    limit: usize,
}
impl<K: BatchData, V: BatchData> Clone for KeyCursor<K, V> {
    fn clone(&self) -> Self {
        Self { cursor: self.cursor.clone(), collector: None }
    }
}
impl<K: BatchData, V: BatchData> KeyCursor<K, V> {
    pub(super) fn new(
        cursor: MergedCursor<K, V>,
        ticket: Option<super::cache::Ticket>,
        key: Arc<KeyIdentity<K>>,
        limit: usize,
    ) -> Self {
        Self {
            cursor,
            collector: ticket.filter(|_| limit >= 512).map(|ticket| Collector {
                ticket,
                key,
                rows: Vec::new(),
                charge: 512,
                limit,
            }),
        }
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
        let stage = self.cursor.staged_advance().await?;
        if let Some(collector) = &mut self.collector {
            if let Some((key, value, weight)) = stage.position.current() {
                let row = ((key, value), weight);
                let charge = crate::engine::execution::record_size(
                    &row,
                    collector.limit.saturating_sub(collector.charge) / 2,
                )
                .ok()
                .and_then(|bytes| {
                    bytes
                        .checked_mul(2)
                        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<Entry<K, V>>()))
                })
                .and_then(|bytes| collector.charge.checked_add(bytes));
                if let Some(charge) = charge.filter(|charge| *charge <= collector.limit) {
                    collector.rows.push(((key.clone(), value.clone()), weight));
                    collector.charge = charge;
                } else {
                    self.collector = None;
                }
            }
            if stage.workspace.current().is_none()
                && let Some(collector) = self.collector.take()
            {
                collector.ticket.remember(&collector.key, Arc::new(collector.rows));
            }
        }
        stage.commit();
        Ok(())
    }
}

/// Forward-only inclusive navigation range across all overlapping immutable runs.
#[derive(Clone)]
pub struct RangeCursor<K: BatchData, V: BatchData> {
    cursor: MergedCursor<K, V>,
}
impl<K: BatchData, V: BatchData> RangeCursor<K, V> {
    pub(super) async fn open(runs: &[Run<K, V>], lower: &K, upper: &K) -> Result<Self> {
        let mut cursors = Vec::new();
        for run in runs {
            if let Some(cursor) = run.cursor_for_range(lower, upper).await? {
                cursors.push(cursor);
            }
        }
        let mut cursor = MergedCursor::new(cursors);
        cursor.find().await?;
        Ok(Self { cursor })
    }
    /// Current consolidated full identity and signed coefficient.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.cursor.current()
    }
    /// Advance without loading blocks beyond the inclusive upper bound.
    ///
    /// # Errors
    /// Read/integrity failures preserve the old position for retry.
    pub async fn advance(&mut self) -> Result<()> {
        self.cursor.advance().await
    }
}

#[cfg(test)]
#[path = "tests/merge_heads.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/staging.rs"]
mod staging_tests;
