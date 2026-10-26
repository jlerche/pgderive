use super::Run;
use crate::engine::reader::{BatchCursor, BatchData, BatchReader, Cursor, Entry, KeyIdentity};
use anyhow::Result;
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
/// Each read operation stages cursor positions, preserving retry on GET errors.
#[derive(Clone)]
pub struct MergedCursor<K: BatchData, V: BatchData> {
    pub(super) cursors: Vec<Cursor<K, V>>,
    pub(super) row: Option<Entry<K, V>>,
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
    pub(super) async fn find(&mut self) -> Result<()> {
        loop {
            let tuple = self
                .cursors
                .iter()
                .filter_map(|cursor| cursor.current().map(|(key, val, _)| (key, val)))
                .min();
            let Some((key, val)) = tuple else {
                self.row = None;
                return Ok(());
            };
            let mut weight = BigInt::default();
            for cursor in &self.cursors {
                if let Some((other_key, other_val, coefficient)) = cursor.current()
                    && (other_key, other_val) == (key, val)
                {
                    weight += coefficient;
                }
            }
            let tuple = (key.clone(), val.clone());
            if weight != BigInt::default() {
                let weight = i64::try_from(weight).map_err(|_| CoefficientOverflow)?;
                self.row = Some((tuple, weight));
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
    fn uncontended_span(&self) -> UncontendedSpan<'_, K, V> {
        let (tuple, _) = self.row.as_ref()?;
        let ordinal = self.cursors.iter().position(|cursor| {
            cursor.current().is_some_and(|(key, value, _)| (key, value) == (&tuple.0, &tuple.1))
        })?;
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

    /// Advance atomically across all runs.
    ///
    /// # Errors
    /// Returns read/corruption/final coefficient errors, retaining prior position.
    pub async fn advance(&mut self) -> Result<()> {
        *self = self.advanced().await?;
        Ok(())
    }
    async fn advanced(&self) -> Result<Self> {
        let mut next = self.clone();
        if let Some((tuple, _)) = &self.row {
            next.skip(tuple).await?;
        }
        next.find().await?;
        Ok(next)
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
    fn current_span(&self) -> crate::engine::reader::EntrySpan<'_, K, V> {
        self.uncontended_span().map(|(_, span)| span)
    }
    async fn advance_span(&mut self) -> Result<()> {
        let Some((ordinal, span)) = self.uncontended_span() else {
            return self.advance().await;
        };
        let count = span.len();
        let mut next = self.clone();
        next.cursors[ordinal].advance_rows(count).await?;
        next.find().await?;
        *self = next;
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
        let next = self.cursor.advanced().await?;
        if let Some(collector) = &mut self.collector {
            if let Some(row) = &self.cursor.row {
                let charge = crate::engine::execution::record_size(
                    row,
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
                    collector.rows.push(row.clone());
                    collector.charge = charge;
                } else {
                    self.collector = None;
                }
            }
            if next.current().is_none()
                && let Some(collector) = self.collector.take()
            {
                collector.ticket.remember(&collector.key, Arc::new(collector.rows));
            }
        }
        self.cursor = next;
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
        let mut cursor = MergedCursor { cursors, row: None };
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
