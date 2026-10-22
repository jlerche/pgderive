use super::{BatchData, Entry, ObjectBatch};
use anyhow::Result;
use std::sync::Arc;

#[derive(Clone)]
enum Source<K: BatchData, V: BatchData> {
    Memory(Arc<Vec<Entry<K, V>>>),
    Object(ObjectBatch<K, V>),
}

/// Forward cursor over sorted full identities. A cursor pins immutable input.
/// Object cursors keep one decoded block; failed loads retain the old position.
#[derive(Clone)]
pub struct Cursor<K: BatchData, V: BatchData> {
    source: Source<K, V>,
    rows: Arc<Vec<Entry<K, V>>>,
    block: usize,
    position: usize,
    end: Option<K>,
}

impl<K: BatchData, V: BatchData> Cursor<K, V> {
    // Finalized Batch storage is canonical by construction. Opening a cursor
    // shares those verified rows; it never copies or rescans the run.
    pub(super) fn memory(rows: Arc<Vec<Entry<K, V>>>) -> Self {
        Self { source: Source::Memory(rows.clone()), rows, block: 0, position: 0, end: None }
    }

    pub(super) async fn object(batch: ObjectBatch<K, V>) -> Result<Self> {
        let rows =
            if batch.blocks.is_empty() { Arc::new(Vec::new()) } else { batch.read(0).await? };
        Ok(Self { source: Source::Object(batch), rows, block: 0, position: 0, end: None })
    }

    pub(crate) async fn object_key(batch: ObjectBatch<K, V>, key: &K) -> Result<Option<Self>> {
        Self::object_key_reusing(batch, key, None).await
    }
    pub(crate) async fn object_key_reusing(
        batch: ObjectBatch<K, V>,
        key: &K,
        previous: Option<&Self>,
    ) -> Result<Option<Self>> {
        let Some(block) = batch.key_block(key) else {
            return Ok(None);
        };
        let rows = match previous {
            Some(previous)
                if previous.block == block
                    && matches!(&previous.source, Source::Object(old) if old.reference() == batch.reference()) =>
            {
                previous.rows.clone()
            }
            _ => batch.read(block).await?,
        };
        let position = rows.partition_point(|row| row.0.0 < *key);
        Ok(Some(Self {
            source: Source::Object(batch),
            rows,
            block,
            position,
            end: Some(key.clone()),
        }))
    }
    pub(crate) async fn object_range(
        batch: ObjectBatch<K, V>,
        lower: &K,
        upper: &K,
    ) -> Result<Option<Self>> {
        let block = batch.blocks.partition_point(|block| block.last.0 < *lower);
        if batch.blocks.get(block).is_none_or(|block| block.first.0 > *upper) {
            return Ok(None);
        }
        let rows = batch.read(block).await?;
        let position = rows.partition_point(|row| row.0.0 < *lower);
        Ok(Some(Self {
            source: Source::Object(batch),
            rows,
            block,
            position,
            end: Some(upper.clone()),
        }))
    }
    pub(crate) fn restrict_to_upper(&mut self, key: &K) {
        self.end = Some(key.clone());
    }
    /// Current key, full value, and signed coefficient; None means exhausted.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.rows
            .get(self.position)
            .filter(|row| self.end.as_ref().is_none_or(|key| &row.0.0 <= key))
            .map(|((key, value), weight)| (key, value, *weight))
    }

    /// Remaining verified rows in this block, honoring the inclusive upper bound.
    #[must_use]
    pub fn current_span(&self) -> &[Entry<K, V>] {
        let remaining = &self.rows[self.position..];
        let length = self
            .end
            .as_ref()
            .map_or(remaining.len(), |key| remaining.partition_point(|row| &row.0.0 <= key));
        &remaining[..length]
    }

    pub(crate) async fn advance_rows(&mut self, count: usize) -> Result<()> {
        anyhow::ensure!(count <= self.current_span().len(), "advance exceeds verified span");
        if count < self.rows.len() - self.position {
            self.position += count;
            return Ok(());
        }
        self.advance_span().await
    }

    /// Consume the current span, loading at most one following block.
    ///
    /// # Errors
    /// Returns corruption or GET errors without changing the prior position.
    pub async fn advance_span(&mut self) -> Result<()> {
        if let Source::Object(batch) = &self.source
            && self.block + 1 < batch.blocks.len()
            && self.end.as_ref().is_none_or(|key| batch.blocks[self.block + 1].first.0 <= *key)
        {
            let rows = batch.read(self.block + 1).await?;
            self.rows = rows;
            self.block += 1;
            self.position = 0;
        } else {
            self.position = self.rows.len();
        }
        Ok(())
    }

    /// Advance one full tuple, loading at most one next object block.
    ///
    /// # Errors
    /// Returns corruption or GET errors without changing the prior position.
    pub async fn advance(&mut self) -> Result<()> {
        if self.position + 1 < self.rows.len() {
            self.position += 1;
            return Ok(());
        }
        if let Source::Object(batch) = &self.source
            && self.block + 1 < batch.blocks.len()
        {
            if self.end.as_ref().is_some_and(|key| batch.blocks[self.block + 1].first.0 > *key) {
                self.position = self.rows.len();
                return Ok(());
            }
            let rows = batch.read(self.block + 1).await?;
            self.rows = rows;
            self.block += 1;
            self.position = 0;
            return Ok(());
        }
        self.position = self.rows.len();
        Ok(())
    }

    /// Seek to the first full tuple whose navigation key is >= key.
    /// Keys spanning blocks retain all their distinct values.
    ///
    /// # Errors
    /// Returns corruption or GET errors without changing the prior position.
    pub async fn seek_key(&mut self, key: &K) -> Result<()> {
        let (block, rows) = match &self.source {
            Source::Memory(rows) => (0, rows.clone()),
            Source::Object(batch) => {
                let block = batch.blocks.partition_point(|block| block.last.0 < *key);
                let rows = if block < batch.blocks.len() {
                    batch.read(block).await?
                } else {
                    Arc::new(Vec::new())
                };
                (block, rows)
            }
        };
        let position = rows.partition_point(|row| row.0.0 < *key);
        self.rows = rows;
        self.block = block;
        self.position = position;
        Ok(())
    }
}
