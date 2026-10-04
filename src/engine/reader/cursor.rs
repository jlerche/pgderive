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
    rows: Vec<Entry<K, V>>,
    block: usize,
    position: usize,
}

impl<K: BatchData, V: BatchData> Cursor<K, V> {
    pub(super) fn memory(rows: Vec<Entry<K, V>>) -> Result<Self> {
        super::format::validate(&rows)?;
        Ok(Self { source: Source::Memory(Arc::new(rows.clone())), rows, block: 0, position: 0 })
    }

    pub(super) async fn object(batch: ObjectBatch<K, V>) -> Result<Self> {
        let rows = if batch.blocks.is_empty() { Vec::new() } else { batch.read(0).await? };
        Ok(Self { source: Source::Object(batch), rows, block: 0, position: 0 })
    }

    /// Current key, full value, and signed coefficient; None means exhausted.
    #[must_use]
    pub fn current(&self) -> Option<(&K, &V, i64)> {
        self.rows.get(self.position).map(|((key, value), weight)| (key, value, *weight))
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
            Source::Memory(rows) => (0, rows.as_ref().clone()),
            Source::Object(batch) => {
                let block = batch.blocks.partition_point(|block| block.last.0 < *key);
                let rows =
                    if block < batch.blocks.len() { batch.read(block).await? } else { Vec::new() };
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
