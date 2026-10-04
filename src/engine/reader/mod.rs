//! Fallible readers over immutable canonical memory and object batches.
mod bounded;
mod cache;
mod cursor;
mod format;
mod object;

use super::Batch;
use anyhow::Result;
pub use cache::{BlockCache, CacheStats};
pub use cursor::Cursor;
pub use object::{ObjectBatch, ObjectRef};
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

/// Data accepted by the initial JSON run codec. Rust Ord defines physical order.
///
/// Serde must round-trip identity/order; schema IDs must identify the exact types
/// and their semantics. The initial codec is not SQL's general tuple codec.
pub trait BatchData: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {}
impl<T> BatchData for T where T: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {}

/// Canonical weighted entry, retaining both navigation key and full value.
pub type Entry<K, V> = ((K, V), i64);

/// Borrowed current key/value/coefficient; None indicates exhaustion.
pub type EntryRef<'a, K, V> = Option<(&'a K, &'a V, i64)>;

/// Open a fallible cursor without depending on the batch's physical storage.
pub trait BatchReader {
    /// Navigation key.
    type Key: BatchData;
    /// Full value under the key.
    type Val: BatchData;
    /// Cursor implementation.
    type Cursor: BatchCursor<Key = Self::Key, Val = Self::Val>;
    /// Open at the first entry. Object reads verify checksums before exposure.
    fn cursor(&self) -> impl Future<Output = Result<Self::Cursor>> + Send;
}

impl<K: BatchData, V: BatchData> BatchReader for Batch<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = Cursor<K, V>;
    async fn cursor(&self) -> Result<Cursor<K, V>> {
        Cursor::memory(self.iter().map(|(tuple, weight)| (tuple.clone(), *weight)).collect())
    }
}

/// Forward navigation over full weighted identities with explicit fallible I/O.
pub trait BatchCursor: Send {
    /// Navigation key.
    type Key: BatchData;
    /// Full value.
    type Val: BatchData;
    /// Current tuple; None indicates exhaustion.
    fn current(&self) -> EntryRef<'_, Self::Key, Self::Val>;
    /// Advance one full tuple.
    fn advance(&mut self) -> impl Future<Output = Result<()>> + Send;
    /// Seek to the first key greater than or equal to the target.
    fn seek_key(&mut self, key: &Self::Key) -> impl Future<Output = Result<()>> + Send;
}

impl<K: BatchData, V: BatchData> BatchCursor for Cursor<K, V> {
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
