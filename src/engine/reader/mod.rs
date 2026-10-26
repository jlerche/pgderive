//! Fallible readers over immutable canonical memory and object batches.
mod bounded;
pub(crate) const READ_CONCURRENCY: usize = 32;
mod cache;
mod compression;
mod cursor;
mod format;
mod hybrid;
pub use hybrid::{CacheConfig, EncodedCache};
mod index;
mod object;
mod reachability;
mod streaming;
mod upload;
pub(crate) mod work;
pub use streaming::WriteLimits;

use super::Batch;
use anyhow::Result;
pub(crate) use cache::points::{CacheScope, KeyIdentity, PointRows, Points};
pub use cache::{BlockCache, CacheStats, ReadStats};
pub(crate) use cache::{ReadAhead, ReadGuard};
pub use cursor::Cursor;
pub use object::{ObjectBatch, ObjectRef};
pub(crate) use object::{SelectedBlock, valid_namespace};
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

/// Data accepted by the initial JSON run codec. Rust Ord defines physical order.
///
/// Serde must round-trip identity/order; schema IDs must identify the exact types
/// and their semantics. Hash must agree with Eq; hashes are only used in resident
/// cache synopses and are never persisted. The initial codec is not SQL's general tuple codec.
pub trait BatchData:
    Ord + std::hash::Hash + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}
impl<T> BatchData for T where
    T: Ord + std::hash::Hash + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// Canonical weighted entry, retaining both navigation key and full value.
pub type Entry<K, V> = ((K, V), i64);

/// Borrowed current key/value/coefficient; None indicates exhaustion.
pub type EntryRef<'a, K, V> = Option<(&'a K, &'a V, i64)>;

/// Optional borrowed contiguous block rows.
pub type EntrySpan<'a, K, V> = Option<&'a [Entry<K, V>]>;

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
        Ok(Cursor::memory(self.shared_entries()))
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
    /// Remaining borrowed rows in the current verified block, when supported.
    /// A key-restricted cursor exposes only matching rows.
    fn current_span(&self) -> EntrySpan<'_, Self::Key, Self::Val> {
        None
    }
    /// Consume the current borrowed span. Override together with `current_span`.
    /// The standard cursor retains its position on a failed block load.
    fn advance_span(&mut self) -> impl Future<Output = Result<()>> + Send {
        async {
            anyhow::ensure!(
                self.current_span().is_none(),
                "span cursor must implement advance_span"
            );
            self.advance().await
        }
    }
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
    fn current_span(&self) -> Option<&[Entry<K, V>]> {
        Some(Self::current_span(self))
    }
    async fn advance_span(&mut self) -> Result<()> {
        Self::advance_span(self).await
    }
    async fn advance(&mut self) -> Result<()> {
        Self::advance(self).await
    }
    async fn seek_key(&mut self, key: &K) -> Result<()> {
        Self::seek_key(self, key).await
    }
}

#[cfg(test)]
#[path = "tests/performance.rs"]
mod performance;

#[cfg(test)]
#[path = "tests/memory.rs"]
mod memory_tests;

#[cfg(test)]
#[path = "tests/packing.rs"]
mod packing_tests;

#[cfg(test)]
#[path = "tests/index_pages.rs"]
mod index_page_tests;

#[cfg(test)]
#[path = "tests/points.rs"]
mod point_tests;

#[cfg(test)]
#[path = "tests/cooperative.rs"]
mod cooperative_tests;
