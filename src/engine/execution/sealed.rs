//! Canonical immutable logical deltas, resident or scratch-backed behind one cursor.
use super::{Consolidator, Limits, record};
use crate::engine::{
    Batch, BatchBuilder,
    reader::{BatchCursor, BatchData, BatchReader, Cursor, Entry, EntryRef},
};
use anyhow::{Context, Result, ensure};
use std::{
    fs::File,
    io::{BufReader, Seek as _},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// A complete sealed logical edge. Physical spill boundaries carry no timestamps.
#[derive(Clone)]
pub struct Sealed<K: BatchData, V: BatchData> {
    storage: Storage<K, V>,
    entries: usize,
}
#[derive(Clone)]
enum Storage<K: BatchData, V: BatchData> {
    Resident(Arc<Batch<K, V>>),
    Scratch(Arc<Scratch>),
}
struct Scratch {
    directory: Option<tempfile::TempDir>,
    run: record::Run,
    record_bytes: usize,
    failed: AtomicBool,
}
impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take()
            && !self.failed.load(Ordering::Relaxed)
        {
            let _cleanup = directory.close();
        }
    }
}
impl<K: BatchData, V: BatchData> From<Batch<K, V>> for Sealed<K, V> {
    fn from(batch: Batch<K, V>) -> Self {
        Self { entries: batch.iter().count(), storage: Storage::Resident(Arc::new(batch)) }
    }
}
impl<K: BatchData, V: BatchData> Sealed<K, V> {
    pub(super) fn scratch(
        directory: tempfile::TempDir,
        run: record::Run,
        entries: usize,
        record_bytes: usize,
    ) -> Self {
        Self {
            entries,
            storage: Storage::Scratch(Arc::new(Scratch {
                directory: Some(directory),
                run,
                record_bytes,
                failed: AtomicBool::new(false),
            })),
        }
    }
    /// Number of canonical nonzero full identities.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries
    }
    /// Whether the sealed edge contains no identities.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries == 0
    }
    /// Materialize only within the caller's existing bounded batch limits.
    ///
    /// # Errors
    /// Returns codec, I/O, capacity or arithmetic errors.
    pub async fn materialize(&self, limits: Limits) -> Result<Batch<K, V>> {
        if let Some(batch) = self.resident() {
            limits.check_batch(batch)?;
            return Ok(batch.as_ref().clone());
        }
        let mut cursor = self.cursor().await?;
        let mut output = Consolidator::new(limits)?;
        while let Some((key, value, weight)) = cursor.current() {
            output.add((key.clone(), value.clone()), weight)?;
            cursor.advance().await?;
        }
        output.finish_batch()
    }
    pub(crate) fn retain_failure(&self) {
        if let Storage::Scratch(owner) = &self.storage {
            owner.failed.store(true, Ordering::Relaxed);
        }
    }
    pub(crate) const fn resident(&self) -> Option<&Arc<Batch<K, V>>> {
        match &self.storage {
            Storage::Resident(batch) => Some(batch),
            Storage::Scratch(_) => None,
        }
    }
}
/// Forward cursor pinning the immutable sealed edge and its scratch ownership.
pub struct SealedCursor<K: BatchData, V: BatchData> {
    state: CursorState<K, V>,
}
enum CursorState<K: BatchData, V: BatchData> {
    Resident(Cursor<K, V>),
    Scratch { owner: Arc<Scratch>, reader: BufReader<File>, current: Option<Entry<K, V>> },
}
fn read<K: BatchData, V: BatchData>(
    reader: &mut BufReader<File>,
    limit: usize,
    file_bytes: u64,
) -> Result<Option<Entry<K, V>>> {
    record::read_complete::<(K, V)>(reader, limit, file_bytes)?
        .map(|(tuple, weight)| Ok((tuple, i64::try_from(weight)?)))
        .transpose()
}
impl<K: BatchData, V: BatchData> BatchReader for Sealed<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = SealedCursor<K, V>;
    async fn cursor(&self) -> Result<Self::Cursor> {
        let state = match &self.storage {
            Storage::Resident(batch) => CursorState::Resident(batch.cursor().await?),
            Storage::Scratch(owner) => {
                let opened = (|| -> Result<_> {
                    let mut reader = BufReader::new(File::open(&owner.run.path)?);
                    let current = read(&mut reader, owner.record_bytes, owner.run.bytes)?;
                    Ok((reader, current))
                })();
                let (reader, current) = opened.inspect_err(|_| {
                    owner.failed.store(true, Ordering::Relaxed);
                })?;
                CursorState::Scratch { owner: owner.clone(), reader, current }
            }
        };
        Ok(SealedCursor { state })
    }
}
impl<K: BatchData, V: BatchData> BatchCursor for SealedCursor<K, V> {
    type Key = K;
    type Val = V;
    fn current(&self) -> EntryRef<'_, K, V> {
        match &self.state {
            CursorState::Resident(cursor) => cursor.current(),
            CursorState::Scratch { current, .. } => current.as_ref().map(|((k, v), w)| (k, v, *w)),
        }
    }
    async fn advance(&mut self) -> Result<()> {
        match &mut self.state {
            CursorState::Resident(cursor) => cursor.advance().await,
            CursorState::Scratch { owner, reader, current } => {
                let position = reader.stream_position()?;
                let loaded: Result<Option<Entry<K, V>>> = (|| {
                    let next = read::<K, V>(reader, owner.record_bytes, owner.run.bytes)?;
                    ensure!(
                        current
                            .as_ref()
                            .zip(next.as_ref())
                            .is_none_or(|(before, after)| before.0 < after.0),
                        "noncanonical sealed delta"
                    );
                    Ok(next)
                })();
                let next = match loaded {
                    Ok(next) => next,
                    Err(error) => {
                        owner.failed.store(true, Ordering::Relaxed);
                        reader.seek(std::io::SeekFrom::Start(position))?;
                        return Err(error).with_context(|| {
                            format!("reading sealed delta {}", owner.run.path.display())
                        });
                    }
                };
                *current = next;
                Ok(())
            }
        }
    }
    async fn seek_key(&mut self, key: &K) -> Result<()> {
        if let CursorState::Resident(cursor) = &mut self.state {
            return cursor.seek_key(key).await;
        }
        if let CursorState::Scratch { owner, reader, current } = &mut self.state {
            let loaded = (|| -> Result<_> {
                let mut replacement = BufReader::new(File::open(&owner.run.path)?);
                let mut next = read::<K, V>(&mut replacement, owner.record_bytes, owner.run.bytes)?;
                while next.as_ref().is_some_and(|((found, _), _)| found < key) {
                    let following =
                        read::<K, V>(&mut replacement, owner.record_bytes, owner.run.bytes)?;
                    ensure!(
                        next.as_ref()
                            .zip(following.as_ref())
                            .is_none_or(|(before, after)| before.0 < after.0),
                        "noncanonical sealed delta"
                    );
                    next = following;
                }
                Ok((replacement, next))
            })();
            let (replacement, next) = loaded.inspect_err(|_| {
                owner.failed.store(true, Ordering::Relaxed);
            })?;
            *reader = replacement;
            *current = next;
        }
        Ok(())
    }
}
/// Consume one complete navigation key without splitting its semantic boundary.
pub async fn group<K: BatchData, V: BatchData>(
    cursor: &mut SealedCursor<K, V>,
    limits: Limits,
) -> Result<Batch<K, V>> {
    let Some((key, _, _)) = cursor.current() else {
        return BatchBuilder::default().finish();
    };
    let key = key.clone();
    let mut output = Consolidator::new(limits)?;
    while let Some((found, value, weight)) = cursor.current() {
        if *found != key {
            break;
        }
        output.add((found.clone(), value.clone()), weight)?;
        cursor.advance().await?;
    }
    output.finish_batch()
}

#[cfg(test)]
#[path = "tests/sealed.rs"]
mod tests;
