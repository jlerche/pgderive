//! Bounded run-head reuse for affected-key probes at one immutable boundary.
use super::{KeyCursor, MergedCursor, Run, TraceSnapshot};
use crate::engine::reader::{BatchData, Cursor, SelectedBlock};
use anyhow::{Context as _, Result};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use std::collections::BTreeMap;

// Limit simultaneous selected-run GETs, including oversized single-row pages.
pub(super) use crate::engine::reader::READ_CONCURRENCY;

/// Key probes retaining one reusable verified head per distinct reader.
///
/// Repeated reader occurrences keep their full weighted multiplicity.
/// Current and returned cursors can pin additional blocks independently.
/// Failed probes preserve the reusable heads.
pub struct KeyProbes<K: BatchData, V: BatchData> {
    runs: Vec<Run<K, V>>,
    slots: Vec<usize>,
    heads: Vec<Option<Cursor<K, V>>>,
    current: Option<LastProbe<K, V>>,
}
#[expect(
    clippy::redundant_pub_crate,
    reason = "resolved probes are internal capabilities, not public engine APIs"
)]
pub(crate) struct PreparedKey<K: BatchData, V: BatchData> {
    key: K,
    pages: Vec<Option<SelectedBlock<K, V>>>,
}
impl<K: BatchData, V: BatchData> PreparedKey<K, V> {
    pub(crate) fn charge(&self) -> Result<usize> {
        let key = serde_json::to_vec(&self.key)?
            .len()
            .checked_mul(2)
            .context("prepared key heap charge overflow")?;
        let initial = self
            .pages
            .capacity()
            .checked_mul(std::mem::size_of::<Option<SelectedBlock<K, V>>>())
            .and_then(|slots| slots.checked_add(key))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .context("prepared key slot charge overflow")?;
        self.pages.iter().flatten().try_fold(initial, |total, page| {
            total.checked_add(page.charge()?).context("prepared key charge overflow")
        })
    }
}
struct LastProbe<K: BatchData, V: BatchData> {
    key: K,
    cursor: KeyCursor<K, V>,
}
impl<K: BatchData, V: BatchData> KeyProbes<K, V> {
    pub(super) fn new(snapshot: TraceSnapshot<K, V>) -> Self {
        let mut runs: Vec<Run<K, V>> = Vec::new();
        let mut paths = BTreeMap::<String, Vec<usize>>::new();
        let mut slots = Vec::new();
        for run in snapshot.runs {
            let matching = if let Run::Object(batch) = &run {
                paths.get(batch.reference().path()).into_iter().flatten().copied().find(
                    |index| matches!(&runs[*index], Run::Object(prior) if batch.same_reader(prior)),
                )
            } else {
                None
            };
            let index = matching.unwrap_or_else(|| {
                let index = runs.len();
                if let Run::Object(batch) = &run {
                    paths.entry(batch.reference().path().to_owned()).or_default().push(index);
                }
                runs.push(run);
                index
            });
            slots.push(index);
        }
        let heads = vec![None; runs.len()];
        Self { runs, slots, heads, current: None }
    }
    pub(crate) async fn prepare_key(&self, key: &K) -> Result<PreparedKey<K, V>> {
        let mut pages = Vec::with_capacity(self.runs.len());
        for run in &self.runs {
            let page = if let Run::Object(batch) = run {
                let Some(page) = batch.key_page(key).await? else {
                    pages.push(None);
                    continue;
                };
                batch.may_contain_page(&page, key)?.then_some(page)
            } else {
                None
            };
            pages.push(page);
        }
        Ok(PreparedKey { key: key.clone(), pages })
    }
    // Reserve resolved pages in original key order; no second navigation lookup.
    pub(crate) fn plan_key(
        &self,
        key: &PreparedKey<K, V>,
        plans: &mut Vec<crate::engine::reader::ReadAhead>,
    ) -> Result<bool> {
        let mut admitted = true;
        for (index, (run, page)) in self.runs.iter().zip(&key.pages).enumerate() {
            let (Run::Object(batch), Some(page)) = (run, page) else { continue };
            if !self.heads[index].as_ref().is_some_and(|head| head.holds_block(batch, page.ordinal))
            {
                admitted &= batch.reserve_page(page, plans)?;
            }
        }
        Ok(admitted)
    }
    #[cfg(test)]
    pub(crate) async fn prefetch(&self, keys: &[&K]) -> Result<crate::engine::reader::ReadGuard> {
        let mut plans = Vec::new();
        for key in keys {
            self.plan_key(&self.prepare_key(key).await?, &mut plans)?;
        }
        crate::engine::reader::ReadAhead::load(plans).await
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
        let prepared = self.prepare_key(key).await?;
        self.cursor_prepared(&prepared).await
    }
    pub(crate) async fn cursor_prepared(
        &mut self,
        prepared: &PreparedKey<K, V>,
    ) -> Result<KeyCursor<K, V>> {
        let key = &prepared.key;
        let requests = self
            .runs
            .iter()
            .zip(&self.heads)
            .zip(&prepared.pages)
            .map(|((run, head), page)| (run.clone(), head.clone(), key.clone(), page.clone()))
            .collect::<Vec<_>>();
        let selected = stream::iter(requests)
            .map(|(run, head, key, page)| async move {
                match run {
                    Run::Memory(batch) => Ok(Some(Cursor::memory_key_reusing(
                        batch.shared_entries(),
                        &key,
                        head.as_ref(),
                    ))),
                    Run::Object(batch) => match page {
                        Some(page) => {
                            Cursor::object_key_page(batch, &key, page, head.as_ref()).await
                        }
                        None => Ok(None),
                    },
                }
            })
            .buffered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut heads = self.heads.clone();
        for (head, selected) in heads.iter_mut().zip(&selected) {
            if let Some(cursor) = selected {
                *head = Some(cursor.clone());
            }
        }
        let mut cursors = Vec::new();
        for index in &self.slots {
            if let Some(cursor) = selected.get(*index).context("missing selected run head")?
                && cursor.current().is_some_and(|(found, _, _)| found == key)
            {
                cursors.push(cursor.clone());
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
