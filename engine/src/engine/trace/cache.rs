//! One bounded logical-key cache across physical trace representations.
use super::Run;
use crate::engine::reader::{BatchData, BlockCache};
use crate::engine::reader::{CacheScope, Entry, KeyIdentity, PointRows, Points};
use std::sync::{Arc, Mutex};

type Lookup<K, V> = (Option<PointRows<K, V>>, Option<Ticket>);

#[derive(Clone, Debug)]
pub(super) struct State {
    tag: Arc<()>,
    head: Arc<Mutex<Head>>,
    points: Arc<Points>,
}
#[derive(Debug)]
struct Head {
    tag: Arc<()>,
    scope: CacheScope,
    populated: bool,
}
#[derive(Clone)]
pub(super) struct Ticket {
    state: State,
    scope: CacheScope,
}
impl State {
    pub(super) fn new(cache: &BlockCache) -> Option<Self> {
        if !cache.admits_points() {
            return None;
        }
        let tag = Arc::new(());
        Some(Self {
            head: Arc::new(Mutex::new(Head {
                tag: tag.clone(),
                scope: CacheScope::new(),
                populated: false,
            })),
            tag,
            points: cache.logical_points(),
        })
    }
    pub(super) fn lookup<K: BatchData, V: BatchData>(
        &self,
        key: &KeyIdentity<K>,
        allowance: usize,
    ) -> Lookup<K, V> {
        let Ok(mut head) = self.head.lock() else { return (None, None) };
        if !Arc::ptr_eq(&head.tag, &self.tag) {
            head.tag = self.tag.clone();
            head.scope = CacheScope::new();
            head.populated = false;
        }
        let rows = self.points.get(&head.scope, key, allowance).unwrap_or_else(|_| {
            head.scope = CacheScope::new();
            head.populated = false;
            None
        });
        if rows.is_some() {
            self.points.record_hit();
        }
        let ticket = Ticket { state: self.clone(), scope: head.scope.clone() };
        (rows, Some(ticket))
    }
    pub(super) fn append<K: BatchData, V: BatchData>(&self, added: &[Run<K, V>]) -> Self {
        let tag = Arc::new(());
        if let Ok(mut head) = self.head.lock() {
            let update = Arc::ptr_eq(&head.tag, &self.tag)
                && added.iter().all(|run| matches!(run, Run::Memory(_)));
            if !update || (head.populated && self.update(&head.scope, added).is_err()) {
                head.scope = CacheScope::new();
                head.populated = false;
            }
            head.tag = tag.clone();
        }
        Self { tag, head: self.head.clone(), points: self.points.clone() }
    }
    fn update<K: BatchData, V: BatchData>(
        &self,
        scope: &CacheScope,
        added: &[Run<K, V>],
    ) -> anyhow::Result<()> {
        for run in added {
            let Run::Memory(batch) = run else { continue };
            let rows = batch.shared_entries();
            let mut start = 0;
            while start < rows.len() {
                let key = KeyIdentity::new(&rows[start].0.0)?;
                let end = start + rows[start..].partition_point(|row| &row.0.0 == key.key());
                self.update_key(scope, &key, &rows[start..end])?;
                start = end;
            }
        }
        Ok(())
    }
    fn update_key<K: BatchData, V: BatchData>(
        &self,
        scope: &CacheScope,
        key: &KeyIdentity<K>,
        rows: &[Entry<K, V>],
    ) -> anyhow::Result<()> {
        let Some(prior) = self.points.get::<K, V>(scope, key, 64 * 1024)? else { return Ok(()) };
        crate::engine::execution::record_size(&rows, 64 * 1024)?;
        let merged = crate::engine::zset::merge_canonical(&prior.rows, rows)?;
        self.points.insert(scope, key, Arc::new(merged)).map(|_| ())
    }
}
impl Ticket {
    pub(super) fn remember<K: BatchData, V: BatchData>(
        &self,
        key: &KeyIdentity<K>,
        rows: Arc<Vec<Entry<K, V>>>,
    ) {
        let Ok(mut head) = self.state.head.lock() else { return };
        if !Arc::ptr_eq(&head.tag, &self.state.tag) || head.scope != self.scope {
            return;
        }
        let Ok(admitted) = self.state.points.insert(&self.scope, key, rows) else {
            head.scope = CacheScope::new();
            head.populated = false;
            return;
        };
        head.populated |= admitted;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Batch;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static SERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
    struct Counted(i64);
    impl Serialize for Counted {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            SERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
            serializer.serialize_i64(self.0)
        }
    }
    #[test]
    fn unpopulated_lineages_skip_key_hashing_until_successful_admission() -> anyhow::Result<()> {
        let state = State::new(&BlockCache::new(4096, 8))
            .ok_or_else(|| anyhow::anyhow!("cache disabled"))?;
        let run = Run::Memory(Arc::new(Batch::from_updates([((Counted(1), 1_i64), 1)])?));
        SERIALIZATIONS.store(0, Ordering::Relaxed);
        let next = state.append(std::slice::from_ref(&run));
        assert_eq!(SERIALIZATIONS.load(Ordering::Relaxed), 0);
        assert!(!next.head.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?.populated);
        assert!(!Arc::ptr_eq(&state.tag, &next.tag));
        let key = KeyIdentity::new(&Counted(1))?;
        let (_, ticket) = next.lookup::<Counted, i64>(&key, 4096);
        ticket
            .ok_or_else(|| anyhow::anyhow!("missing ticket"))?
            .remember(&key, Arc::new(vec![((Counted(1), 1_i64), 1)]));
        assert!(next.head.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?.populated);
        SERIALIZATIONS.store(0, Ordering::Relaxed);
        let latest = next.append(&[run]);
        assert!(SERIALIZATIONS.load(Ordering::Relaxed) > 0);
        let (rows, _) = latest.lookup::<Counted, i64>(&key, 4096);
        assert_eq!(rows.ok_or_else(|| anyhow::anyhow!("missing updated key"))?.rows[0].1, 2);
        let _ = state.lookup::<Counted, i64>(&key, 4096);
        assert!(!state.head.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?.populated);
        Ok(())
    }
}
