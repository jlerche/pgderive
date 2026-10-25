//! Reserve actual shared windows, rather than charging a maximum window per page.
use super::{Identity, Pack, Ranges, Window};
use anyhow::{Context as _, Result, ensure};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use object_store::ObjectStore;
use std::{collections::BTreeMap, ops::Range, sync::Arc};

pub struct ReadAhead {
    ranges: Arc<Ranges>,
    width: u64,
    windows: BTreeMap<Identity, Planned>,
    bytes: usize,
    missing: usize,
    #[cfg(test)]
    planning_steps: usize,
}
struct Planned {
    store: Arc<dyn ObjectStore>,
    bytes: Option<Arc<Vec<u8>>>,
    length: usize,
    used: u64,
}
impl ReadAhead {
    pub(in crate::engine::reader) fn new(ranges: Arc<Ranges>) -> Result<Option<Self>> {
        if ranges.bytes == 0 || ranges.entries == 0 {
            return Ok(None);
        }
        let width = u64::try_from(
            ranges.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?.width,
        )?;
        Ok(Some(Self {
            ranges,
            width,
            windows: BTreeMap::new(),
            bytes: 0,
            missing: 0,
            #[cfg(test)]
            planning_steps: 0,
        }))
    }
    pub(in crate::engine::reader) fn shares(&self, ranges: &Arc<Ranges>) -> bool {
        Arc::ptr_eq(&self.ranges, ranges)
    }
    pub(in crate::engine::reader) fn reserve(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        pack: &Pack,
        requested: Range<u64>,
    ) -> Result<bool> {
        ensure!(requested.start < requested.end, "empty prefetch range");
        ensure!(requested.end <= pack.bytes, "prefetch range exceeds pack");
        if requested.end - requested.start > u64::try_from(self.ranges.bytes)? {
            return Ok(false);
        }
        let mut pending = BTreeMap::new();
        let mut position = requested.start;
        let mut entries = 0_usize;
        let mut bytes = 0_usize;
        while position < requested.end {
            #[cfg(test)]
            {
                self.planning_steps += 1;
            }
            let (identity, mut window) = self.cover(store, pack, position)?;
            let end = identity
                .2
                .checked_add(u64::try_from(window.length)?)
                .context("prefetch window overflow")?;
            window.used |= usage(identity.2, window.length, &(position..requested.end.min(end)))?;
            if !self.windows.contains_key(&identity) && !pending.contains_key(&identity) {
                entries += 1;
                bytes = bytes.checked_add(window.length).context("prefetch charge overflow")?;
                if entries > self.ranges.entries - self.windows.len()
                    || bytes > self.ranges.bytes - self.bytes
                {
                    return Ok(false);
                }
            }
            pending.insert(identity, window);
            position = requested.end.min(end);
        }
        self.bytes += bytes;
        for (identity, window) in pending {
            if let Some(existing) = self.windows.get_mut(&identity) {
                existing.used |= window.used;
            } else {
                self.missing += usize::from(window.bytes.is_none());
                self.windows.insert(identity, window);
            }
        }
        self.ranges.metrics.requested(self.ranges.scan, requested.end - requested.start);
        Ok(true)
    }
    fn cover(
        &self,
        store: &Arc<dyn ObjectStore>,
        pack: &Pack,
        position: u64,
    ) -> Result<(Identity, Planned)> {
        let mut identities = Vec::new();
        for width in self.ranges.widths().into_iter().rev() {
            let width = u64::try_from(width)?;
            let identity =
                (pack.path.clone(), pack.hash.clone(), position / width * width, pack.bytes, width);
            if let Some(window) = self.windows.get(&identity) {
                return Ok((
                    identity,
                    Planned {
                        store: window.store.clone(),
                        bytes: window.bytes.clone(),
                        length: window.length,
                        used: window.used,
                    },
                ));
            }
            identities.push(identity);
        }
        let state =
            self.ranges.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        for identity in identities {
            if let Some(window) = state.windows.get(&identity) {
                return self.cached_cover(store, position, identity, window);
            }
        }
        drop(state);
        let start = position / self.width * self.width;
        let end = start.saturating_add(self.width).min(pack.bytes);
        Ok((
            (pack.path.clone(), pack.hash.clone(), start, pack.bytes, self.width),
            Planned {
                store: store.clone(),
                bytes: None,
                length: usize::try_from(end - start)?,
                used: 0,
            },
        ))
    }
    fn cached_cover(
        &self,
        store: &Arc<dyn ObjectStore>,
        position: u64,
        identity: Identity,
        window: &Window,
    ) -> Result<(Identity, Planned)> {
        let start = position / self.width * self.width;
        let end = start.saturating_add(self.width).min(identity.3);
        if self.width < identity.4
            && start >= identity.2
            && end
                <= identity
                    .2
                    .checked_add(u64::try_from(window.bytes.len())?)
                    .context("cached cover overflow")?
        {
            let bytes = window.bytes
                [usize::try_from(start - identity.2)?..usize::try_from(end - identity.2)?]
                .to_vec();
            let length = bytes.len();
            return Ok((
                (identity.0, identity.1, start, identity.3, self.width),
                Planned { store: store.clone(), bytes: Some(Arc::new(bytes)), length, used: 0 },
            ));
        }
        Ok((
            identity,
            Planned {
                store: store.clone(),
                bytes: Some(window.bytes.clone()),
                length: window.bytes.len(),
                used: window.used,
            },
        ))
    }
    pub(crate) fn ready(plans: &[Self]) -> bool {
        plans.iter().map(|plan| plan.missing).sum::<usize>()
            >= crate::engine::reader::READ_CONCURRENCY
    }
    pub(crate) async fn load(plans: Vec<Self>) -> Result<()> {
        let requests = plans
            .into_iter()
            .flat_map(|plan| {
                plan.windows
                    .into_iter()
                    .map(move |(identity, window)| (plan.ranges.clone(), identity, window))
            })
            .collect::<Vec<_>>();
        let loaded = stream::iter(requests)
            .map(|(ranges, identity, window)| async move {
                let bytes = if let Some(bytes) = window.bytes {
                    bytes
                } else {
                    let end = identity
                        .2
                        .checked_add(u64::try_from(window.length)?)
                        .context("prefetch end overflow")?;
                    let first = identity.2
                        + u64::from(window.used.trailing_zeros())
                            * u64::try_from(window.length)?.div_ceil(64);
                    ranges.fetch(&window.store, identity.clone(), end, &(first..first + 1)).await?
                };
                Ok::<_, anyhow::Error>((
                    ranges,
                    identity,
                    Window { bytes, accessed: 0, used: window.used },
                ))
            })
            .buffered(crate::engine::reader::READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        // All requested windows fit together. Re-admit pinned cached covers that
        // another planned fetch may have evicted before its turn completed.
        for (ranges, identity, window) in loaded {
            ranges.retain(identity, window)?;
        }
        Ok(())
    }
}

fn usage(start: u64, length: usize, requested: &Range<u64>) -> Result<u64> {
    let unit = u64::try_from(length)?.div_ceil(64);
    let first = (requested.start - start) / unit;
    let last = (requested.end - 1 - start) / unit;
    Ok((first..=last).fold(0, |mask, bit| mask | (1_u64 << bit)))
}

#[cfg(test)]
mod tests;
