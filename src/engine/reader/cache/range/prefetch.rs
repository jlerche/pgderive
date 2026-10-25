//! Reserve actual shared windows, rather than charging a maximum window per page.
use super::{Identity, Pack, Ranges, ReadGuard, Window};
use anyhow::{Context as _, Result, ensure};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use object_store::ObjectStore;
use std::{collections::BTreeMap, ops::Range, sync::Arc};

mod coalesce;

pub struct ReadAhead {
    ranges: Arc<Ranges>,
    width: u64,
    windows: BTreeMap<Identity, Planned>,
    bytes: usize,
    missing: usize,
    #[cfg(test)]
    planning_steps: usize,
}
#[cfg_attr(test, derive(Clone))]
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
            let Some((identity, mut window)) = self.cover(store, pack, position)? else {
                return Ok(false);
            };
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
            self.admit_plan(identity, window)?;
        }
        self.ranges.metrics.requested(self.ranges.scan, requested.end - requested.start);
        Ok(true)
    }
    fn admit_plan(&mut self, identity: Identity, window: Planned) -> Result<()> {
        if let Some(existing) = self.windows.get_mut(&identity) {
            existing.used |= window.used;
            return Ok(());
        }
        self.missing = if self.ranges.coalesces() {
            coalesce::missing_after(&self.windows, &identity, &window, self.missing)?
        } else {
            self.missing
                .checked_add(usize::from(window.bytes.is_none()))
                .context("missing-window count overflow")?
        };
        self.windows.insert(identity, window);
        Ok(())
    }
    fn cover(
        &self,
        store: &Arc<dyn ObjectStore>,
        pack: &Pack,
        position: u64,
    ) -> Result<Option<(Identity, Planned)>> {
        let mut identities = Vec::new();
        for width in self.ranges.widths().into_iter().rev() {
            let width = u64::try_from(width)?;
            let identity =
                (pack.path.clone(), pack.hash.clone(), position / width * width, pack.bytes, width);
            if let Some(window) = self.windows.get(&identity) {
                return Ok(Some((
                    identity,
                    Planned {
                        store: window.store.clone(),
                        bytes: window.bytes.clone(),
                        length: window.length,
                        used: window.used,
                    },
                )));
            }
            identities.push(identity);
        }
        let state =
            self.ranges.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        for identity in identities {
            if let Some(window) = state.windows.get(&identity) {
                return self.cached_cover(store, position, identity, window).map(Some);
            }
        }
        drop(state);
        let Some(width) = self.ranges.fitting_width(pack, position, self.width)? else {
            return Ok(None);
        };
        let start = position / width * width;
        let end = start.saturating_add(width).min(pack.bytes);
        Ok(Some((
            (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width),
            Planned {
                store: store.clone(),
                bytes: None,
                length: usize::try_from(end - start)?,
                used: 0,
            },
        )))
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
    pub(crate) async fn load(plans: Vec<Self>) -> Result<ReadGuard> {
        let pins = stream::iter(coalesce::groups(plans)?)
            .map(coalesce::Group::load)
            .buffer_unordered(crate::engine::reader::READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(ReadGuard { _pins: pins })
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
