//! Merge only contiguous planned misses; never bridge unread gaps.
use super::{Identity, Planned, Ranges, ReadAhead, Window};
use anyhow::{Context as _, Result, ensure};
use std::sync::Arc;

type Loaded = Arc<super::super::pins::Pinned>;
pub(super) struct Group {
    ranges: Arc<Ranges>,
    windows: Vec<(Identity, Planned)>,
}
pub(super) fn groups(plans: Vec<ReadAhead>) -> Result<Vec<Group>> {
    let mut groups: Vec<Group> = Vec::new();
    for plan in plans {
        for (identity, window) in plan.windows {
            let previous = groups.last_mut().filter(|group| {
                Arc::ptr_eq(&group.ranges, &plan.ranges) && plan.ranges.coalesces()
            });
            if let Some(previous) = previous
                && adjacent(
                    {
                        let (key, value) = previous.windows.last().context("empty read group")?;
                        (key, value)
                    },
                    (&identity, &window),
                )?
            {
                previous.windows.push((identity, window));
            } else {
                groups
                    .push(Group { ranges: plan.ranges.clone(), windows: vec![(identity, window)] });
            }
        }
    }
    Ok(groups)
}
pub(super) fn adjacent(left: (&Identity, &Planned), right: (&Identity, &Planned)) -> Result<bool> {
    let (last, prior) = left;
    let (next, window) = right;
    Ok(prior.bytes.is_none()
        && window.bytes.is_none()
        && Arc::ptr_eq(&prior.store, &window.store)
        && last.0 == next.0
        && last.1 == next.1
        && last.3 == next.3
        && last.2.checked_add(u64::try_from(prior.length)?).context("read group overflow")?
            == next.2)
}
pub(super) fn missing_after(
    windows: &std::collections::BTreeMap<Identity, Planned>,
    identity: &Identity,
    window: &Planned,
    missing: usize,
) -> Result<usize> {
    use std::ops::Bound::{Excluded, Unbounded};
    let previous = windows.range(..identity.clone()).next_back();
    let next = windows.range((Excluded(identity.clone()), Unbounded)).next();
    let before = previous
        .map(|(key, value)| adjacent((key, value), (identity, window)))
        .transpose()?
        .unwrap_or(false);
    let after = next
        .map(|(key, value)| adjacent((identity, window), (key, value)))
        .transpose()?
        .unwrap_or(false);
    let removed =
        previous.zip(next).map(|(left, right)| adjacent(left, right)).transpose()?.unwrap_or(false);
    missing
        .checked_add(usize::from(window.bytes.is_none()) + usize::from(removed))
        .and_then(|count| count.checked_sub(usize::from(before) + usize::from(after)))
        .context("invalid missing-window components")
}
impl Group {
    pub(super) async fn load(self) -> Result<Loaded> {
        if let [(identity, Planned { bytes: Some(bytes), .. })] = self.windows.as_slice() {
            return self.ranges.pin([(identity.clone(), Window::new(bytes.clone()))].into());
        }
        self.load_contiguous().await
    }
    async fn load_contiguous(self) -> Result<Loaded> {
        // Use the same stripes as ordinary window fetches, in one global order.
        // Cancellation releases every guard without advancing a probe cursor.
        let mut stripes = self
            .windows
            .iter()
            .map(|(identity, _)| Ranges::stripe(identity))
            .collect::<Result<Vec<_>>>()?;
        stripes.sort_unstable();
        stripes.dedup();
        #[cfg(test)]
        let flight_start = tokio::time::Instant::now();
        #[cfg(test)]
        let stripe_count = stripes.len();
        let mut guards = Vec::new();
        for stripe in stripes {
            guards.push(self.ranges.flights[stripe].lock().await);
        }
        #[cfg(test)]
        {
            let mut state = self
                .ranges
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
            state.flight_wait_us += flight_start.elapsed().as_micros();
            state.flight_groups += 1;
            state.flight_stripes += stripe_count;
        }
        let mut loaded = Vec::new();
        for (identity, _) in &self.windows {
            let bytes = self
                .ranges
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
                .get_or_cover(identity)?;
            loaded.push(bytes.map(Window::new));
        }
        let mut position = 0;
        while position < loaded.len() {
            if loaded[position].is_some() {
                position += 1;
                continue;
            }
            let first = position;
            while position < loaded.len() && loaded[position].is_none() {
                position += 1;
            }
            self.load_span(first..position, &mut loaded).await?;
        }
        for ((identity, _), window) in self.windows.iter().zip(&loaded) {
            self.ranges.retain(
                identity.clone(),
                window.as_ref().context("missing coalesced window")?.clone(),
            )?;
        }
        let windows = self
            .windows
            .into_iter()
            .zip(loaded)
            .map(|((identity, _), window)| {
                Ok((identity, window.context("missing coalesced window")?))
            })
            .collect::<Result<_>>()?;
        let pin = self.ranges.pin(windows)?;
        drop(guards);
        Ok(pin)
    }

    async fn load_span(
        &self,
        span: std::ops::Range<usize>,
        loaded: &mut [Option<Window>],
    ) -> Result<()> {
        let (first, request) = &self.windows[span.start];
        let (last, tail) = &self.windows[span.end - 1];
        let end =
            last.2.checked_add(u64::try_from(tail.length)?).context("coalesced end overflow")?;
        let bytes = self.ranges.load(&request.store, &first.0, first.2..end).await?;
        ensure!(u64::try_from(bytes.len())? == end - first.2, "coalesced range length mismatch");
        for index in span {
            let (identity, window) = &self.windows[index];
            let start = usize::try_from(identity.2 - first.2)?;
            let part = bytes
                .get(start..start + window.length)
                .context("coalesced window outside response")?;
            loaded[index] = Some(Window::new(Arc::new(part.to_vec())));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Group, Planned, Ranges, ReadAhead, groups};
    use crate::engine::tests::store::Store;
    use anyhow::{Context as _, Result};
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[test]
    fn overlapping_covers_and_bridges_keep_readiness_equal_to_missing_groups() -> Result<()> {
        let ranges = Arc::new(Ranges::new(512 * 1024, 8));
        let store: Arc<dyn ObjectStore> = Arc::new(Store::unpaced());
        let mut plan = ReadAhead::new(ranges)?.context("missing planner")?;
        for (start, length, cached) in [
            (0, 65_536, false),
            (131_072, 65_536, false),
            (65_536, 65_536, false),
            (100_000, 100_000, true),
            (196_608, 65_536, false),
        ] {
            let identity = ("pack".into(), "digest".into(), start, 300_000, length);
            let window = Planned {
                store: store.clone(),
                bytes: cached.then(|| Arc::new(vec![0; 100_000])),
                length: usize::try_from(length)?,
            };
            plan.admit_plan(identity, window)?;
            let snapshot = ReadAhead {
                windows: plan.windows.clone(),
                ..ReadAhead::new(plan.ranges.clone())?.context("missing planner")?
            };
            let actual = groups(vec![snapshot])?
                .iter()
                .filter(|Group { windows, .. }| {
                    windows.iter().any(|(_, window)| window.bytes.is_none())
                })
                .count();
            assert_eq!(plan.missing, actual);
        }
        assert_eq!(plan.missing, 2);
        Ok(())
    }
}
