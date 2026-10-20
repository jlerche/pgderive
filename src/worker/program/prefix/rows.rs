use super::Key;
use crate::{
    engine::{
        dataflow::PartitionWork,
        trace::{RangeCursor, TraceSnapshot},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
pub(super) type Entry = ((Key, Row), i64);
/// Bounded delta overlay over an ordered retained range; no partition materialization.
pub(super) struct Rows<'a> {
    prior: RangeCursor<Key, Row>,
    delta: &'a [Entry],
    index: usize,
}
impl<'a> Rows<'a> {
    pub(super) async fn open(
        prior: &TraceSnapshot<Key, Row>,
        delta: &'a [Entry],
        bounds: (&Key, &Key),
    ) -> Result<Self> {
        Ok(Self { prior: prior.range_cursor(bounds.0, bounds.1).await?, delta, index: 0 })
    }
    pub(super) async fn next(&mut self, work: &mut PartitionWork) -> Result<Option<Entry>> {
        loop {
            let old = self.prior.current().map(|(key, row, w)| ((key.clone(), row.clone()), w));
            let changed = self.delta.get(self.index);
            let tuple = old
                .as_ref()
                .map(|entry| &entry.0)
                .into_iter()
                .chain(changed.map(|entry| &entry.0))
                .min()
                .cloned();
            let Some(tuple) = tuple else { return Ok(None) };
            work.charge(1)?;
            let mut weight = 0_i64;
            if let Some((candidate, w)) = &old
                && candidate == &tuple
            {
                weight = *w;
                self.prior.advance().await?;
            }
            if let Some((candidate, w)) = changed
                && candidate == &tuple
            {
                weight = weight.checked_add(*w).context("prefix row multiplicity overflow")?;
                self.index += 1;
            }
            ensure!(weight >= 0, "negative prefix input multiplicity");
            if weight != 0 {
                return Ok(Some((tuple, weight)));
            }
        }
    }
}
