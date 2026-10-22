//! Resolve process-local source counts across an uncertain publication COMMIT.
use crate::catalog::{Durable, Progress};
use anyhow::{Context, Result, ensure};
#[derive(Default)]
pub(in crate::worker) struct Count {
    total: u64,
    attempted: Option<(Progress, u64)>,
}
impl Count {
    pub(in crate::worker) const fn total(&self) -> u64 {
        self.total
    }
    pub(in crate::worker) fn attempt(
        &mut self,
        progress: Progress,
        transactions: u64,
    ) -> Result<()> {
        ensure!(self.attempted.is_none() && transactions > 0, "unresolved epoch source count");
        self.attempted = Some((progress, transactions));
        Ok(())
    }
    pub(in crate::worker) fn complete(&mut self) -> Result<()> {
        let (_, count) = self.attempted.take().context("epoch source count missing")?;
        self.total = self.total.checked_add(count).context("worker source counter overflow")?;
        Ok(())
    }
    pub(in crate::worker) fn resolve(&mut self, durable: &Durable) -> Result<()> {
        if let Some((progress, _)) = &self.attempted {
            if durable.covers(progress)? {
                self.complete()?;
            } else {
                self.attempted = None;
            }
        }
        Ok(())
    }
}
