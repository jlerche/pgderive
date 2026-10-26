//! Bound foreground operations, never an intervening consolidation debt drain.
use anyhow::{Context as _, Result};
use std::{future::Future, time::Duration};
pub(super) fn run<T>(
    seconds: u64,
    work: impl Future<Output = Result<T>>,
) -> impl Future<Output = Result<T>> {
    let work = Box::pin(work);
    async move {
        tokio::time::timeout(Duration::from_secs(seconds), work)
            .await
            .context("worker operation timed out; recover retained WAL without ACK")?
    }
}
#[cfg(test)]
mod tests;
