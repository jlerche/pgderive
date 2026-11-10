//! Production retry waits, separated from transport I/O for virtual-time tests.
use anyhow::{Context, Result};
use tokio::{
    sync::watch,
    time::{Duration, sleep},
};

pub(super) async fn backoff(
    base_ms: u64,
    attempt: u32,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    let shift = attempt.checked_sub(1).context("retry before a failed attempt")?;
    let delay = base_ms.saturating_mul(1_u64.checked_shl(shift).unwrap_or(u64::MAX)).min(5000);
    tokio::select! {
        ()=sleep(Duration::from_millis(delay))=>{},
        changed=stop.changed()=>{changed.context("worker stop monitor closed")?;}
    }
    Ok(())
}

#[cfg(test)]
mod tests;
