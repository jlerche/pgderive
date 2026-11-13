//! Durable reservation of billable attempts before issuing object requests.
use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

/// A shared on-disk Class A allowance. Keep this directory across worker restarts.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Absolute ledger path, shared by every process charged to this allowance.
    pub ledger: PathBuf,
    /// Maximum attempted PUTs, listing pages and copies in each UTC window.
    pub class_a_limit: u64,
    /// UTC-aligned accounting window length in seconds.
    pub window_secs: u64,
}
impl Config {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(self.ledger.is_absolute(), "operation budget ledger must be absolute");
        ensure!(self.class_a_limit > 0, "operation budget limit must be positive");
        ensure!((1..=2_678_400).contains(&self.window_secs), "invalid operation budget window");
        Ok(())
    }
}
/// Most recently observed durable usage, including reservations from peer processes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Stats {
    window_start: u64,
    window_secs: u64,
    class_a_limit: u64,
    used: u64,
    remaining: u64,
    puts: u64,
    listing_pages: u64,
    copies: u64,
}
#[derive(Debug)]
struct Exhausted(Stats);
impl std::fmt::Display for Exhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "operation budget exceeded: used {} of {} Class A attempts; window starts {} for {} seconds; retain source WAL and renew allowance before restarting",
            self.0.used, self.0.class_a_limit, self.0.window_start, self.0.window_secs
        )
    }
}
impl std::error::Error for Exhausted {}
/// Identify permanent allowance exhaustion through layered storage errors.
pub fn exhausted(error: &anyhow::Error) -> bool {
    error.chain().any(<dyn std::error::Error>::is::<Exhausted>)
}
#[derive(Clone, Copy)]
pub(super) enum Operation {
    Put,
    List,
    Copy,
}
#[derive(Debug)]
pub(super) struct Budget {
    config: Config,
    observed: Mutex<Stats>,
}
impl Budget {
    pub(super) fn new(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        let budget = Arc::new(Self { config, observed: Mutex::new(Stats::default()) });
        budget.update(None, now)?;
        Ok(budget)
    }
    pub(super) fn stats(&self) -> Option<Stats> {
        self.observed.lock().ok().map(|stats| stats.clone())
    }
    pub(super) async fn reserve(
        self: &Arc<Self>,
        operation: Operation,
    ) -> object_store::Result<()> {
        let budget = self.clone();
        tokio::task::spawn_blocking(move || budget.update(Some(operation), now))
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "operation-budget",
                source: error.into(),
            })?
            .map_err(|error| object_store::Error::Generic {
                store: "operation-budget",
                source: error.reallocate_into_boxed_dyn_error_without_backtrace(),
            })
    }
    fn update(
        &self,
        operation: Option<Operation>,
        clock: impl FnOnce() -> Result<u64>,
    ) -> Result<()> {
        let mut lock_path = self.config.ledger.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .context("opening operation budget lock")?;
        lock.lock().context("locking operation budget ledger")?;
        let mut stats = self.load(clock()?)?;
        if let Some(operation) = operation {
            *self.observed.lock().map_err(|_| anyhow::anyhow!("poisoned budget statistics"))? =
                stats.clone();
            if stats.remaining == 0 {
                return Err(Exhausted(stats).into());
            }
            stats.used += 1;
            stats.remaining -= 1;
            match operation {
                Operation::Put => stats.puts += 1,
                Operation::List => stats.listing_pages += 1,
                Operation::Copy => stats.copies += 1,
            }
        }
        persist(&self.config.ledger, &stats)?;
        *self.observed.lock().map_err(|_| anyhow::anyhow!("poisoned budget statistics"))? = stats;
        Ok(())
    }
    fn load(&self, now: u64) -> Result<Stats> {
        let previous = match std::fs::read(&self.config.ledger) {
            Ok(bytes) => Some(
                serde_json::from_slice::<Stats>(&bytes)
                    .context("invalid operation budget ledger; refusing to reset allowance")?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("reading operation budget ledger"),
        };
        let start = (now / self.config.window_secs) * self.config.window_secs;
        if let Some(mut previous) = previous {
            ensure!(
                previous.window_secs == self.config.window_secs,
                "operation budget window changed; use a separately authorized ledger"
            );
            ensure!(
                previous.used
                    == previous
                        .puts
                        .checked_add(previous.listing_pages)
                        .and_then(|n| n.checked_add(previous.copies))
                        .context("budget count overflow")?,
                "invalid operation budget accounting"
            );
            if start <= previous.window_start {
                previous.class_a_limit = self.config.class_a_limit;
                previous.remaining = self.config.class_a_limit.saturating_sub(previous.used);
                return Ok(previous);
            }
        }
        Ok(Stats {
            window_start: start,
            window_secs: self.config.window_secs,
            class_a_limit: self.config.class_a_limit,
            remaining: self.config.class_a_limit,
            ..Stats::default()
        })
    }
}
fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
fn persist(path: &Path, stats: &Stats) -> Result<()> {
    let directory = path.parent().context("budget ledger has no parent directory")?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    serde_json::to_writer(&mut temporary, stats)?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests;
