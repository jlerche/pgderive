use super::{Budget, Config, Operation};
use anyhow::{Context as _, Result};
use futures_util::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, PutMode, PutOptions, path::Path};
use std::sync::{Arc, atomic::Ordering};

fn config(directory: &tempfile::TempDir, limit: u64) -> Config {
    Config {
        ledger: directory.path().join("allowance.json"),
        class_a_limit: limit,
        window_secs: 3600,
    }
}
#[tokio::test]
async fn failed_conditional_put_pages_and_restart_share_durable_allowance() -> Result<()> {
    use crate::{
        engine::tests::store::Store,
        storage::{PageStore as _, scoped::Scoped},
    };
    let directory = tempfile::tempdir()?;
    let config = config(&directory, 4);
    let raw = Arc::new(Store::unpaced());
    let budget = Budget::new(config.clone())?;
    let store = Scoped::new(raw.clone(), raw.clone(), Path::default(), 2)?
        .with_budget(Some(budget.clone()));
    let path = Path::from("immutable");
    store
        .put_opts(
            &path,
            vec![1_u8].into(),
            PutOptions { mode: PutMode::Create, ..PutOptions::default() },
        )
        .await?;
    assert!(
        store
            .put_opts(
                &path,
                vec![1_u8].into(),
                PutOptions { mode: PutMode::Create, ..PutOptions::default() }
            )
            .await
            .is_err()
    );
    store.page("", None, 1).await?;
    let reopened = Budget::new(config)?;
    reopened.reserve(Operation::Copy).await?;
    assert!(store.put(&Path::from("blocked"), vec![2_u8].into()).await.is_err());
    assert_eq!(raw.puts.load(Ordering::SeqCst), 2);
    assert_eq!(store.get_range(&path, 0..1).await?.len(), 1);
    store.delete(&path).await?;
    let stats = budget.stats().context("missing budget stats")?;
    assert_eq!(stats.used, 4);
    assert_eq!(stats.remaining, 0);
    assert_eq!(stats.puts, 2);
    assert_eq!(stats.listing_pages, 1);
    assert_eq!(stats.copies, 1);
    assert!(store.list_with_delimiter(None).await.is_err());
    assert!(store.list(None).try_collect::<Vec<_>>().await.is_err());
    Ok(())
}
#[tokio::test]
async fn concurrent_budget_handles_cannot_overspend_and_cancelled_work_stays_charged() -> Result<()>
{
    use futures_util::{StreamExt as _, TryStreamExt as _};
    let directory = tempfile::tempdir()?;
    let config = config(&directory, 7);
    let a = Budget::new(config.clone())?;
    let b = Budget::new(config)?;
    let results = futures_util::stream::iter(0..32)
        .map(|n| {
            let budget = if n % 2 == 0 { a.clone() } else { b.clone() };
            async move { Ok::<_, anyhow::Error>(budget.reserve(Operation::Put).await.is_ok()) }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(results.iter().filter(|allowed| **allowed).count(), 7);
    let error = a.reserve(Operation::List).await.err().context("expected exhaustion")?;
    assert!(super::exhausted(&anyhow::Error::new(error).context("checkpoint upload")));
    assert!(!super::exhausted(&anyhow::anyhow!("unrelated failure")));
    assert_eq!(a.stats().context("stats")?.used, 7);
    Ok(())
}
#[test]
fn utc_rollover_backward_clock_corruption_and_configuration_fail_closed() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = config(&directory, 2);
    let budget = Budget { config: config.clone(), observed: std::sync::Mutex::default() };
    budget.update(Some(Operation::Put), || Ok(7200))?;
    budget.update(Some(Operation::List), || Ok(7100))?;
    assert!(budget.update(Some(Operation::Put), || Ok(7199)).is_err());
    budget.update(Some(Operation::Copy), || Ok(10800))?;
    let stats = budget.stats().context("stats")?;
    assert_eq!(stats.window_start, 10800);
    assert_eq!(stats.used, 1);
    std::fs::write(&config.ledger, "broken")?;
    assert!(budget.update(Some(Operation::Put), || Ok(14400)).is_err());
    assert!(Budget::new(Config { window_secs: 0, ..config.clone() }).is_err());
    assert!(Budget::new(Config { ledger: "relative".into(), ..config }).is_err());
    assert!(Budget::new(Config { class_a_limit: 0, ..config }).is_err());
    Ok(())
}
#[test]
fn changed_window_or_unavailable_persistence_prevents_requests() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = config(&directory, 10);
    Budget::new(config.clone())?;
    assert!(Budget::new(Config { window_secs: 60, ..config.clone() }).is_err());
    std::fs::write(&config.ledger, br#"{"window_start":0,"window_secs":3600,"class_a_limit":10,"used":1,"remaining":9,"puts":0,"listing_pages":0,"copies":0}"#)?;
    assert!(Budget::new(config.clone()).is_err());
    assert!(
        Budget::new(Config { ledger: directory.path().join("missing/ledger"), ..config }).is_err()
    );
    Ok(())
}

#[test]
fn ledger_with_lock_extension_uses_distinct_lock_file() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config =
        Config { ledger: directory.path().join("allowance.lock"), ..config(&directory, 3) };
    let budget = Budget::new(config.clone())?;
    budget.update(Some(Operation::Put), || Ok(u64::MAX / 2))?;
    assert!(config.ledger.exists());
    assert!(directory.path().join("allowance.lock.lock").exists());
    assert_eq!(std::fs::read(directory.path().join("allowance.lock.lock"))?.len(), 0);
    Ok(())
}

#[test]
fn accounting_clock_is_sampled_under_lock_before_window_rollover() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = config(&directory, 1);
    let budget = Budget { config, observed: std::sync::Mutex::default() };
    budget.update(Some(Operation::Put), || Ok(7199))?;
    let observer = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("allowance.json.lock"))?;
    budget.update(Some(Operation::List), || {
        assert!(matches!(observer.try_lock(), Err(std::fs::TryLockError::WouldBlock)));
        Ok(7200)
    })?;
    let stats = budget.stats().context("missing budget stats")?;
    assert_eq!(stats.window_start, 7200);
    assert_eq!(stats.used, 1);
    assert_eq!(stats.puts, 0);
    assert_eq!(stats.listing_pages, 1);
    Ok(())
}
