use super::supervise;
use super::tasks::Tasks;
use anyhow::{Result, bail};
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::watch;

#[tokio::test]
async fn failure_isolated_until_shutdown_drains_peers() -> Result<()> {
    let (stop, receiver) = watch::channel(false);
    let drained = Arc::new(AtomicUsize::new(0));
    let mut workers = Tasks::default();
    for index in 0..3 {
        let mut receiver = receiver.clone();
        let drained = drained.clone();
        workers.spawn(identity()?, async move {
            if index == 0 {
                bail!("fixture failure");
            }
            receiver.wait_for(|stopped| *stopped).await?;
            tokio::task::yield_now().await;
            drained.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
    }
    let observe = drained.clone();
    let shutdown = async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(observe.load(Ordering::SeqCst), 0);
        Ok(())
    };
    let result = supervise(workers, stop, shutdown).await;
    assert!(result.is_err_and(|error| error.to_string() == "fixture failure"));
    assert_eq!(drained.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_every_query() -> Result<()> {
    let (stop, receiver) = watch::channel(false);
    let mut workers = Tasks::default();
    for _ in 0..3 {
        let mut receiver = receiver.clone();
        workers.spawn(identity()?, async move {
            receiver.wait_for(|stopped| *stopped).await?;
            Ok(())
        });
    }
    supervise(workers, stop, async { Ok(()) }).await
}

#[tokio::test]
async fn finite_queries_finish_without_waiting_for_signal() -> Result<()> {
    let (stop, _receiver) = watch::channel(false);
    let mut workers = Tasks::default();
    workers.spawn(identity()?, async { Ok(()) });
    supervise(workers, stop, pending()).await
}

#[tokio::test]
async fn signal_failure_still_drains_queries() -> Result<()> {
    let (stop, mut receiver) = watch::channel(false);
    let mut workers = Tasks::default();
    workers.spawn(identity()?, async move {
        receiver.wait_for(|stopped| *stopped).await?;
        Ok(())
    });
    let result = supervise(workers, stop, async { Err(std::io::Error::other("fixture")) }).await;
    assert!(result.is_err_and(|error| error.to_string() == "worker Ctrl-C monitor failed"));
    Ok(())
}

#[tokio::test]
async fn empty_query_list_is_rejected() {
    assert!(super::run_many(Vec::new(), crate::worker::ProcessLimits::default()).await.is_err());
}

fn identity() -> Result<crate::diagnostics::Identity> {
    let config: crate::Config = serde_json::from_value(serde_json::json!({
        "postgres":{"host":"localhost","port":5432,"database":"test","user":"postgres","tls":"disable"},
        "replication":{"slot":"test","publication":"test"}
    }))?;
    let settings: crate::worker::Settings = serde_json::from_value(serde_json::json!({
        "tenant_id":"tenant", "catalog_schema":"catalog", "query_id":"query", "sink_table":"result",
        "object_prefix":"prefix", "block_rows":64,"maintenance_ticks":0,"retry_attempts":1,"retry_delay_ms":1,
        "max_transactions":0,"query":{"sql":"SELECT 1"}
    }))?;
    Ok(crate::diagnostics::Identity::new(&config, &settings))
}
