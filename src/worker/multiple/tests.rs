use super::supervise;
use anyhow::{Result, bail};
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{sync::watch, task::JoinSet};

#[tokio::test]
async fn failure_drains_peers_without_cancelling_them() -> Result<()> {
    let (stop, receiver) = watch::channel(false);
    let drained = Arc::new(AtomicUsize::new(0));
    let mut workers = JoinSet::new();
    for index in 0..3 {
        let mut receiver = receiver.clone();
        let drained = drained.clone();
        workers.spawn(async move {
            if index == 0 {
                bail!("fixture failure");
            }
            receiver.wait_for(|stopped| *stopped).await?;
            tokio::task::yield_now().await;
            drained.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
    }
    let result = supervise(workers, stop, pending()).await;
    assert!(result.is_err_and(|error| error.to_string() == "fixture failure"));
    assert_eq!(drained.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_every_query() -> Result<()> {
    let (stop, receiver) = watch::channel(false);
    let mut workers = JoinSet::new();
    for _ in 0..3 {
        let mut receiver = receiver.clone();
        workers.spawn(async move {
            receiver.wait_for(|stopped| *stopped).await?;
            Ok(())
        });
    }
    supervise(workers, stop, async { Ok(()) }).await
}

#[tokio::test]
async fn finite_queries_finish_without_waiting_for_signal() -> Result<()> {
    let (stop, _receiver) = watch::channel(false);
    let mut workers = JoinSet::new();
    workers.spawn(async { Ok(()) });
    supervise(workers, stop, pending()).await
}

#[tokio::test]
async fn signal_failure_still_drains_queries() -> Result<()> {
    let (stop, mut receiver) = watch::channel(false);
    let mut workers = JoinSet::new();
    workers.spawn(async move {
        receiver.wait_for(|stopped| *stopped).await?;
        Ok(())
    });
    let result = supervise(workers, stop, async { Err(std::io::Error::other("fixture")) }).await;
    assert!(result.is_err_and(|error| error.to_string() == "worker Ctrl-C monitor failed"));
    Ok(())
}

#[tokio::test]
async fn empty_query_list_is_rejected() {
    assert!(super::run_many(Vec::new()).await.is_err());
}
