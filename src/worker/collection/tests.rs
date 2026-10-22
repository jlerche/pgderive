use super::Collector;
use crate::catalog::Collection;
use anyhow::Result;
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};

#[tokio::test]
async fn cancelling_wait_retains_owned_task_and_shutdown_drains_it() -> Result<()> {
    let (send, recv) = oneshot::channel();
    let task = tokio::spawn(async move {
        recv.await?;
        Ok(Collection::Collected { retained: 1, deleted: 0 })
    });
    let mut collector = Collector { task: Some(task), session: None };
    assert!(timeout(Duration::from_millis(1), collector.completed()).await.is_err());
    assert!(collector.task.is_some());
    send.send(()).map_err(|()| anyhow::anyhow!("collector receiver closed"))?;
    collector.drain().await?;
    assert!(collector.task.is_none());
    collector.drain().await?;
    Ok(())
}

#[tokio::test]
async fn collection_failure_is_consumed_once() -> Result<()> {
    let mut collector = Collector {
        task: Some(tokio::spawn(async { anyhow::bail!("injected collection failure") })),
        session: None,
    };
    assert!(collector.completed().await.is_err());
    assert!(collector.task.is_none());
    collector.drain().await?;
    let task = tokio::spawn(std::future::pending::<Result<Collection>>());
    task.abort();
    collector.task = Some(task);
    assert!(collector.completed().await.is_err());
    assert!(collector.task.is_none());
    collector.drain().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_collector_aborts_owned_task() -> Result<()> {
    struct Finished(Option<oneshot::Sender<()>>);
    impl Drop for Finished {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _closed = sender.send(());
            }
        }
    }
    let (started, ready) = oneshot::channel();
    let (finished, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _finished = Finished(Some(finished));
        let _closed = started.send(());
        std::future::pending::<Result<Collection>>().await
    });
    let collector = Collector { task: Some(task), session: None };
    ready.await?;
    drop(collector);
    timeout(Duration::from_secs(1), stopped).await??;
    Ok(())
}
