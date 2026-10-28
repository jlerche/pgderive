use super::Workspaces;
use anyhow::{Context as _, Result};
use std::{
    future::{Future as _, poll_fn},
    sync::Arc,
    task::Poll,
};

#[tokio::test]
async fn shared_admission_releases_cancelled_waiters_and_active_tasks() -> Result<()> {
    let pool = Arc::new(Workspaces::new(1)?);
    let permit = pool.acquire().await?;
    let mut queued = Box::pin(pool.acquire());
    poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(pool.stats().active, 1);
    assert_eq!(pool.stats().waits, 1);
    drop(queued);
    drop(permit);
    let (send, ready) = tokio::sync::oneshot::channel();
    let worker_pool = pool.clone();
    let task = tokio::spawn(async move {
        let _permit = worker_pool.acquire().await?;
        let _sent = send.send(());
        std::future::pending::<Result<()>>().await
    });
    ready.await?;
    task.abort();
    assert!(task.await.is_err());
    let next = pool.acquire().await?;
    assert_eq!(pool.stats().peak, 1);
    drop(next);
    assert_eq!(pool.stats().active, 0);
    pool.slots.close();
    assert!(pool.acquire().await.is_err());
    assert_eq!(pool.stats().foreground_demand, 0);
    assert!(Workspaces::new(0).is_err());
    assert!(Workspaces::new(257).is_err());
    Ok(())
}

#[tokio::test]
async fn background_work_preserves_foreground_headroom() -> Result<()> {
    let pool = Arc::new(Workspaces::new(2)?);
    let background = pool.background().await?;
    let mut queued = Box::pin(pool.background());
    poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(pool.stats().background_active, 1);
    assert_eq!(pool.stats().background_waits, 1);
    let foreground = tokio::time::timeout(std::time::Duration::from_secs(1), pool.acquire())
        .await
        .context("maintenance exhausted foreground headroom")??;
    assert_eq!(pool.stats().active, 2);
    drop(foreground);
    drop(background);
    drop(queued.await?);
    assert_eq!(pool.stats().active, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queued_foreground_demand_cancels_and_fifo_preserves_maintenance_service() -> Result<()> {
    let pool = Arc::new(Workspaces::new(1)?);
    for _ in 0..1024 {
        let held = pool.acquire().await?;
        let mut maintenance = Box::pin(pool.background());
        poll_fn(|cx| {
            assert!(maintenance.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let mut later = Box::pin(pool.acquire());
        poll_fn(|cx| {
            assert!(later.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(pool.stats().foreground_demand, 2);
        drop(held);
        let served = maintenance.await?;
        poll_fn(|cx| {
            assert!(later.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(later);
        assert_eq!(pool.stats().foreground_demand, 0);
        drop(served);
    }
    assert_eq!(pool.stats().active, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn admission_feedback_is_available_without_optional_phase_metrics() -> Result<()> {
    let pool = Arc::new(Workspaces::new(1)?);
    let held = pool.acquire().await?;
    assert!(held.waited().is_zero());
    let mut waiting = Box::pin(pool.acquire());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::advance(std::time::Duration::from_millis(7)).await;
    drop(held);
    let served = waiting.await?;
    let mut profile = crate::timing::Profile::new(false);
    profile.admitted(&served);
    assert_eq!(profile.admission(), std::time::Duration::from_millis(7));
    assert!(profile.finish().is_none());
    Ok(())
}
