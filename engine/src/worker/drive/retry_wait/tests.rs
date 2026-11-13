use super::*;

#[tokio::test(start_paused = true)]
async fn virtual_retry_schedule_preserves_exponential_delays_and_cap() -> Result<()> {
    let (_sender, mut stop) = watch::channel(false);
    for (attempt, expected) in [(1, 100), (2, 200), (6, 3200), (7, 5000), (u32::MAX, 5000)] {
        let started = tokio::time::Instant::now();
        backoff(100, attempt, &mut stop).await?;
        assert_eq!(started.elapsed(), Duration::from_millis(expected));
    }
    assert!(backoff(100, 0, &mut stop).await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn virtual_retry_wait_is_interrupted_by_shutdown_and_closed_monitor() -> Result<()> {
    let (sender, mut stop) = watch::channel(false);
    let signal = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        sender.send(true)?;
        Ok::<_, anyhow::Error>(())
    });
    let started = tokio::time::Instant::now();
    backoff(100, 7, &mut stop).await?;
    assert_eq!(started.elapsed(), Duration::from_millis(10));
    assert!(*stop.borrow());
    signal.await??;
    let (sender, mut closed) = watch::channel(false);
    drop(sender);
    assert!(backoff(100, 1, &mut closed).await.is_err());
    Ok(())
}
