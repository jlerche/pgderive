use super::*;
#[tokio::test(start_paused = true)]
async fn foreground_deadline_still_expires_but_does_not_include_prior_maintenance() -> Result<()> {
    let clock = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_hours(2)).await;
    run(1, async { Ok(()) }).await?;
    let error =
        run(1, std::future::pending::<Result<()>>()).await.err().context("missing timeout")?;
    assert!(error.to_string().contains("without ACK"));
    assert_eq!(clock.elapsed(), Duration::from_hours(2) + Duration::from_secs(1));
    Ok(())
}
