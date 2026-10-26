use super::*;
#[tokio::test(start_paused = true)]
async fn long_streaming_job_is_not_subject_to_foreground_timeout() -> Result<()> {
    let started = Instant::now();
    let task = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_hours(2)).await;
        anyhow::bail!("injected final storage failure")
    });
    let mut pending = Some(Pending { task, upload: None, reader: None, started, resolved: false });
    let error = ready(&mut pending).await.err().context("missing injected error")?;
    assert!(error.to_string().contains("injected final storage failure"));
    assert_eq!(started.elapsed(), std::time::Duration::from_hours(2));
    assert!(pending.as_ref().context("lost pending job")?.resolved);
    Ok(())
}
#[tokio::test]
async fn dropping_unresolved_consolidation_aborts_its_task() {
    let task = tokio::spawn(std::future::pending::<Result<Uploaded>>());
    let aborted = task.abort_handle();
    let pending =
        Pending { task, upload: None, reader: None, started: Instant::now(), resolved: false };
    drop(pending);
    tokio::task::yield_now().await;
    assert!(aborted.is_finished());
}
