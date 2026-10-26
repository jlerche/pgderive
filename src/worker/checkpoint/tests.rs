use super::bounded_work;

#[test]
fn replay_budget_counts_all_epochs_since_durable_checkpoint() {
    assert!(!bounded_work(140, 12, 4096, 1));
    assert!(bounded_work(140, 12, 4097, 1));
    assert!(bounded_work(376, 1, 10, 9));
    assert!(!bounded_work(375, 1, 10, 9));
    assert!(!bounded_work(121, 119, 1000, 1));
    assert!(bounded_work(122, 120, 1001, 1));
}

#[tokio::test(start_paused = true)]
async fn virtual_checkpoint_timeout_retains_unresolved_upload_and_aborts_on_drop()
-> anyhow::Result<()> {
    use super::{Pending, Uploaded, ready};
    let started = tokio::time::Instant::now();
    let task = tokio::spawn(std::future::pending::<anyhow::Result<Uploaded>>());
    let aborted = task.abort_handle();
    let duration = std::time::Duration::from_hours(1);
    let mut pending = Some(Pending {
        task,
        protection: None,
        started,
        runs: 1,
        deadline: started + duration,
        resolved: false,
    });
    let error =
        ready(&mut pending).await.err().ok_or_else(|| anyhow::anyhow!("timeout missing"))?;
    assert!(error.to_string().contains("retain protected namespace and source WAL"));
    assert_eq!(started.elapsed(), duration);
    assert!(!pending.as_ref().ok_or_else(|| anyhow::anyhow!("upload disappeared"))?.resolved);
    drop(pending);
    tokio::task::yield_now().await;
    assert!(aborted.is_finished());
    Ok(())
}
