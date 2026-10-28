use super::*;
use anyhow::Result;

#[tokio::test(start_paused = true)]
async fn foreground_demand_shortens_turns_without_withholding_maintenance() -> Result<()> {
    let pool = Arc::new(Workspaces::new(2)?);
    for foreground in [false, true] {
        let permit = if foreground { Some(pool.acquire().await?) } else { None };
        let _maintenance = pool.background().await?;
        let ((), report) = scope(pool.clone(), async {
            for _ in 0..4096 {
                super::super::super::reader::work::spend(0).await;
            }
        })
        .await;
        assert_eq!(report.work_units, 4096);
        assert_eq!(report.yields, if foreground { 128 } else { 32 });
        assert_eq!(report.work_bytes, 0);
        drop(permit);
    }
    assert_eq!(pool.stats().active, 0);
    assert_eq!(pool.stats().foreground_demand, 0);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn oversized_indivisible_work_and_cancellation_do_not_wait_for_idle() -> Result<()> {
    let pool = Arc::new(Workspaces::new(1)?);
    let (result, report) = scope(pool.clone(), async {
        let _permit = pool.background().await?;
        crate::engine::reader::work::spend(8 * 1024 * 1024).await;
        retained(123);
        coverage(1000, 200);
        coverage(1000, 900);
        selection(123, 2, false, false);
        selection(456, 3, true, true);
        escalation(true);
        escalation(false);
        Ok::<_, anyhow::Error>(())
    })
    .await;
    result?;
    assert_eq!(report.work_bytes, 8 * 1024 * 1024);
    assert_eq!(report.work_units, 2049);
    assert_eq!(report.yields, 1);
    assert_eq!(report.retained_bytes, 123);
    assert_eq!(report.referenced_pack_bytes, 2000);
    assert_eq!(report.sparse_pack_slack_bytes, 800);
    assert_eq!(report.selected_bytes, 579);
    assert_eq!(report.selected_runs, 5);
    assert_eq!(report.oversized_selections, 1);
    assert_eq!(report.pressure_selections, 1);
    assert_eq!(report.overflow_compactions, 1);
    assert_eq!(report.root_limit_compactions, 1);
    let (send, ready) = tokio::sync::oneshot::channel();
    let worker = pool.clone();
    let task = tokio::spawn(async move {
        scope(worker.clone(), async move {
            let _permit = worker.background().await?;
            let _sent = send.send(());
            std::future::pending::<Result<()>>().await
        })
        .await
    });
    ready.await?;
    task.abort();
    assert!(task.await.is_err());
    assert_eq!(pool.stats().active, 0);
    assert!(current().is_none());
    Ok(())
}
