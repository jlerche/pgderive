use super::*;
use crate::engine::tests::store::Store;
use std::collections::BTreeMap;

async fn tick(
    query: &mut program::relational::Query,
    compiled: &crate::compiler::Compiled,
) -> Result<()> {
    let id = query.time() + 1;
    let changes = super::super::changes(vec![super::super::Update {
        table: "entry".into(),
        weight: 1,
        row: BTreeMap::from([
            ("id".into(), Some(id.to_string())),
            ("account_id".into(), Some("1".into())),
            ("amount".into(), Some(id.to_string())),
        ]),
    }]);
    let input = program::relational::inputs(&changes, compiled.relational(), id)?;
    let work = query.prepare_memtable(input, 1_000_000).await?;
    query.commit(work)?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn forced_capture_clears_overtaken_prefix_debt_before_another_upload() -> Result<()> {
    let native = super::super::native();
    let compiled = crate::compiler::compile(
        "SELECT e.account_id,COUNT(*),SUM(e.amount) FROM source.entry e GROUP BY e.account_id",
        &native,
    )?;
    let store = Arc::new(Store::unpaced());
    let mut settings = super::super::replay_settings(crate::engine::execution::Limits::default())?;
    settings.store = store.clone();
    let mut query = program::relational::build_committed(&native, &compiled, &settings)?;
    let config = Config {
        memtable_bytes: 1_000_000,
        checkpoint_interval_ms: 1000,
        maintenance_ticks: 64,
        source_rows_per_second: 1000,
    };
    let mut lifecycle = Lifecycle::new(config, store);
    let mut old_job = None;
    for _ in 0..128 {
        tick(&mut query, &compiled).await?;
        let uploaded = query
            .capture_flush(format!("pgderive/upload-v1/debt-{}", query.time()))
            .await?
            .upload()
            .await?;
        query.commit_compaction(query.prepare_uploaded_flush(&uploaded).await?)?;
        lifecycle.durable = Some(uploaded.checkpoint().clone());
        lifecycle.report.checkpoints += 1;
        if query.time() == 8 {
            old_job = Some(query.capture_consolidation(
                uploaded.checkpoint(),
                "pgderive/upload-v1/old-debt".into(),
            )?);
        }
    }
    let frozen = old_job.context("missing old captured job")?;
    lifecycle.consolidation = Some(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_hours(2)).await;
        frozen.upload().await
    }));
    tick(&mut query, &compiled).await?;
    let clock = tokio::time::Instant::now();
    // Direct forced capture bypasses the worker consume-loop service. An old
    // small-prefix replacement still leaves >112 newer durable roots, so the
    // same capture entrypoint must perform another pressure consolidation.
    lifecycle.capture(&mut query).await?;
    assert!(clock.elapsed() >= Duration::from_hours(2));
    assert!(lifecycle.report.consolidations >= 2);
    assert!(!super::super::super::super::consolidation::must_drain(
        lifecycle.durable.as_ref().context("missing durable checkpoint")?
    ));
    lifecycle.install(&mut query).await?;
    assert!(lifecycle.is_quiescent());
    assert!(query.run_count() <= 2);
    let expected =
        BTreeMap::from([(vec![Some("1".into()), Some("129".into()), Some("8385".into())], 1)]);
    super::super::verify_snapshot(&query, &compiled, &expected).await?;
    super::super::verify_cold(&query, &native, &compiled, &settings, &expected).await
}
