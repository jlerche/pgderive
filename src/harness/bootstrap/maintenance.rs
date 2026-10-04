use super::super::{connect, mvp::Query};
use crate::{
    Config,
    catalog::{Resolution, Writer},
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    schema: &str,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &super::super::recovery::Recovery,
) -> Result<()> {
    let config = &recovery.config;
    let pinned = graph.snapshot();
    let before = writer.confirmed(sql, graph.plan()).await?;
    let visible = pinned.output.materialize().await?;
    for mode in ["BEFORE", "AFTER"] {
        exercise(sql, graph, writer, config, mode).await?;
    }
    let protection = writer.protect_upload(sql, "maintenance-final").await?;
    let prepared = graph.prepare_compaction_protected(&protection).await?;
    let checkpoint = graph.prepared_compaction_checkpoint(&prepared)?;
    graph.publish_compaction(sql, writer, prepared).await?;
    protection.published(sql, writer, graph.plan(), &checkpoint).await?;
    let after = writer.confirmed(sql, graph.plan()).await?;
    ensure!(
        after.end == before.end
            && after.commit == before.commit
            && after.xid == before.xid
            && after.stored.checkpoint.time == before.stored.checkpoint.time
            && after.stored.epoch == before.stored.epoch + 3,
        "maintenance advanced logical/source state"
    );
    ensure!(
        pinned.output.materialize().await? == visible
            && graph.snapshot().output.materialize().await? == visible,
        "maintenance changed weighted state or pinned readers"
    );
    let report = recovery.recover_query(sql, schema, "snapshot_grouped").await?;
    ensure!(report.time == 2 && report.epoch == 5, "fresh process lost compacted membership");
    eprintln!(
        "MVP durable streaming compaction preserved source/sink/logical state and pinned reader"
    );
    Ok(())
}

async fn exercise(
    sql: &mut Client,
    graph: &mut Query,
    writer: &mut Writer,
    config: &Config,
    mode: &str,
) -> Result<()> {
    let protection = writer.protect_upload(sql, &format!("maintenance-{mode}")).await?;
    let prepared = graph.prepare_compaction_protected(&protection).await?;
    let checkpoint = graph.prepared_compaction_checkpoint(&prepared)?;
    if disconnect(config, writer, graph.plan(), &checkpoint, mode).await? {
        ensure!(
            writer.confirmed(sql, graph.plan()).await.is_err(),
            "uncertain maintenance authorized ACK"
        );
        ensure!(
            writer.maintain(sql, graph.plan(), &checkpoint).await.is_err(),
            "uncertain maintenance permitted blind retry"
        );
        let resolution = writer.reconcile_maintenance(sql, graph.plan(), &checkpoint).await?;
        ensure!(
            resolution
                == if mode == "BEFORE" { Resolution::NotCommitted } else { Resolution::Committed },
            "maintenance resolved wrong COMMIT outcome"
        );
        if resolution == Resolution::NotCommitted {
            writer.maintain(sql, graph.plan(), &checkpoint).await?;
        }
        graph.restore_checkpoint(checkpoint.clone()).await?;
        ensure!(
            graph.publish_compaction(sql, writer, prepared).await.is_err(),
            "recovered maintenance accepted stale preparation"
        );
    } else {
        graph.publish_compaction(sql, writer, prepared).await?;
    }
    protection.published(sql, writer, graph.plan(), &checkpoint).await?;
    eprintln!("MVP maintenance uncertain {mode} COMMIT resolved without logical/source advance");
    Ok(())
}

async fn disconnect(
    config: &Config,
    writer: &mut Writer,
    plan: &crate::engine::plan::Plan,
    checkpoint: &crate::engine::plan::Checkpoint,
    mode: &str,
) -> Result<bool> {
    let Ok(port) = std::env::var(format!("PGDERIVE_SQL_COMMIT_{mode}_PORT")) else {
        return Ok(false);
    };
    let mut config = config.clone();
    config.postgres.port = port.parse()?;
    let (mut fault, task) = connect(&config).await?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        writer.maintain(&mut fault, plan, checkpoint),
    )
    .await
    .context("maintenance COMMIT fault timed out")?;
    ensure!(result.is_err(), "maintenance fault did not disconnect COMMIT");
    drop(fault);
    let _disconnected = task.await.context("maintenance fault SQL task failed")?;
    Ok(true)
}
