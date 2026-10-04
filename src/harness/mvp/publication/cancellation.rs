use super::{PublicationFixture, Query};
use crate::catalog::{Publication, Resolution, Writer};
use anyhow::{Context, Result, ensure};
use std::{path::PathBuf, time::Duration};
use tokio_postgres::Client;

impl PublicationFixture {
    pub(super) async fn held_commit(
        &mut self,
        sql: &mut Client,
        graph: &mut Query,
        attempt: Publication<'_>,
    ) -> Result<bool> {
        let Ok(port) = std::env::var("PGDERIVE_SQL_COMMIT_HOLD_PORT") else { return Ok(false) };
        let gate = PathBuf::from(std::env::var("PGDERIVE_SQL_COMMIT_HOLD_GATE")?);
        let mut config = self.config.clone();
        config.postgres.port = port.parse()?;
        let (mut fault, task) = crate::harness::connect(&config).await?;
        cancel_at_commit(&mut self.grouped, &mut fault, graph.plan(), attempt, &gate).await?;
        ensure!(
            self.grouped.confirmed(sql, graph.plan()).await.is_err(),
            "cancelled COMMIT authorized acknowledgement"
        );
        let mut resolution = Box::pin(self.grouped.reconcile(sql, graph.plan(), attempt));
        ensure!(
            tokio::time::timeout(Duration::from_millis(350), &mut resolution).await.is_err(),
            "reconciliation did not wait for original pending transaction"
        );
        std::fs::write(&gate, b"release")?;
        let result = tokio::time::timeout(Duration::from_secs(15), resolution)
            .await
            .context("held COMMIT reconciliation timed out")??;
        ensure!(result == Resolution::Committed, "released COMMIT resolved as uncommitted");
        drop(fault);
        task.await.context("held connection task panicked")??;
        let durable = self.grouped.confirmed(sql, graph.plan()).await?;
        graph.restore_checkpoint(durable.stored.checkpoint).await?;
        eprintln!(
            "MVP cancelled pending COMMIT blocked acknowledgement; reconciliation waited for original transaction and recovered committed state"
        );
        Ok(true)
    }
}
async fn cancel_at_commit(
    writer: &mut Writer,
    sql: &mut Client,
    plan: &crate::engine::plan::Plan,
    attempt: Publication<'_>,
    gate: &std::path::Path,
) -> Result<()> {
    let pending = PathBuf::from(format!("{}.pending", gate.display()));
    let mut publish = Box::pin(writer.publish(sql, plan, attempt));
    tokio::time::timeout(Duration::from_secs(15), async {
        tokio::select! {
            result = &mut publish => { result?; anyhow::bail!("held COMMIT completed before cancellation") },
            result = wait_pending(&pending) => result,
        }
    }).await.context("waiting for pending COMMIT")??;
    // Dropping this future cancels the response wait after COMMIT has been issued.
    drop(publish);
    Ok(())
}
async fn wait_pending(path: &std::path::Path) -> Result<()> {
    loop {
        if path.try_exists()? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
