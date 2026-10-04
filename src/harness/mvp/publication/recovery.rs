use super::{PublicationFixture, Query};
use crate::catalog::{Publication, Resolution};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

impl PublicationFixture {
    /// Use dedicated local fault connections, preserving the normal oracle connection.
    pub(super) async fn uncertain_commit(
        &mut self,
        sql: &mut Client,
        graph: &mut Query,
        attempt: Publication<'_>,
    ) -> Result<bool> {
        if attempt.checkpoint.time == 4 {
            return self.held_commit(sql, graph, attempt).await;
        }
        let mode = match attempt.checkpoint.time {
            2 => "BEFORE",
            3 => "AFTER",
            _ => return Ok(false),
        };
        let Ok(port) = std::env::var(format!("PGDERIVE_SQL_COMMIT_{mode}_PORT")) else {
            return Ok(false);
        };
        let mut config = self.config.clone();
        config.postgres.port = port.parse()?;
        let (mut fault, task) = crate::harness::connect(&config).await?;
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            self.grouped.publish(&mut fault, graph.plan(), attempt),
        )
        .await
        .context("commit fault timed out")?;
        ensure!(outcome.is_err(), "COMMIT fault proxy did not disconnect publication");
        eprintln!(
            "expected uncertain {mode} COMMIT: {:#}",
            outcome.err().context("missing fault outcome")?
        );
        drop(fault);
        let _disconnected = task.await.context("fault connection task panicked")?;
        ensure!(
            self.grouped.publish(sql, graph.plan(), attempt).await.is_err(),
            "uncertain writer accepted blind retry"
        );
        ensure!(
            self.grouped.confirmed(sql, graph.plan()).await.is_err(),
            "uncertain COMMIT authorized acknowledgement"
        );
        let resolved = self.grouped.reconcile(sql, graph.plan(), attempt).await?;
        let expected =
            if mode == "BEFORE" { Resolution::NotCommitted } else { Resolution::Committed };
        ensure!(resolved == expected, "COMMIT reconciliation selected wrong outcome");
        if resolved == Resolution::Committed {
            let durable = self
                .catalog
                .load_durable(sql, graph.plan())
                .await?
                .context("missing resolved durable boundary")?;
            ensure!(
                durable.stored.checkpoint == *attempt.checkpoint
                    && durable.covers(attempt.progress)?,
                "resolved candidate/replay mismatch"
            );
            graph.restore_checkpoint(durable.stored.checkpoint).await?;
            ensure!(
                self.grouped.publish(sql, graph.plan(), attempt).await.is_err(),
                "committed replay applied destination twice"
            );
        }
        eprintln!("MVP uncertain {mode} COMMIT resolved as {resolved:?}; blind retry rejected");
        Ok(resolved == Resolution::Committed)
    }
}
