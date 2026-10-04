//! Registered source stream whose feedback follows verified durable publication.
use crate::{
    Config,
    catalog::{Durable, Lsn, Writer},
    engine::plan::Plan,
    transaction::{Decoder, Transaction},
};
use anyhow::{Context, Result, bail, ensure};
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use postgres_replication::protocol::LogicalReplicationMessage;
use tokio_postgres::Client;

pub struct Stream {
    client: ReplicationClient,
    decoder: Decoder,
    source: String,
    acknowledged: Lsn,
    received: Lsn,
    change_limit: usize,
    durable: Durable,
}
impl Stream {
    /// Caller cold-restores the compatible engine before connecting.
    pub(crate) async fn connect(config: &Config, durable: &Durable) -> Result<Self> {
        config.validate()?;
        let source = format!(
            "{}:{}:{}",
            config.postgres.database, config.replication.publication, config.replication.slot
        );
        ensure!(
            durable.binding.source == source,
            "replication source does not match durable registration"
        );
        let replication = config
            .replication_config()
            .with_start_lsn(durable.end.to_string().parse()?)
            .with_status_interval(std::time::Duration::from_millis(100))
            .with_wakeup_interval(std::time::Duration::from_millis(100));
        let client = ReplicationClient::connect(replication)
            .await
            .context("connecting registered durable source")?;
        Ok(Self {
            client,
            decoder: Decoder::new(config.execution),
            source,
            acknowledged: durable.end,
            received: durable.end,
            change_limit: config.listener.max_transaction_changes,
            durable: durable.clone(),
        })
    }
    pub(crate) async fn recv(&mut self) -> Result<Transaction> {
        loop {
            match self.client.recv().await?.context("durable source stream ended")? {
                ReplicationEvent::Begin { xid, .. } => self.decoder.begin(xid)?,
                ReplicationEvent::XLogData { data, .. } => self
                    .decoder
                    .message(&LogicalReplicationMessage::parse(&data)?, self.change_limit)?,
                ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                    let transaction = self.decoder.commit(lsn.to_string(), end_lsn.to_string())?;
                    self.received = self.received.max(transaction.end_lsn.parse()?);
                    let progress = crate::catalog::Progress::new(
                        transaction.xid,
                        &transaction.commit_lsn,
                        &transaction.end_lsn,
                    )?;
                    if self.durable.covers(&progress)? {
                        continue;
                    }

                    return Ok(transaction);
                }
                ReplicationEvent::KeepAlive { .. } => {}
                other => bail!("unsupported durable replication event: {other:?}"),
            }
        }
    }
    pub(crate) async fn acknowledge(
        &mut self,
        sql: &mut Client,
        writer: &Writer,
        plan: &Plan,
    ) -> Result<()> {
        let durable = writer.confirmed(sql, plan).await?;
        ensure!(durable.binding.source == self.source, "acknowledgement source mismatch");
        ensure!(
            durable.end >= self.acknowledged && durable.end <= self.received,
            "acknowledgement outside received durable boundary"
        );
        self.client.update_applied_lsn(durable.end.to_string().parse()?);
        self.acknowledged = durable.end;
        self.durable = durable;
        Ok(())
    }
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.client.shutdown().await.context("shutting down durable replication stream")
    }
}
