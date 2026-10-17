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
    source: crate::source::Contract,
    acknowledged: Lsn,
    received: Lsn,
    change_limit: usize,
    durable: Durable,
    measured: bool,
    decode_us: u128,
}
impl Stream {
    /// Caller cold-restores the compatible engine before connecting.
    pub(crate) async fn connect(config: &Config, durable: &Durable) -> Result<Self> {
        config.validate()?;
        let source = durable.binding.source.clone();
        source.validate()?;
        ensure!(
            source.publication == config.replication.publication
                && source.slot == config.replication.slot
                && source.identity.database == config.postgres.database,
            "registered source identity changed"
        );
        source.identity.validate_resume(config, durable.end).await?;
        let replication = config
            .replication_config()
            .with_start_lsn(durable.end.to_string().parse()?)
            // One queued event bounds read-ahead while object publication is slow.
            // Decoder byte budgets apply after upstream wire-frame allocation.
            .with_buffer_size(1)
            .with_status_interval(std::time::Duration::from_millis(100))
            .with_wakeup_interval(std::time::Duration::from_millis(100));
        let client = ReplicationClient::connect(replication)
            .await
            .context("connecting registered durable source")?;
        Ok(Self {
            client,
            decoder: Decoder::registered(config.execution, source.clone()),
            source,
            acknowledged: durable.end,
            received: durable.end,
            change_limit: config.listener.max_transaction_changes,
            durable: durable.clone(),
            measured: config.worker.as_ref().is_some_and(|worker| worker.phase_metrics),
            decode_us: 0,
        })
    }
    pub(crate) async fn recv(&mut self) -> Result<Transaction> {
        loop {
            let event = self.client.recv().await?.context("durable source stream ended")?;
            if matches!(event, ReplicationEvent::Begin { .. }) {
                self.decode_us = 0;
            }
            let mut clock = crate::timing::Clock::new(self.measured);
            let result = self.event(event)?;
            self.decode_us = self.decode_us.saturating_add(clock.lap());
            if let Some(transaction) = result {
                return Ok(transaction);
            }
        }
    }
    pub(crate) const fn decode_us(&self) -> u128 {
        self.decode_us
    }
    fn event(&mut self, event: ReplicationEvent) -> Result<Option<Transaction>> {
        match event {
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
                if !self.durable.covers(&progress)? {
                    return Ok(Some(transaction));
                }
            }
            ReplicationEvent::KeepAlive { .. } => {}
            other => bail!("unsupported durable replication event: {other:?}"),
        }
        Ok(None)
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
