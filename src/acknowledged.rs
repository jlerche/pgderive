//! Registered source stream whose feedback follows verified durable publication.
mod reader;
use crate::replication::Client as ReplicationClient;
use crate::{
    Config,
    catalog::{Durable, Lsn, Writer},
    engine::plan::Plan,
    transaction::Transaction,
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub struct Stream {
    queue: tokio::sync::mpsc::Receiver<reader::Decoded>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    stop: tokio::sync::watch::Sender<bool>,
    feedback: Box<dyn Fn(pgwire_replication::Lsn) + Send + Sync>,
    source: crate::source::Contract,
    acknowledged: Lsn,
    received: Lsn,
    decode_us: u128,
}
impl Stream {
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
        source.identity.validate_resume(config, durable.checkpoint_end).await?;
        let replication = config
            .replication_config()
            .with_start_lsn(durable.checkpoint_end.to_string().parse()?)
            // Wire-frame allocation remains bounded independently of the decoded
            // transaction queue. Neither queue authorizes replication feedback.
            .with_buffer_size(1)
            .with_status_interval(std::time::Duration::from_millis(100))
            .with_wakeup_interval(std::time::Duration::from_millis(100));
        let client = ReplicationClient::connect(replication);
        let feedback = Box::new(client.durable_feedback());
        let bytes = config
            .worker
            .as_ref()
            .map_or(8 * 1024 * 1024, |w| w.epoch.max_bytes)
            .min(config.execution.output_bytes);
        let bytes = u32::try_from(bytes.max(1))?;
        let (send, received) = tokio::sync::mpsc::channel(64);
        let (stop, signal) = tokio::sync::watch::channel(false);
        let reader = reader::Reader::new(client, config, durable);
        let task = tokio::spawn(reader.run(send, signal, bytes));
        Ok(Self {
            feedback,
            queue: received,
            stop,
            task: Some(task),
            source,
            acknowledged: durable.checkpoint_end,
            received: durable.checkpoint_end,
            decode_us: 0,
        })
    }
    pub(crate) async fn recv(&mut self) -> Result<Transaction> {
        if let Some(decoded) = self.queue.recv().await {
            self.decode_us = decoded.decode_us;
            self.received = self.received.max(decoded.transaction.end_lsn.parse()?);
            return Ok(decoded.transaction);
        }
        self.finish().await?;
        anyhow::bail!("durable source reader ended")
    }
    pub(crate) const fn decode_us(&self) -> u128 {
        self.decode_us
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
            durable.checkpoint_end >= self.acknowledged && durable.checkpoint_end <= self.received,
            "acknowledgement outside received durable boundary"
        );
        (self.feedback)(durable.checkpoint_end.to_string().parse()?);
        self.acknowledged = durable.checkpoint_end;
        Ok(())
    }
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        let _closed = self.stop.send(true);
        self.queue.close();
        self.finish().await
    }
    async fn finish(&mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.await.context("source reader task failed")??;
        }
        Ok(())
    }
}
