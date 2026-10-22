//! Bounded committed-transaction read-ahead; feedback belongs to the consumer.
use crate::{
    Config,
    catalog::{Durable, Lsn},
    transaction::{Decoder, Transaction},
};
use anyhow::{Context, Result, bail};
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use postgres_replication::protocol::LogicalReplicationMessage;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, watch};

pub(super) struct Decoded {
    pub(super) transaction: Transaction,
    pub(super) decode_us: u128,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
pub(super) struct Reader {
    client: ReplicationClient,
    decoder: Decoder,
    limit: usize,
    checkpoint: Lsn,
    measured: bool,
    decode_us: u128,
}
impl Reader {
    pub(super) fn new(client: ReplicationClient, config: &Config, durable: &Durable) -> Self {
        Self {
            client,
            decoder: Decoder::registered(config.execution, durable.binding.source.clone()),
            limit: config.listener.max_transaction_changes,
            checkpoint: durable.checkpoint_end,
            measured: config.worker.as_ref().is_some_and(|w| w.phase_metrics),
            decode_us: 0,
        }
    }
    pub(super) async fn run(
        mut self,
        send: mpsc::Sender<Decoded>,
        mut stop: watch::Receiver<bool>,
        bytes: u32,
    ) -> Result<()> {
        let budget = Arc::new(Semaphore::new(usize::try_from(bytes)?));
        let result = async {
            loop {
                tokio::select! {
                    biased;
                    signal=stop.changed()=>{signal.context("source reader stop monitor closed")?;break;}
                    result=self.send_next(&send,&budget,bytes)=>result?,
                }
            }
            Ok(())
        }.await;
        let shutdown = self.client.shutdown().await.context("source reader replication shutdown");
        crate::outcome::combine(result, shutdown, "source reader shutdown")
    }
    async fn send_next(
        &mut self,
        send: &mpsc::Sender<Decoded>,
        budget: &Arc<Semaphore>,
        bytes: u32,
    ) -> Result<()> {
        let transaction = self.recv().await?;
        let charge = u32::try_from(transaction.bytes.min(u64::from(bytes)).max(1))?;
        let permit = Arc::clone(budget)
            .acquire_many_owned(charge)
            .await
            .context("source read-ahead budget closed")?;
        send.send(Decoded { transaction, decode_us: self.decode_us, _permit: permit })
            .await
            .context("source reader consumer closed")
    }
    async fn recv(&mut self) -> Result<Transaction> {
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
    fn event(&mut self, event: ReplicationEvent) -> Result<Option<Transaction>> {
        match event {
            ReplicationEvent::Begin { xid, .. } => self.decoder.begin(xid)?,
            ReplicationEvent::XLogData { data, .. } => {
                self.decoder.message(&LogicalReplicationMessage::parse(&data)?, self.limit)?;
            }
            ReplicationEvent::Commit { lsn, end_lsn, commit_time_micros } => {
                let mut transaction = self.decoder.commit(lsn.to_string(), end_lsn.to_string())?;
                transaction.commit_time_micros = Some(commit_time_micros);
                if transaction.end_lsn.parse::<Lsn>()? > self.checkpoint {
                    return Ok(Some(transaction));
                }
            }
            ReplicationEvent::KeepAlive { .. } => {}
            other => bail!("unsupported durable replication event: {other:?}"),
        }
        Ok(None)
    }
}
