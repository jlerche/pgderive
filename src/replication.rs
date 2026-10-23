//! Upstream protocol primitives with Pgderive's all-phase frame and durable-feedback bounds.
mod handshake;
mod messages;
#[cfg(test)]
mod tests;
use anyhow::{Context, Result};
use pgwire_replication::protocol::framing::{MessageReader, write_copy_data, write_copy_done};
use pgwire_replication::{Lsn, ReplicationConfig, ReplicationEvent};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader},
    sync::{mpsc, watch},
};

pub const MAX_PAYLOAD: usize = 1024 * 1024;
trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
type Socket = Box<dyn Transport>;

pub struct Client {
    events: mpsc::Receiver<ReplicationEvent>,
    progress: Arc<AtomicU64>,
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}
impl Client {
    pub(crate) fn connect(config: ReplicationConfig) -> Self {
        let (send, events) = mpsc::channel(config.buffer_events);
        let (stop, signal) = watch::channel(false);
        let progress = Arc::new(AtomicU64::new(config.start_lsn.as_u64()));
        let worker = Worker { config, send, progress: Arc::clone(&progress), stop: signal };
        let task = tokio::spawn(worker.run());
        Self { events, progress, stop, task: Some(task) }
    }
    pub(crate) fn durable_feedback(&self) -> impl Fn(Lsn) + Send + Sync + 'static {
        let progress = Arc::clone(&self.progress);
        move |lsn| {
            progress.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        }
    }
    pub(crate) async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        if let Some(event) = self.events.recv().await {
            return Ok(Some(event));
        }
        self.finish().await?;
        Ok(None)
    }
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.stop.send_replace(true);
        self.events.close();
        self.finish().await
    }
    async fn finish(&mut self) -> Result<()> {
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task.take();
            result.context("replication worker failed")??;
        }
        Ok(())
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Worker {
    config: ReplicationConfig,
    send: mpsc::Sender<ReplicationEvent>,
    progress: Arc<AtomicU64>,
    stop: watch::Receiver<bool>,
}
impl Worker {
    async fn run(mut self) -> Result<()> {
        let socket = tokio::select! {
            _ = self.stop.changed() => return Ok(()),
            socket = handshake::connect(&self.config) => socket?,
        };
        let mut stream = BufReader::with_capacity(128 * 1024, socket);
        // The upstream client currently configures only its streaming reader;
        // use the same bounded, cancellation-safe reader for authentication too.
        let mut reader = MessageReader::new().with_max_message_size(MAX_PAYLOAD);
        tokio::select! {
            _ = self.stop.changed() => return Ok(()),
            result = handshake::start(&mut stream, &mut reader, &self.config) => result?,
        }
        self.stream(&mut stream, &mut reader).await
    }
    async fn stream(
        &mut self,
        stream: &mut BufReader<Socket>,
        reader: &mut MessageReader,
    ) -> Result<()> {
        let period = self.config.status_interval.min(self.config.idle_wakeup_interval);
        let mut status = tokio::time::interval(period);
        status.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pending = None;
        loop {
            tokio::select! {
                biased;
                _ = self.stop.changed() => {
                    // Match upstream: failure to send CopyDone cannot authorize feedback.
                    let _closed = write_copy_done(stream).await;
                    return Ok(());
                }
                _ = status.tick() => self.feedback(stream, false).await?,
                permit = self.send.reserve(), if pending.is_some() => {
                    permit.context("replication consumer closed")?.send(pending.take().context("missing replication event")?);
                }
                message = reader.read(stream), if pending.is_none() => {
                    let message = message?;
                    messages::check_error(&message)?;
                    if message.tag == b'd' {
                        let (event, reply) = messages::event(message)?;
                        if reply { self.feedback(stream, true).await?; }
                        pending = Some(event);
                    }
                }
            }
        }
    }
    async fn feedback(&self, stream: &mut BufReader<Socket>, reply: bool) -> Result<()> {
        use pgwire_replication::protocol::replication::{
            PG_EPOCH_MICROS, encode_standby_status_update,
        };
        let micros = i64::try_from(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_micros(),
        )?;
        let position = Lsn::from_u64(self.progress.load(Ordering::Acquire));
        let payload = encode_standby_status_update(position, micros - PG_EPOCH_MICROS, reply);
        write_copy_data(stream, &payload).await?;
        Ok(())
    }
}
