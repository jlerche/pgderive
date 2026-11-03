//! Checkpoint-local coalescing of data payloads across independent arrangements.
//! The caller drives uploads and this bounded queue together; no task is detached.
use super::{
    bounded,
    format::{Pack, TARGET_OBJECT_BYTES, hash},
};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::{future::Future, sync::Arc};
use tokio::sync::{mpsc, oneshot};

type Address = (Pack, u64);
struct Request {
    bytes: Vec<u8>,
    reply: oneshot::Sender<Result<Address, UploadFailure>>,
}
#[derive(Debug, Clone)]
struct UploadFailure(Arc<anyhow::Error>);
impl std::fmt::Display for UploadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "shared pack upload failed: {}", self.0)
    }
}
impl std::error::Error for UploadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
tokio::task_local! {
    static QUEUE: mpsc::Sender<Request>;
}

pub async fn scope<T>(
    store: Arc<dyn ObjectStore>,
    namespace: Option<&str>,
    capacity: usize,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    let store = super::object::scoped_store(&store, namespace);
    let (result, uploaded) =
        tokio::join!(biased; QUEUE.scope(sender, work), drive(store, receiver));
    uploaded?;
    result
}

/// All data uploads use this address boundary, including standalone writers.
pub(super) async fn put(store: &Arc<dyn ObjectStore>, bytes: Vec<u8>) -> Result<Address> {
    if let Ok(sender) = QUEUE.try_with(Clone::clone) {
        let (reply, result) = oneshot::channel();
        sender.send(Request { bytes, reply }).await.context("pack upload queue closed")?;
        return result.await.context("pack upload response lost")?.map_err(anyhow::Error::new);
    }
    Ok((upload(store, bytes).await?, 0))
}

async fn upload(store: &Arc<dyn ObjectStore>, bytes: Vec<u8>) -> Result<Pack> {
    let digest = hash(&bytes);
    let pack = Pack {
        path: format!("pgderive/pack/{digest}"),
        bytes: u64::try_from(bytes.len())?,
        hash: digest,
    };
    bounded::put(store, &pack.path, bytes).await?;
    Ok(pack)
}
async fn drive(store: Arc<dyn ObjectStore>, mut queue: mpsc::Receiver<Request>) -> Result<()> {
    let mut carry = None;
    loop {
        let first = if let Some(request) = carry.take() {
            request
        } else {
            let Some(request) = queue.recv().await else { return Ok(()) };
            request
        };
        let mut bytes = first.bytes;
        let mut replies = vec![(first.reply, 0)];
        // The work future is polled before the driver by join!, so every admitted
        // writer can enqueue its next bounded payload before this queue is drained.
        while let Ok(request) = queue.try_recv() {
            if bytes.len().saturating_add(request.bytes.len()) > TARGET_OBJECT_BYTES {
                carry = Some(request);
                break;
            }
            let offset = u64::try_from(bytes.len())?;
            bytes.extend_from_slice(&request.bytes);
            replies.push((request.reply, offset));
        }
        let result = upload(&store, bytes).await.map_err(|error| UploadFailure(Arc::new(error)));
        for (reply, offset) in replies {
            let response = result.as_ref().map(|pack| (pack.clone(), offset)).map_err(Clone::clone);
            let _closed = reply.send(response);
        }
        // Keep draining after failures so all sibling futures terminate, retaining
        // the originating upload protection. Never publish a partial checkpoint.
    }
}
