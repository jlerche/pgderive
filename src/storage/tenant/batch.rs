//! Cancellation-independent tenant upload ownership with bounded byte admission.
use super::{
    Config,
    manifest::{Catalog, Manifest},
    upload::{Chunk, Installed, Owner},
};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, PutMode, PutOptions, PutResult, path::Path};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

pub(super) struct Group {
    pub(super) manifest: Arc<Manifest>,
    pub(super) store: Arc<dyn super::super::PageStore>,
    sender: Mutex<Option<mpsc::Sender<Request>>>,
    task: tokio::sync::Mutex<Option<JoinHandle<Result<()>>>>,
    bytes: Arc<Semaphore>,
    global: Arc<Semaphore>,
    budget: usize,
    pub(super) tenant: String,
    config: Config,
    failures: Arc<AtomicU64>,
    maintenance: Arc<Semaphore>,
}
struct Request {
    chunk: Chunk,
    tenant_permit: OwnedSemaphorePermit,
    global_permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<Installed, Failure>>,
}
#[derive(Clone, Debug)]
struct Failure(Arc<anyhow::Error>);
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tenant upload failed: {}", self.0)
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
impl std::fmt::Debug for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TenantUploadGroup")
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        // close() drains normally. If the entire runtime is canceled instead,
        // do not detach its uploader; durable intents cover abandoned I/O.
        if let Some(task) = self.task.get_mut().take() {
            task.abort();
        }
    }
}
impl Group {
    pub(super) async fn start(
        manifest: Arc<Manifest>,
        tenant: String,
        store: Arc<dyn super::super::PageStore>,
        global: Arc<Semaphore>,
        maintenance: Arc<Semaphore>,
    ) -> Result<Arc<Self>> {
        let owner = Owner::connect(manifest.clone(), &tenant).await?;
        let config = manifest.config.clone();
        let (sender, receiver) = mpsc::channel(128);
        let failures = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(drive(
            owner,
            tenant.clone(),
            store.clone(),
            receiver,
            (failures.clone(), maintenance.clone()),
        ));
        Ok(Arc::new(Self {
            manifest,
            store,
            sender: Mutex::new(Some(sender)),
            task: tokio::sync::Mutex::new(Some(task)),
            bytes: Arc::new(Semaphore::new(config.batching.buffer_bytes)),
            global,
            budget: config.batching.buffer_bytes,
            tenant,
            config,
            failures,
            maintenance,
        }))
    }
    pub(super) fn open(&self) -> bool {
        self.sender.lock().is_ok_and(|guard| guard.as_ref().is_some_and(|s| !s.is_closed()))
    }
    pub(super) async fn finished(&self) -> bool {
        self.task.lock().await.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub(super) async fn put(
        &self,
        catalog: Catalog,
        name: String,
        payload: object_store::PutPayload,
    ) -> Result<PutResult> {
        ensure!(catalog.tenant == self.tenant, "cross-tenant upload rejected");
        let length = payload.content_length();
        ensure!(
            length > 0 && length <= 8 * 1024 * 1024 + 65_536,
            "logical object exceeds bounded upload size"
        );
        let charge = length
            .checked_mul(2)
            .and_then(|b| b.checked_add(4096))
            .context("upload charge overflow")?;
        ensure!(charge <= self.budget, "logical object exceeds tenant upload budget");
        let tenant = self.bytes.clone().acquire_many_owned(u32::try_from(charge)?).await?;
        let global = self.global.clone().acquire_many_owned(u32::try_from(charge)?).await?;
        let mut bytes = bytes::BytesMut::with_capacity(length);
        for part in payload {
            bytes.extend_from_slice(&part);
        }
        let bytes = bytes.freeze();
        let checksum = super::checksum(&bytes);
        let (reply, received) = oneshot::channel();
        let sender = self
            .sender
            .lock()
            .map_err(|_| anyhow::anyhow!("tenant uploader poisoned"))?
            .clone()
            .context("tenant uploader closed")?;
        sender
            .send(Request {
                chunk: Chunk { catalog, name: name.clone(), bytes, checksum: checksum.clone() },
                tenant_permit: tenant,
                global_permit: global,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("tenant uploader stopped"))?;
        match received
            .await
            .context("tenant upload outcome unavailable; retain logical publication")?
            .map_err(anyhow::Error::new)?
        {
            Installed::Created => Ok(PutResult {
                e_tag: Some(checksum),
                version: None,
                extensions: object_store::Extensions::default(),
            }),
            Installed::Exists => Err(object_store::Error::AlreadyExists {
                path: name,
                source: "immutable logical object already exists".into(),
            }
            .into()),
            Installed::Sealed => {
                Err(anyhow::anyhow!("logical upload namespace sealed; stale PUT rejected"))
            }
        }
    }
    pub(super) async fn close(&self) -> Result<()> {
        self.sender.lock().map_err(|_| anyhow::anyhow!("tenant uploader poisoned"))?.take();
        let mut state = self.task.lock().await;
        if let Some(task) = state.as_mut() {
            // Borrow the handle across the await: cancellation releases this
            // mutex but leaves the task available to a subsequent close.
            let result = task.await.context("tenant uploader task failed");
            state.take();
            drop(state);
            match result? {
                // Upload failures were delivered to every accepted waiter and
                // handled by query recovery. A drained retired group must not
                // report that same failure again after a successful retry.
                Err(error) if error.is::<Failure>() => {}
                other => other?,
            }
        }
        Ok(())
    }
    pub(super) async fn collect(&self) -> Result<serde_json::Value> {
        let _permit = self.maintenance.clone().acquire_owned().await?;
        let mut owner = Owner::connect(self.manifest.clone(), &self.tenant).await?;
        let workspaces = Arc::new(crate::engine::execution::Workspaces::new(2)?);
        let (result, _service) = crate::engine::execution::service::scope(
            workspaces,
            owner.collect(&self.tenant, &self.store),
        )
        .await;
        let progressed =
            crate::outcome::combine(result, owner.close().await, "tenant collector owner close")?;
        let mut stats = self.stats();
        stats["collection_progress"] = serde_json::json!(progressed);
        Ok(stats)
    }
    pub(super) fn stats(&self) -> serde_json::Value {
        serde_json::json!({"buffer_limit":self.budget,"buffer_used":self.budget-self.bytes.available_permits(),"batch_target":self.config.batching.target_bytes,"max_delay_ms":self.config.batching.delay_ms,"gc_failures":self.failures.load(Ordering::Relaxed)})
    }
}
async fn drive(
    owner: Owner,
    tenant: String,
    store: Arc<dyn super::super::PageStore>,
    queue: mpsc::Receiver<Request>,
    maintenance: (Arc<AtomicU64>, Arc<Semaphore>),
) -> Result<()> {
    let background = maintain(owner.manifest.clone(), &tenant, &store, maintenance);
    let uploading = drive_upload(owner, &tenant, &store, queue);
    // Both futures are owned by this supervised task. When uploads drain,
    // cancellation drops the maintenance SQL owner and fences unfinished work.
    tokio::select! {
        result = uploading => result,
        result = background => result,
    }
}
async fn maintain(
    manifest: Arc<Manifest>,
    tenant: &str,
    store: &Arc<dyn super::super::PageStore>,
    maintenance: (Arc<AtomicU64>, Arc<Semaphore>),
) -> Result<()> {
    let workspaces = Arc::new(crate::engine::execution::Workspaces::new(2)?);
    let mut collector = None;
    let mut inventory = (None, tokio::time::Instant::now());
    let mut delay = std::time::Duration::from_secs(30);
    loop {
        tokio::time::sleep(delay).await;
        let permit = maintenance.1.clone().acquire_owned().await?;
        let (result, _service) = crate::engine::execution::service::scope(
            workspaces.clone(),
            collect_turn(manifest.clone(), tenant, store, &mut collector, &mut inventory),
        )
        .await;
        drop(permit);
        delay = match result {
            Ok(true) => std::time::Duration::from_millis(100),
            Ok(false) => std::time::Duration::from_secs(30),
            Err(_) => {
                maintenance.0.fetch_add(1, Ordering::Relaxed);
                std::time::Duration::from_secs(1)
            }
        };
    }
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "carry and selected requests transfer byte permits to the next upload; they cannot be dropped before that transfer"
)]
async fn drive_upload(
    mut owner: Owner,
    tenant: &str,
    store: &Arc<dyn super::super::PageStore>,
    mut queue: mpsc::Receiver<Request>,
) -> Result<()> {
    let workspaces = Arc::new(crate::engine::execution::Workspaces::new(2)?);
    let mut carry = None;
    loop {
        let first =
            if let Some(request) = carry.take() { Some(request) } else { queue.recv().await };
        let Some(first) = first else { break };
        let (requests, next) = gather(first, &mut queue, &owner.manifest.config).await;
        carry = next;
        let (result, _service) = crate::engine::execution::service::scope(
            workspaces.clone(),
            upload(&mut owner, tenant, store, requests),
        )
        .await;
        if let Err(error) = result {
            queue.close();
            let failure = Failure(Arc::new(error));
            if let Some(request) = carry.take() {
                let _closed = request.reply.send(Err(failure.clone()));
            }
            while let Ok(request) = queue.try_recv() {
                let _closed = request.reply.send(Err(failure.clone()));
            }
            return Err(anyhow::Error::new(failure));
        }
    }
    // Accepted requests are fully drained before closing the durable owner.
    owner.close().await
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "selected requests move their admission permits into the returned batch, rather than retaining an idle guard"
)]
async fn gather(
    first: Request,
    queue: &mut mpsc::Receiver<Request>,
    config: &Config,
) -> (Vec<Request>, Option<Request>) {
    let mut size = first.chunk.bytes.len();
    let mut requests = vec![first];
    let deadline = tokio::time::Instant::now() + config.delay();
    while size < config.batching.target_bytes {
        let request = tokio::select! {
            request=queue.recv()=>request,
            ()=tokio::time::sleep_until(deadline)=>None,
        };
        let Some(request) = request else { break };
        if size.saturating_add(request.chunk.bytes.len()) > config.batching.target_bytes {
            return (requests, Some(request));
        }
        size += request.chunk.bytes.len();
        requests.push(request);
    }
    (requests, None)
}
async fn collect_turn(
    manifest: Arc<Manifest>,
    tenant: &str,
    store: &Arc<dyn super::super::PageStore>,
    collector: &mut Option<Owner>,
    inventory: &mut (Option<String>, tokio::time::Instant),
) -> Result<bool> {
    if collector.is_none() {
        let mut owner = Owner::connect(manifest, tenant).await?;
        owner.inventory_cursor.clone_from(&inventory.0);
        owner.inventory_due = inventory.1;
        *collector = Some(owner);
    }
    // Reuse a successful maintenance session, rather than growing owner
    // tombstones every tick. Any failed turn closes its dedicated owner so its
    // unfinished intents are reclaimable without stopping foreground uploads.
    let result =
        collector.as_mut().context("maintenance owner missing")?.collect(tenant, store).await;
    let owner = collector.as_ref().context("maintenance inventory owner missing")?;
    inventory.0.clone_from(&owner.inventory_cursor);
    inventory.1 = owner.inventory_due;
    if result.is_err() {
        let owner = collector.take().context("failed maintenance owner missing")?;
        return crate::outcome::combine(
            result,
            owner.close().await,
            "failed collector owner close",
        );
    }
    result
}

async fn upload(
    owner: &mut Owner,
    tenant: &str,
    store: &Arc<dyn super::super::PageStore>,
    requests: Vec<Request>,
) -> Result<()> {
    let total = requests.iter().try_fold(0_usize, |bytes, r| {
        bytes.checked_add(r.chunk.bytes.len()).context("batch byte sum overflow")
    })?;
    let mut payload = Vec::with_capacity(total);
    let mut replies = Vec::with_capacity(requests.len());
    let mut chunks = Vec::with_capacity(requests.len());
    for request in requests {
        payload.extend_from_slice(&request.chunk.bytes);
        replies.push((request.reply, request.tenant_permit, request.global_permit));
        chunks.push(request.chunk);
    }
    let result = perform(owner, tenant, store, payload, &chunks).await;
    let outcomes = match result {
        Ok(outcomes) => outcomes,
        Err(error) => {
            let failure = Failure(Arc::new(error));
            for (reply, _tenant, _global) in replies {
                let _closed = reply.send(Err(failure.clone()));
            }
            return Err(anyhow::Error::new(failure));
        }
    };
    ensure!(outcomes.len() == replies.len(), "tenant upload result count mismatch");
    for ((reply, _tenant, _global), outcome) in replies.into_iter().zip(outcomes) {
        let _closed = reply.send(Ok(outcome));
    }
    Ok(())
}

async fn perform(
    owner: &mut Owner,
    tenant: &str,
    store: &Arc<dyn super::super::PageStore>,
    payload: Vec<u8>,
    chunks: &[Chunk],
) -> Result<Vec<Installed>> {
    let pack = owner.reserve(tenant, &payload).await?;
    store
        .put_opts(&Path::from(pack.path.clone()), payload.into(), PutOptions::from(PutMode::Create))
        .await?;
    owner.install(&pack, chunks).await
}

#[cfg(test)]
mod tests;
