//! One bounded uploader per compatible tenant domain in this worker runtime.
use super::{
    Config,
    batch::Group,
    manifest::{Catalog, Manifest},
    ownership,
    store::Store,
};
use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{OnceCell, Semaphore};

type Cell = Arc<OnceCell<Arc<Group>>>;
#[derive(Default)]
struct Groups {
    cells: BTreeMap<String, Cell>,
    closed: bool,
    pools: BTreeMap<String, Arc<super::pool::Pool>>,
    retired: Vec<Arc<Group>>,
}

#[cfg(test)]
pub(super) async fn verify_recovery_retention(
    manifest: Arc<Manifest>,
    catalog: Catalog,
) -> Result<()> {
    let registry = Registry::default();
    let group = Group::start(
        manifest,
        catalog.tenant.clone(),
        Arc::new(crate::engine::tests::store::Store::new(Some(0))),
        registry.bytes.clone(),
        registry.maintenance.clone(),
    )
    .await?;
    anyhow::ensure!(
        group
            .put(
                catalog,
                "pgderive/upload-v1/reap-failure/pgderive/pack/a".into(),
                bytes::Bytes::from_static(b"failed uploader").into()
            )
            .await
            .is_err(),
        "physical failure was not delivered"
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !group.finished().await {
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "failed uploader did not finish");
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let weak = Arc::downgrade(&group);
    registry
        .groups
        .lock()
        .map_err(|_| anyhow::anyhow!("test registry poisoned"))?
        .retired
        .push(group.clone());
    drop(group);
    registry.reap().await?;
    anyhow::ensure!(
        weak.upgrade().is_none(),
        "completed uploader resources survived recovery reap"
    );
    registry.close().await
}
pub struct Registry {
    groups: Mutex<Groups>,
    bytes: Arc<Semaphore>,
    maintenance: Arc<Semaphore>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            groups: Mutex::new(Groups::default()),
            bytes: Arc::new(Semaphore::new(64 * 1024 * 1024)),
            maintenance: Arc::new(Semaphore::new(1)),
        }
    }
}
impl Registry {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the initialized group is moved into the returned virtual store after catalog registration"
    )]
    pub(crate) async fn store(
        &self,
        config: &crate::Config,
        settings: &crate::worker::Settings,
        identity: &Value,
        id: String,
    ) -> Result<Arc<dyn crate::storage::PageStore>> {
        self.reap().await?;
        let control =
            config.tenant_storage.as_ref().context("worker requires tenant storage metadata")?;
        let storage = config.object_store.as_ref().context("worker requires object storage")?;
        // This secret-bearing serialization is hashed in memory only and never
        // persisted or emitted. Credential identity, rather than its file path, selects a compatible uploader.
        let key = super::checksum(&serde_json::to_vec(&(
            control,
            storage.sharing_key()?,
            storage.identity()?,
            &settings.tenant_id,
            config.execution.object_requests,
        ))?);
        let cell = self.cell(&key)?;
        let group = cell.get_or_try_init(|| self.open(config, settings, control)).await?.clone();
        drop(cell);
        let catalog = Catalog { tenant: settings.tenant_id.clone(), catalog: id };
        group.manifest.catalog(&catalog, identity, &settings.object_prefix).await?;
        Ok(Arc::new(Store { group, catalog }))
    }
    fn cell(&self, key: &str) -> Result<Cell> {
        let mut groups =
            self.groups.lock().map_err(|_| anyhow::anyhow!("tenant registry poisoned"))?;
        anyhow::ensure!(!groups.closed, "tenant registry closed");
        if groups.cells.get(key).and_then(|c| c.get()).is_some_and(|g| !g.open())
            && let Some(cell) = groups.cells.remove(key)
            && let Some(group) = cell.get()
        {
            groups.retired.push(group.clone());
        }
        Ok(groups.cells.entry(key.into()).or_default().clone())
    }
    async fn reap(&self) -> Result<()> {
        let retired = self
            .groups
            .lock()
            .map_err(|_| anyhow::anyhow!("tenant registry poisoned"))?
            .retired
            .clone();
        for group in retired {
            if !group.finished().await {
                continue;
            }
            let result = group.close().await;
            // Remove only after the join completed. Cancellation before here
            // leaves the registered handle available to the next reap/close.
            self.groups
                .lock()
                .map_err(|_| anyhow::anyhow!("tenant registry poisoned"))?
                .retired
                .retain(|entry| !Arc::ptr_eq(entry, &group));
            result?;
        }
        Ok(())
    }
    async fn open(
        &self,
        config: &crate::Config,
        settings: &crate::worker::Settings,
        control: &Config,
    ) -> Result<Arc<Group>> {
        let storage = config.object_store.as_ref().context("missing object storage")?;
        let pool_key = super::checksum(&serde_json::to_vec(&control.postgres)?);
        let pool = {
            let mut groups =
                self.groups.lock().map_err(|_| anyhow::anyhow!("tenant registry poisoned"))?;
            groups
                .pools
                .entry(pool_key)
                .or_insert_with(|| super::pool::Pool::new(control.postgres.clone()))
                .clone()
        };
        let manifest = Manifest::connect(control.clone(), pool).await?;
        let identity =
            serde_json::json!({"provider":storage.identity()?,"prefix":control.object_prefix});
        let id = manifest.tenant(&settings.tenant_id, &identity).await?;
        ownership::claim(
            storage,
            &manifest,
            &settings.tenant_id,
            &id,
            config.execution.object_requests,
        )
        .await?;
        let store = storage
            .build(&format!("{}/{id}", control.object_prefix), config.execution.object_requests)?;
        storage.preflight(store.as_ref()).await?;
        Group::start(
            manifest,
            settings.tenant_id.clone(),
            store,
            self.bytes.clone(),
            self.maintenance.clone(),
        )
        .await
    }
    pub(crate) async fn collect(&self, config: &crate::Config) -> Result<Value> {
        let settings =
            config.worker.as_ref().context("tenant collector requires tenant identity")?;
        let control = config
            .tenant_storage
            .as_ref()
            .context("tenant collector requires metadata endpoint")?;
        let group = self.open(config, settings, control).await?;
        let result = group.collect().await;
        crate::outcome::combine(result, group.close().await, "tenant collector shutdown")
    }
    pub(crate) async fn close(&self) -> Result<()> {
        let (cells, retired) = {
            let mut groups =
                self.groups.lock().map_err(|_| anyhow::anyhow!("tenant registry poisoned"))?;
            groups.closed = true;
            // Keep drain handles registered until completion. Cancellation of
            // this close must not make a retry report an already-empty runtime.
            let result = (groups.cells.clone(), groups.retired.clone());
            drop(groups);
            result
        };
        let mut error = None;
        for group in retired {
            if let Err(failure) = group.close().await {
                error.get_or_insert(failure);
            }
        }
        for cell in cells.into_values() {
            if let Some(group) = cell.get()
                && let Err(failure) = group.close().await
            {
                error.get_or_insert(failure);
            }
        }
        error.map_or(Ok(()), Err)
    }
}
