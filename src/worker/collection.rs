//! One bounded physical collector, independent of transaction publication.
use super::sql::Session;
use crate::{
    Config,
    catalog::{Catalog, Collection, GcCache, GcLimits},
};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Duration, timeout},
};

#[derive(Default)]
pub(super) struct Collector {
    session: Option<Arc<Mutex<CollectionSession>>>,
    task: Option<JoinHandle<Result<Collection>>>,
}
struct CollectionSession {
    sql: Session,
    roots: GcCache,
}
impl Collector {
    pub(super) async fn connect(config: &Config) -> Result<Self> {
        Ok(Self {
            session: Some(Arc::new(Mutex::new(CollectionSession {
                sql: Session::connect(config).await?,
                roots: GcCache::default(),
            }))),
            task: None,
        })
    }

    pub(super) fn start(
        &mut self,
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        seconds: u64,
    ) -> Result<()> {
        if self.task.is_some() {
            return Ok(());
        }
        let session = self.session.as_ref().context("collector has no SQL session")?.clone();
        self.task = Some(tokio::spawn(async move {
            timeout(Duration::from_secs(seconds), async move {
                let mut session = session.lock().await;
                let CollectionSession { sql, roots } = &mut *session;
                let result =
                    catalog.collect(&mut sql.client, store, GcLimits::default(), roots).await;
                drop(session);
                result
            })
            .await
            .context("worker background collection timed out")?
        }));
        Ok(())
    }
    // Await by mutable reference: cancelling a select branch keeps ownership of
    // the running task, so it cannot become an untracked detached collector.
    pub(super) async fn completed(&mut self) -> Result<()> {
        let Some(task) = self.task.as_mut() else {
            return std::future::pending().await;
        };
        let result = task.await;
        self.task = None;
        let collection = result.context("worker background collection task failed")??;
        super::drive::emit(
            &serde_json::json!({"event":"collection","collection":format!("{collection:?}")}),
        )
    }
    pub(super) async fn drain(&mut self) -> Result<()> {
        if self.task.is_some() {
            self.completed().await?;
        }
        Ok(())
    }
}
impl Drop for Collector {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
