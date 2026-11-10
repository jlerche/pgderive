//! Bounded reusable control-plane sessions, independent of source connections.
use crate::configuration::Postgres;
use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Pool {
    config: Postgres,
    idle: Mutex<Vec<crate::postgres::Session>>,
    permits: Arc<Semaphore>,
}
pub(super) struct Session {
    inner: Option<crate::postgres::Session>,
    pool: Arc<Pool>,
    _permit: OwnedSemaphorePermit,
}
impl Pool {
    pub(super) fn new(config: Postgres) -> Arc<Self> {
        Arc::new(Self {
            config,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(8)),
        })
    }
    pub(super) async fn acquire(self: &Arc<Self>) -> Result<Session> {
        let permit = self.permits.clone().acquire_owned().await?;
        let prior =
            self.idle.lock().map_err(|_| anyhow::anyhow!("manifest session pool poisoned"))?.pop();
        let inner = match prior {
            Some(session) if !session.client.is_closed() => session,
            _ => crate::postgres::Session::connect(&self.config).await?,
        };
        Ok(Session { inner: Some(inner), pool: self.clone(), _permit: permit })
    }
}
impl Session {
    pub(super) fn connection(&mut self) -> Result<&mut crate::catalog::Connection> {
        Ok(&mut self.inner.as_mut().context("manifest session absent")?.client)
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if let Some(session) = self.inner.take()
            && !session.client.is_closed()
            && let Ok(mut idle) = self.pool.idle.lock()
        {
            idle.push(session);
        }
    }
}
