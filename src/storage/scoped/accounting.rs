//! Capture classification when an operation starts, including streamed consumption.
use crate::engine::execution::{
    WorkspaceStats, Workspaces,
    service::{self, Ledger, Report},
};
use std::sync::Arc;
#[derive(Debug, Default)]
pub(super) struct Accounting {
    foreground: Arc<Ledger>,
    maintenance: Arc<Ledger>,
}
/// Classified physical API traffic and the shared request admission budget.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceStats {
    pub(crate) foreground: Report,
    pub(crate) maintenance: Report,
    pub(crate) admission: WorkspaceStats,
}
impl Accounting {
    pub(super) fn request(&self) -> Request {
        let job = service::current();
        let global = if job.is_some() { &self.maintenance } else { &self.foreground };
        Request { global: global.clone(), job }
    }
    pub(super) fn stats(&self, pool: &Workspaces) -> ServiceStats {
        ServiceStats {
            foreground: self.foreground.snapshot(),
            maintenance: self.maintenance.snapshot(),
            admission: pool.stats(),
        }
    }
}
#[derive(Clone)]
pub(super) struct Request {
    global: Arc<Ledger>,
    job: Option<Arc<Ledger>>,
}
impl Request {
    pub(super) async fn permit(
        &self,
        pool: &Arc<Workspaces>,
    ) -> object_store::Result<crate::engine::execution::WorkspacePermit> {
        let result =
            if self.job.is_some() { pool.background().await } else { pool.acquire().await };
        let permit = result.map_err(|source| object_store::Error::Generic {
            store: "shared-request-budget",
            source: source.into(),
        })?;
        self.each(Ledger::requested);
        Ok(permit)
    }
    fn each(&self, apply: impl Fn(&Ledger)) {
        apply(&self.global);
        if let Some(job) = &self.job {
            apply(job);
        }
    }
    pub(super) fn reading(&self, bytes: u64) {
        self.each(|l| l.reading(bytes));
    }
    pub(super) fn read(&self, bytes: usize) {
        self.each(|l| l.read(u64::try_from(bytes).unwrap_or(u64::MAX)));
    }
    pub(super) fn writing(&self, bytes: usize) {
        self.each(|l| l.writing(u64::try_from(bytes).unwrap_or(u64::MAX)));
    }
    pub(super) fn written(&self, bytes: usize) {
        self.each(|l| l.written(u64::try_from(bytes).unwrap_or(u64::MAX)));
    }
}
