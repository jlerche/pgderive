use crate::diagnostics::Identity;
use anyhow::{Context, Result};
use std::{collections::HashMap, future::Future, io::Write};
use tokio::task::{Id, JoinSet};

#[derive(Default)]
pub(super) struct Tasks {
    tasks: JoinSet<Result<()>>,
    identities: HashMap<Id, Identity>,
}
pub(super) struct Completion {
    pub(super) result: Result<()>,
    pub(super) fatal: bool,
}
impl Tasks {
    pub(super) fn spawn(
        &mut self,
        identity: Identity,
        work: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        let handle = self.tasks.spawn(work);
        self.identities.insert(handle.id(), identity);
    }
    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }
    pub(super) async fn next(&mut self) -> Completion {
        match self.join().await {
            Ok(result) => {
                let fatal = result
                    .as_ref()
                    .is_err_and(anyhow::Error::is::<crate::engine::capacity::Invariant>);
                Completion { result, fatal }
            }
            Err(error) => Completion { result: Err(error), fatal: true },
        }
    }
    async fn join(&mut self) -> Result<Result<()>> {
        let joined =
            self.tasks.join_next_with_id().await.context("query supervisor stream ended")?;
        let (id, result) = match joined {
            Ok(pair) => pair,
            Err(error) => (error.id(), Err(anyhow::Error::new(error).context("query task failed"))),
        };
        let identity = self.identities.remove(&id).context("query task identity missing")?;
        let event = match &result {
            Ok(()) => serde_json::json!({"event":"query_finished","status":"stopped"}),
            Err(error) => {
                serde_json::json!({"event":"query_finished","status":"failed","error":format!("{error:#}")})
            }
        };
        let mut output = std::io::stdout().lock();
        let logged = (|| -> Result<()> {
            crate::diagnostics::write_for(&mut output, &event, &identity)?;
            writeln!(output)?;
            output.flush()?;
            Ok(())
        })();
        logged.context("terminal query diagnostics failed")?;
        Ok(result)
    }
}
