use super::{Plan, State};
use crate::engine::dataflow::{Graph, PreparedGraph, TimedBatch};
use anyhow::Result;
use std::{future::Future, sync::Arc};

/// Registered execution uses the same ownership runtime with a sealed plan policy.
pub type Engine<S, I, O> = Graph<S, I, O, Plan>;
impl<S: State, I: 'static, O: 'static> Graph<S, I, O, Plan> {
    /// Bind a registered initial state and evaluator.
    ///
    /// # Errors
    /// Rejects invalid initial state membership, schemas or clocks.
    pub fn registered<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        plan: Plan,
        initial: S,
        evaluate: impl Fn(Arc<S>, TimedBatch<I>) -> F + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::restore(plan, initial, 0, evaluate)
    }
    /// Bind a cold-validated state at its authoritative logical tick.
    ///
    /// # Errors
    /// Rejects mismatched state layout, schema or clock.
    pub fn restore<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        plan: Plan,
        initial: S,
        time: u64,
        evaluate: impl Fn(Arc<S>, TimedBatch<I>) -> F + Send + Sync + 'static,
    ) -> Result<Self> {
        plan.validate_state(&initial, time)?;
        Ok(Self::with_policy(initial, time, plan, evaluate))
    }
    /// Exact registered semantic and storage plan.
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.policy
    }
}

impl<S: super::checkpoint::DurableState, I: 'static, O: 'static> Graph<S, I, O, Plan> {
    pub(crate) fn checkpoint(&self) -> Result<super::Checkpoint> {
        super::Checkpoint::new(self.plan(), self.time(), self.snapshot().memberships()?)
    }
    pub(crate) fn prepared_checkpoint(
        &self,
        work: &PreparedGraph<S, O>,
    ) -> Result<super::Checkpoint> {
        self.validate_prepared(work)?;
        super::Checkpoint::new(self.plan(), work.output().time, work.candidate().memberships()?)
    }
    pub(crate) fn prepared_compaction_checkpoint(
        &self,
        work: &crate::engine::dataflow::PreparedMaintenance<S>,
    ) -> Result<super::Checkpoint> {
        self.validate_maintenance(work)?;
        super::Checkpoint::new(self.plan(), self.time(), work.candidate().memberships()?)
    }
    pub(crate) async fn publish_compaction(
        &mut self,
        sql: &mut crate::catalog::Connection,
        writer: &mut crate::catalog::Writer,
        work: crate::engine::dataflow::PreparedMaintenance<S>,
    ) -> Result<()> {
        self.validate_maintenance(&work)?;
        let durable = writer.confirmed(sql, self.plan()).await?;
        anyhow::ensure!(
            durable.stored.checkpoint == self.checkpoint()?,
            "compaction base differs from durable membership"
        );
        let checkpoint = self.prepared_compaction_checkpoint(&work)?;
        writer.maintain(sql, self.plan(), &checkpoint).await?;
        self.commit_maintenance(work)
    }
}
