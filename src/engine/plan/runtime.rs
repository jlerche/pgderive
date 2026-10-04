use super::{Plan, State};
use crate::engine::dataflow::{Graph, PreparedGraph, Stream};
use anyhow::Result;
use std::{future::Future, sync::Arc};

/// Production entry point for a registered, lifetime-bound typed acyclic query.
/// Immutable uploads are permitted during evaluation; external publication is not.
pub struct Engine<S: State, I, O> {
    plan: Arc<Plan>,
    graph: Graph<S, I, O>,
}
impl<S: State, I: 'static, O: 'static> Engine<S, I, O> {
    /// Bind validated registration to typed state and evaluator semantics.
    /// Semantic callback changes require a new plan revision.
    ///
    /// # Errors
    /// Rejects an invalid initial state contract.
    pub fn new<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        plan: Plan,
        initial: S,
        evaluate: impl Fn(Arc<S>, Stream<I>) -> F + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::restore(plan, initial, 0, evaluate)
    }
    /// Bind cold-validated durable state at its recorded logical boundary.
    /// Caller supplies authoritative membership; schema and all clocks are checked.
    ///
    /// # Errors
    /// Rejects missing, extra, or differently typed/timed arrangement state.
    pub fn restore<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        plan: Plan,
        initial: S,
        time: u64,
        evaluate: impl Fn(Arc<S>, Stream<I>) -> F + Send + Sync + 'static,
    ) -> Result<Self> {
        plan.validate_state(&initial, time)?;
        let plan = Arc::new(plan);
        let checked = plan.clone();
        let graph = Graph::at_boundary(initial, time, move |state, input: Stream<I>| {
            let time = input.time;
            let result = evaluate(state, input);
            let plan = checked.clone();
            async move {
                let (state, output) = result.await?;
                plan.validate_state(&state, time)?;
                Ok((state, output))
            }
        });
        Ok(Self { plan, graph })
    }
    /// Immutable registered query contract.
    #[must_use]
    pub fn plan(&self) -> &Plan {
        &self.plan
    }
    /// Last committed logical tick, independent of source LSNs.
    #[must_use]
    pub fn time(&self) -> u64 {
        self.graph.time()
    }
    /// Pin all committed query state.
    #[must_use]
    pub fn snapshot(&self) -> Arc<S> {
        self.graph.snapshot()
    }
    /// Stage a complete transaction; all declared arrangement clocks are checked.
    ///
    /// # Errors
    /// Returns node, state contract or transaction ordering failures.
    pub async fn prepare(&self, input: Stream<I>) -> Result<PreparedGraph<S, O>> {
        self.graph.prepare(input).await
    }
    pub(crate) async fn prepare_using<F: Future<Output = Result<(S, O)>>>(
        &self,
        input: Stream<I>,
        evaluate: impl FnOnce(Arc<S>, Stream<I>) -> F,
    ) -> Result<PreparedGraph<S, O>> {
        let time = input.time;
        let plan = self.plan.clone();
        self.graph
            .prepare_using(input, move |state, input| async move {
                let (state, output) = evaluate(state, input).await?;
                plan.validate_state(&state, time)?;
                Ok((state, output))
            })
            .await
    }
    /// Publish checked state through one local visibility pointer.
    ///
    /// # Errors
    /// Rejects stale/foreign work without changing visibility.
    pub fn commit(&mut self, prepared: PreparedGraph<S, O>) -> Result<Stream<O>> {
        self.graph.commit(prepared)
    }
    /// Verify preparation ownership before attempting external publication.
    ///
    /// # Errors
    /// Rejects foreign or stale candidates.
    pub fn validate_prepared(&self, prepared: &PreparedGraph<S, O>) -> Result<()> {
        self.graph.validate_prepared(prepared)
    }
}

impl<S: State, I: 'static, O: 'static> Engine<S, I, O> {
    /// Stage equivalence-checked physical maintenance and verify state contracts.
    ///
    /// # Errors
    /// Returns callback/state-contract errors without publication.
    pub async fn maintenance<F: Future<Output = Result<S>>>(
        &self,
        maintain: impl FnOnce(Arc<S>) -> F,
    ) -> Result<crate::engine::dataflow::PreparedMaintenance<S>> {
        let plan = self.plan.clone();
        let time = self.time();
        self.graph
            .prepare_maintenance(move |state| async move {
                let next = maintain(state).await?;
                plan.validate_state(&next, time)?;
                Ok(next)
            })
            .await
    }
    /// Check ownership before physical membership publication.
    ///
    /// # Errors
    /// Rejects foreign or stale maintenance.
    pub fn validate_maintenance(
        &self,
        prepared: &crate::engine::dataflow::PreparedMaintenance<S>,
    ) -> Result<()> {
        self.graph.validate_maintenance(prepared)
    }
    /// Publish checked equivalent physical state without advancing logical time.
    ///
    /// # Errors
    /// Rejects stale/foreign maintenance.
    pub fn commit_maintenance(
        &mut self,
        prepared: crate::engine::dataflow::PreparedMaintenance<S>,
    ) -> Result<()> {
        self.graph.commit_maintenance(prepared)
    }
}
