use super::{
    CircuitInputs, Output, TimedBatch,
    stream::{Value, Values},
};
use crate::engine::plan::{Engine, Node, Plan, State};
use anyhow::{Context, Result, ensure};
use std::{future::Future, pin::Pin, sync::Arc};

/// Immutable committed prior state and logical time shared by every node in a tick.
pub struct NodeContext<S> {
    /// Prior committed state. Nodes never read another node's staged replacement.
    pub prior: Arc<S>,
    /// Logical delta-tick index, unrelated to WAL position or physical work chunks.
    pub time: u64,
}
impl<S> Clone for NodeContext<S> {
    fn clone(&self) -> Self {
        Self { prior: self.prior.clone(), time: self.time }
    }
}
type Replace<S> = dyn FnOnce(&mut S) + Send;

/// Deferred replacement of this node's registered state in a candidate snapshot.
///
/// The callback must only assign owned values or immutable snapshot descriptors.
/// It must not publish external effects or mutate shared prior state.
pub struct StateUpdate<S>(Box<Replace<S>>);
impl<S> StateUpdate<S> {
    /// Bind a pure, owned candidate-state replacement.
    pub fn new(update: impl FnOnce(&mut S) + Send + 'static) -> Self {
        Self(Box::new(update))
    }
}
/// An operator's delta value and optional deferred immutable state replacement.
pub struct NodeOutput<S, T> {
    value: T,
    update: Option<StateUpdate<S>>,
}
impl<S, T> NodeOutput<S, T> {
    /// Return a stateless operator's value.
    pub const fn pure(value: T) -> Self {
        Self { value, update: None }
    }
    /// Return a value and stage replacement of this node's registered arrangements.
    pub fn staged(value: T, update: impl FnOnce(&mut S) + Send + 'static) -> Self {
        Self { value, update: Some(StateUpdate::new(update)) }
    }
    pub(super) fn erase(self) -> Evaluated<S>
    where
        T: Send + Sync + 'static,
    {
        Evaluated { value: Arc::new(self.value), update: self.update }
    }
}
pub(super) struct Evaluated<S> {
    pub(super) value: Value,
    pub(super) update: Option<StateUpdate<S>>,
}
pub(super) type Task<S> = Pin<Box<dyn Future<Output = Result<Evaluated<S>>> + Send>>;
pub(super) type Evaluate<S> = dyn Fn(NodeContext<S>, &Values) -> Result<Task<S>> + Send + Sync;
pub(super) struct BoundNode<S> {
    pub(super) registration: Node,
    pub(super) source: bool,
    pub(super) evaluate: Option<Arc<Evaluate<S>>>,
}

/// Executable typed acyclic circuit bound to its complete semantic/storage plan.
///
/// Stateful snapshots must clone independently or contain only immutable shared
/// descriptors. Arbitrary shared mutable state and external callback effects are
/// outside this API's contract. Cycles, recursion and within-tick streaming are deferred.
pub struct Circuit<S, O> {
    pub(super) plan: Plan,
    pub(super) owner: Arc<()>,
    pub(super) nodes: Vec<BoundNode<S>>,
    pub(super) output: Output<O>,
}
impl<S: State + Clone, O: Send + Sync + 'static> Circuit<S, O> {
    /// The validated graph and durable semantic identity implemented by this circuit.
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.plan
    }
    /// Create an empty source collection for one future tick.
    #[must_use]
    pub fn inputs(&self) -> CircuitInputs {
        CircuitInputs { owner: self.owner.clone(), values: vec![None; self.nodes.len()] }
    }
    /// Evaluate a complete synchronized delta tick and return an unpublished candidate.
    /// Physical operator work never advances time; all nodes share one prior boundary.
    ///
    /// # Errors
    /// Rejects incomplete/foreign input, wrong time, node failures or invalid state.
    /// Every failure retains the caller's prior state and produces no visible output.
    pub async fn evaluate(
        &self,
        prior: Arc<S>,
        input: TimedBatch<CircuitInputs>,
    ) -> Result<(S, O)> {
        ensure!(Arc::ptr_eq(&self.owner, &input.batch.owner), "foreign circuit input");
        self.plan.validate_state(
            &*prior,
            input.time.checked_sub(1).context("tick zero is initial state")?,
        )?;
        self.validate_inputs(&input.batch.values)?;
        let context = NodeContext { prior: prior.clone(), time: input.time };
        let mut values = input.batch.values;
        let mut updates = Vec::new();
        for (index, node) in self.nodes.iter().enumerate() {
            if let Some(evaluate) = &node.evaluate {
                let result = evaluate(context.clone(), &values)?
                    .await
                    .with_context(|| format!("circuit node {}", node.registration.id))?;
                *values.get_mut(index).context("invalid circuit node index")? = Some(result.value);
                updates.extend(result.update);
            }
        }
        let output = (self.output.select)(&values)?;
        let mut candidate = (*prior).clone();
        for update in updates {
            (update.0)(&mut candidate);
        }
        self.plan.validate_state(&candidate, input.time)?;
        Ok((candidate, output))
    }
    fn validate_inputs(&self, values: &Values) -> Result<()> {
        ensure!(values.len() == self.nodes.len(), "invalid circuit input layout");
        for (node, value) in self.nodes.iter().zip(values) {
            ensure!(node.source == value.is_some(), "missing source or injected operator value");
        }
        Ok(())
    }
    /// Bind local prepare/commit ownership and state validation to this circuit.
    /// Durable publication and source acknowledgement remain the caller's responsibility.
    ///
    /// # Errors
    /// Rejects invalid initial arrangement membership, schemas or clocks.
    pub fn engine(self: Arc<Self>, initial: S) -> Result<Engine<S, CircuitInputs, O>> {
        self.engine_at(initial, 0)
    }
    /// Bind a cold-validated state at its authoritative logical tick.
    /// Source progress and checkpoint membership must be established by the caller.
    ///
    /// # Errors
    /// Rejects invalid arrangement membership, schemas or clocks.
    pub fn engine_at(
        self: Arc<Self>,
        initial: S,
        time: u64,
    ) -> Result<Engine<S, CircuitInputs, O>> {
        Engine::restore(self.plan.clone(), initial, time, move |prior, input| {
            let circuit = self.clone();
            async move { circuit.evaluate(prior, input).await }
        })
    }
}
