use super::Stream;
use anyhow::{Result, ensure};
use std::{future::Future, pin::Pin, sync::Arc};
type Evaluation<S, O> = Pin<Box<dyn Future<Output = Result<(S, O)>> + Send>>;
type Evaluator<S, I, O> = dyn Fn(Arc<S>, Stream<I>) -> Evaluation<S, O> + Send + Sync;
struct Boundary<S> {
    state: Arc<S>,
    time: u64,
}

/// Unpublished complete graph state/output, tied to its exact prior boundary.
pub struct PreparedGraph<S, O> {
    base: Arc<Boundary<S>>,
    next: Arc<Boundary<S>>,
    output: Stream<O>,
}
impl<S, O> PreparedGraph<S, O> {
    /// Logical boundary from which this transaction was evaluated.
    #[must_use]
    pub fn base_time(&self) -> u64 {
        self.base.time
    }
    /// Pin the complete unpublished candidate for durable membership encoding.
    #[must_use]
    pub fn candidate(&self) -> Arc<S> {
        self.next.state.clone()
    }
    /// Inspect staged result deltas without publishing local visibility.
    #[must_use]
    pub const fn output(&self) -> &Stream<O> {
        &self.output
    }
}
/// Typed acyclic transaction runtime with lifetime-bound graph semantics.
///
/// State must consist of immutable snapshots/owned values, never shared mutable
/// state. Evaluation may upload immutable objects but must not publish external
/// effects. This runtime provides local visibility, not durable publication.
pub struct Graph<S, I, O> {
    root: Arc<Boundary<S>>,
    evaluate: Arc<Evaluator<S, I, O>>,
}
impl<S: Send + Sync + 'static, I: 'static, O: 'static> Graph<S, I, O> {
    /// Bind an explicit typed operator graph and its immutable initial state.
    /// Every node must use the incoming tick and stage its state before returning.
    pub fn new<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        initial: S,
        evaluate: impl Fn(Arc<S>, Stream<I>) -> F + Send + Sync + 'static,
    ) -> Self {
        Self::at_boundary(initial, 0, evaluate)
    }
    pub(crate) fn at_boundary<F: Future<Output = Result<(S, O)>> + Send + 'static>(
        initial: S,
        time: u64,
        evaluate: impl Fn(Arc<S>, Stream<I>) -> F + Send + Sync + 'static,
    ) -> Self {
        Self {
            root: Arc::new(Boundary { state: Arc::new(initial), time }),
            evaluate: Arc::new(move |state, input| Box::pin(evaluate(state, input))),
        }
    }
    /// Pin all graph state at one committed boundary.
    #[must_use]
    pub fn snapshot(&self) -> Arc<S> {
        self.root.state.clone()
    }
    /// Last committed transaction tick.
    #[must_use]
    pub fn time(&self) -> u64 {
        self.root.time
    }
    /// Evaluate the fixed graph without changing its visibility boundary.
    ///
    /// # Errors
    /// Rejects wrong ticks and propagates every node's failure.
    pub async fn prepare(&self, input: Stream<I>) -> Result<PreparedGraph<S, O>> {
        self.prepare_using(input, |state, input| (self.evaluate)(state, input)).await
    }
    pub(crate) async fn prepare_using<F: Future<Output = Result<(S, O)>>>(
        &self,
        input: Stream<I>,
        evaluate: impl FnOnce(Arc<S>, Stream<I>) -> F,
    ) -> Result<PreparedGraph<S, O>> {
        ensure!(self.root.time.checked_add(1) == Some(input.time), "out-of-order graph tick");
        let time = input.time;
        let (state, output) = evaluate(self.snapshot(), input).await?;
        Ok(PreparedGraph {
            base: self.root.clone(),
            next: Arc::new(Boundary { state: Arc::new(state), time }),
            output: Stream { time, batch: output },
        })
    }
    /// Publish every staged node through one root assignment.
    ///
    /// # Errors
    /// Rejects foreign/stale work without changing state or returning its output.
    pub fn commit(&mut self, prepared: PreparedGraph<S, O>) -> Result<Stream<O>> {
        self.validate_prepared(&prepared)?;
        self.root = prepared.next;
        Ok(prepared.output)
    }
    /// Check ownership and freshness before starting external durable publication.
    /// Hold exclusive ownership of this runtime across publication and local commit.
    ///
    /// # Errors
    /// Rejects a preparation evaluated by another runtime or from an older root.
    pub fn validate_prepared(&self, prepared: &PreparedGraph<S, O>) -> Result<()> {
        ensure!(Arc::ptr_eq(&self.root, &prepared.base), "foreign or stale graph preparation");
        Ok(())
    }
}

/// Unpublished physical maintenance, with no logical output or time advance.
pub struct PreparedMaintenance<S> {
    base: Arc<Boundary<S>>,
    next: Arc<Boundary<S>>,
}
impl<S> PreparedMaintenance<S> {
    /// Pin equivalence-checked physical state before durable membership publication.
    #[must_use]
    pub fn candidate(&self) -> Arc<S> {
        self.next.state.clone()
    }
}
impl<S: Send + Sync + 'static, I: 'static, O: 'static> Graph<S, I, O> {
    /// Check maintenance belongs to the current immutable boundary.
    ///
    /// # Errors
    /// Rejects foreign or stale physical candidates.
    pub fn validate_maintenance(&self, prepared: &PreparedMaintenance<S>) -> Result<()> {
        ensure!(Arc::ptr_eq(&self.root, &prepared.base), "foreign or stale graph maintenance");
        Ok(())
    }
    /// Stage physical maintenance over immutable state at the current boundary.
    /// The callback must use equivalence-checked arrangement compaction only.
    ///
    /// # Errors
    /// Returns maintenance errors without changing the root.
    pub async fn prepare_maintenance<F: Future<Output = Result<S>>>(
        &self,
        maintain: impl FnOnce(Arc<S>) -> F,
    ) -> Result<PreparedMaintenance<S>> {
        let state = maintain(self.snapshot()).await?;
        Ok(PreparedMaintenance {
            base: self.root.clone(),
            next: Arc::new(Boundary { state: Arc::new(state), time: self.time() }),
        })
    }
    /// Publish equivalent physical state without emitting a logical edge.
    ///
    /// # Errors
    /// Rejects stale or foreign maintenance without changing visibility.
    pub fn commit_maintenance(&mut self, prepared: PreparedMaintenance<S>) -> Result<()> {
        self.validate_maintenance(&prepared)?;
        self.root = prepared.next;
        Ok(())
    }
}
