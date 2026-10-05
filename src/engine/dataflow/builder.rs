use super::{
    Circuit, NodeContext, NodeOutput, Output, StateUpdate, Stream, TimedBatch,
    circuit::{BoundNode, Evaluated, Task},
    stream::{Values, read},
};
use crate::engine::plan::{Kind, Node, Plan};
use anyhow::{Context, Result, ensure};
use std::{future::Future, sync::Arc};

/// Bind typed source/operator edges to a validated acyclic plan.
///
/// Handles can refer only to previously constructed nodes from this builder.
/// Plan revisions must identify callback semantics; callback effects must be pure.
pub struct CircuitBuilder<S> {
    plan: Plan,
    owner: Arc<()>,
    nodes: Vec<BoundNode<S>>,
}
impl<S: Send + Sync + 'static> CircuitBuilder<S> {
    /// Start binding the executable graph described by a semantic/storage plan.
    #[must_use]
    pub fn new(plan: Plan) -> Self {
        Self { plan, owner: Arc::new(()), nodes: Vec::new() }
    }
    /// Register a typed source edge. Each tick must explicitly supply its value.
    ///
    /// # Errors
    /// Rejects unknown, duplicate or non-source registrations.
    pub fn source<T: Send + Sync + 'static>(&mut self, id: &str) -> Result<Stream<T>> {
        let registration = self.registration(id, &[], true)?;
        Ok(self.push(registration, true, None))
    }
    /// Register a source edge that also stages its owned input arrangement.
    ///
    /// # Errors
    /// Rejects invalid source registration. Callback errors fail the whole tick.
    pub fn source_with<T, F>(
        &mut self,
        id: &str,
        evaluate: impl Fn(NodeContext<S>, Arc<T>) -> F + Send + Sync + 'static,
    ) -> Result<Stream<T>>
    where
        T: Send + Sync + 'static,
        F: Future<Output = Result<StateUpdate<S>>> + Send + 'static,
    {
        let registration = self.registration(id, &[], true)?;
        let index = self.nodes.len();
        let evaluate = Arc::new(move |context, values: &Values| -> Result<Task<S>> {
            let value = read::<T>(values, index)?;
            let future = evaluate(context, value.clone());
            Ok(Box::pin(async move { Ok(Evaluated { value, update: Some(future.await?) }) }))
        });
        Ok(self.push(registration, true, Some(evaluate)))
    }
    /// Bind a unary project/filter or aggregate to its typed input edge.
    ///
    /// # Errors
    /// Rejects foreign handles, wrong kind/arity or mismatched declared dependencies.
    pub fn unary<A, B, F>(
        &mut self,
        id: &str,
        input: &Stream<A>,
        evaluate: impl Fn(NodeContext<S>, TimedBatch<Arc<A>>) -> F + Send + Sync + 'static,
    ) -> Result<Stream<B>>
    where
        A: Send + Sync + 'static,
        B: Send + Sync + 'static,
        F: Future<Output = Result<NodeOutput<S, B>>> + Send + 'static,
    {
        let registration = self.registration(id, &[self.dependency(input)?], false)?;
        let index = input.index;
        let evaluate =
            Arc::new(move |context: NodeContext<S>, values: &Values| -> Result<Task<S>> {
                let input = TimedBatch { time: context.time, batch: read::<A>(values, index)? };
                let future = evaluate(context, input);
                Ok(Box::pin(async move { Ok(future.await?.erase()) }))
            });
        Ok(self.push(registration, false, Some(evaluate)))
    }
    /// Bind a binary join to its two typed input edges, preserving fan-out sharing.
    ///
    /// # Errors
    /// Rejects foreign handles, wrong kind/arity or mismatched declared dependencies.
    pub fn binary<A, B, C, F>(
        &mut self,
        id: &str,
        inputs: (&Stream<A>, &Stream<B>),
        evaluate: impl Fn(NodeContext<S>, TimedBatch<(Arc<A>, Arc<B>)>) -> F + Send + Sync + 'static,
    ) -> Result<Stream<C>>
    where
        A: Send + Sync + 'static,
        B: Send + Sync + 'static,
        C: Send + Sync + 'static,
        F: Future<Output = Result<NodeOutput<S, C>>> + Send + 'static,
    {
        let registration = self.registration(
            id,
            &[self.dependency(inputs.0)?, self.dependency(inputs.1)?],
            false,
        )?;
        let indices = (inputs.0.index, inputs.1.index);
        let evaluate =
            Arc::new(move |context: NodeContext<S>, values: &Values| -> Result<Task<S>> {
                let input = TimedBatch {
                    time: context.time,
                    batch: (read::<A>(values, indices.0)?, read::<B>(values, indices.1)?),
                };
                let future = evaluate(context, input);
                Ok(Box::pin(async move { Ok(future.await?.erase()) }))
            });
        Ok(self.push(registration, false, Some(evaluate)))
    }
    /// Freeze the complete executable graph with its declared visible output edges.
    ///
    /// # Errors
    /// Rejects missing nodes, foreign outputs or mismatched output order/membership.
    pub fn build<O>(self, output: Output<O>) -> Result<Circuit<S, O>> {
        ensure!(Arc::ptr_eq(&self.owner, &output.owner), "foreign circuit output");
        ensure!(self.nodes.len() == self.plan.definition().nodes.len(), "unbound plan nodes");
        let names = output
            .indices
            .iter()
            .map(|index| {
                self.nodes
                    .get(*index)
                    .map(|node| node.registration.id.clone())
                    .context("invalid output index")
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(names == self.plan.definition().outputs, "circuit outputs differ from plan");
        Ok(Circuit { plan: self.plan, owner: self.owner, nodes: self.nodes, output })
    }
    fn dependency<T>(&self, edge: &Stream<T>) -> Result<String> {
        ensure!(Arc::ptr_eq(&self.owner, &edge.owner), "foreign stream dependency");
        self.nodes
            .get(edge.index)
            .map(|node| node.registration.id.clone())
            .context("unknown stream dependency")
    }
    fn registration(&self, id: &str, inputs: &[String], source: bool) -> Result<Node> {
        ensure!(
            !self.nodes.iter().any(|node| node.registration.id == id),
            "duplicate circuit node"
        );
        let node = self
            .plan
            .definition()
            .nodes
            .iter()
            .find(|node| node.id == id)
            .context("unknown plan node")?;
        ensure!((node.kind == Kind::Source) == source, "circuit source/operator kind mismatch");
        ensure!(source || node.inputs == inputs, "circuit dependencies differ from plan");
        Ok(node.clone())
    }
    fn push<T>(
        &mut self,
        registration: Node,
        source: bool,
        evaluate: Option<Arc<super::circuit::Evaluate<S>>>,
    ) -> Stream<T> {
        let edge = Stream::new(self.owner.clone(), self.nodes.len(), source);
        self.nodes.push(BoundNode { registration, source, evaluate });
        edge
    }
}
