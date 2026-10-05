//! Executable grouped SQL lowering into typed circuit edges and stateful nodes.
use super::{Execution, Output, QueryState};
use crate::engine::{
    Batch,
    dataflow::{Circuit, CircuitBuilder, Join, NodeOutput, Project, Stream, SumState, TimedBatch},
    reader::BatchData,
};
use anyhow::Result;

type Executable<K, L, R, G> = Circuit<QueryState<K, L, R, G>, Output<G>>;
type Builder<K, L, R, G> = CircuitBuilder<QueryState<K, L, R, G>>;
type Source<K, V> = Stream<Batch<K, V>>;
type BuildResult<K, A, B, L, R, G> = Result<Built<K, A, B, L, R, G>>;
pub(super) struct Built<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
> {
    pub(super) circuit: Executable<K, L, R, G>,
    pub(super) left: Source<K, A>,
    pub(super) right: Source<K, B>,
}
impl<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> Execution<K, A, B, L, R, G, V>
{
    pub(super) fn build_circuit(&self) -> BuildResult<K, A, B, L, R, G> {
        let mut builder = CircuitBuilder::<QueryState<K, L, R, G>>::new(self.plan.clone());
        let source = |id: &str| {
            self.plan
                .definition()
                .nodes
                .iter()
                .find(|node| node.id == id)
                .and_then(|node| node.inputs.first())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing projected input"))
        };
        let left_source = builder.source(&source("left")?)?;
        let right_source = builder.source(&source("right")?)?;
        let left = self.left_edge(&mut builder, &left_source)?;
        let right = self.right_edge(&mut builder, &right_source)?;
        let limits = self.limits;
        let joined = builder.binary("join", (&left, &right), move |context, input| async move {
            let left = TimedBatch { time: input.time, batch: input.batch.0 };
            let right = TimedBatch { time: input.time, batch: input.batch.1 };
            let result = Join
                .evaluate_with_limits(
                    (&left, &right),
                    (&context.prior.left, &context.prior.right),
                    limits,
                )
                .await?;
            Ok(NodeOutput::pure(result.batch))
        })?;
        let operators = self.operators.clone();
        let grouped = builder.unary("group", &joined, move |_, input| {
            let operators = operators.clone();
            async move {
                Ok(NodeOutput::pure(operators.group.evaluate_with_limits(&input, limits)?.batch))
            }
        })?;
        let output = self.aggregate_edge(&mut builder, &grouped)?;
        Ok(Built {
            circuit: builder.build(output.output())?,
            left: left_source,
            right: right_source,
        })
    }
    fn left_edge(
        &self,
        builder: &mut Builder<K, L, R, G>,
        source: &Stream<Batch<K, A>>,
    ) -> Result<Stream<Batch<K, L>>> {
        let writer = self.left.clone();
        let operators = self.operators.clone();
        let limits = self.limits;
        builder.unary("left", source, move |context, input| {
            let operators = operators.clone();
            let writer = writer.clone();
            async move {
                let delta = operators.left.evaluate_with_limits(&input, limits)?;
                let next = writer.stage(&context.prior.left, &delta).await?;
                Ok(NodeOutput::staged(delta.batch, move |state: &mut QueryState<K, L, R, G>| {
                    state.left = next;
                }))
            }
        })
    }
    fn right_edge(
        &self,
        builder: &mut Builder<K, L, R, G>,
        source: &Stream<Batch<K, B>>,
    ) -> Result<Stream<Batch<K, R>>> {
        let writer = self.right.clone();
        let operators = self.operators.clone();
        let limits = self.limits;
        builder.unary("right", source, move |context, input| {
            let operators = operators.clone();
            let writer = writer.clone();
            async move {
                let delta = operators.right.evaluate_with_limits(&input, limits)?;
                let next = writer.stage(&context.prior.right, &delta).await?;
                Ok(NodeOutput::staged(delta.batch, move |state: &mut QueryState<K, L, R, G>| {
                    state.right = next;
                }))
            }
        })
    }
    fn aggregate_edge(
        &self,
        builder: &mut Builder<K, L, R, G>,
        grouped: &Stream<Batch<G, V>>,
    ) -> Result<Stream<Output<G>>> {
        let writers = (self.sums.clone(), self.output.clone());
        let operators = self.operators.clone();
        let limits = self.limits;
        builder.unary("aggregate", grouped, move |context, input| {
            let operators = operators.clone();
            let writers = writers.clone();
            async move {
                let delta =
                    operators.sum.evaluate_with_limits(&input, &context.prior.sums, limits).await?;
                let output = Project::new(|key: &G, value: &SumState| {
                    Ok(Some((
                        key.clone(),
                        (value.rows, (value.non_null != 0).then_some(value.sum)),
                    )))
                })
                .evaluate_with_limits(&delta.state, limits)?;
                let sums = writers.0.stage(&context.prior.sums, &delta.state).await?;
                let rows = writers.1.stage(&context.prior.output, &output).await?;
                Ok(NodeOutput::staged(output.batch, move |state: &mut QueryState<K, L, R, G>| {
                    state.sums = sums;
                    state.output = rows;
                }))
            }
        })
    }
}
