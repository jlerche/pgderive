use super::{Entry, Execution, Operators, RelationalState};
use crate::{
    catalog::Protection,
    engine::{
        Batch,
        dataflow::{Circuit, CircuitBuilder, Join, NodeOutput, Stream, TimedBatch},
        plan::{Kind, Node, Plan},
        reader::BatchData,
    },
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
type Edge<K, V> = Stream<Batch<K, V>>;
type Builder<K, V> = CircuitBuilder<RelationalState<K, V>>;
type Bound<K, V> = Circuit<RelationalState<K, V>, Batch<K, V>>;
pub(super) struct Built<K: BatchData, V: BatchData> {
    circuit: Bound<K, V>,
    sources: Vec<Edge<K, V>>,
}
impl<K: BatchData, V: BatchData> Built<K, V> {
    pub(super) async fn evaluate(
        &self,
        state: Arc<RelationalState<K, V>>,
        input: TimedBatch<Vec<Batch<K, V>>>,
    ) -> Result<(RelationalState<K, V>, Batch<K, V>)> {
        ensure!(self.sources.len() == input.batch.len(), "incomplete relational input");
        let mut values = self.circuit.inputs();
        for (source, batch) in self.sources.iter().zip(input.batch) {
            values.insert(source, batch)?;
        }
        self.circuit.evaluate(state, TimedBatch { time: input.time, batch: values }).await
    }
}
pub(super) fn validate<K: BatchData, V: BatchData>(
    plan: &Plan,
    operators: &Operators<K, V>,
) -> Result<()> {
    let definition = plan.definition();
    ensure!(definition.outputs.len() == 1, "relational executor requires one output");
    let mut projects = BTreeSet::new();
    let mut expansions = BTreeSet::new();
    let mut joins = BTreeSet::new();
    let mut partitions = BTreeSet::new();
    let mut statistics = BTreeSet::new();
    let mut maintained = BTreeSet::new();
    for member in &definition.arrangements {
        ensure!(maintained.insert(&member.node), "multiple arrangements on one relational node");
        let node = definition
            .nodes
            .iter()
            .find(|node| node.id == member.node)
            .context("missing state node")?;
        ensure!(node.kind != Kind::Source, "source state requires a project node");
    }
    ensure!(maintained.contains(&definition.outputs[0]), "missing relational output state");
    for node in &definition.nodes {
        match node.kind {
            Kind::Source => {}
            Kind::Expand => {
                expansions.insert(node.id.clone());
            }
            Kind::Project => {
                projects.insert(node.id.clone());
            }
            Kind::Join => {
                joins.insert(node.id.clone());
                ensure!(
                    node.inputs.iter().all(|id| maintained.contains(id)),
                    "join inputs require maintained state"
                );
            }
            Kind::Statistics => {
                statistics.insert(node.id.clone());
                ensure!(
                    maintained.contains(&node.id) && maintained.contains(&node.inputs[0]),
                    "statistics node requires retained input and state"
                );
            }
            Kind::Aggregate => {
                partitions.insert(node.id.clone());
                ensure!(
                    maintained.contains(&node.inputs[0]),
                    "partition input requires maintained state"
                );
            }
        }
    }
    ensure!(
        projects == operators.projects.keys().cloned().collect()
            && expansions == operators.expansions.keys().cloned().collect()
            && joins == operators.joins.keys().cloned().collect()
            && partitions == operators.partitions.keys().cloned().collect()
            && statistics == operators.statistics.keys().cloned().collect(),
        "relational callbacks differ from declarations"
    );
    Ok(())
}
impl<K: BatchData, V: BatchData> Execution<K, V> {
    pub(super) fn build_circuit(&self, protection: Option<&Protection>) -> Result<Built<K, V>> {
        let mut builder = CircuitBuilder::new(self.plan.clone());
        let mut edges = BTreeMap::new();
        let mut sources = Vec::new();
        for node in &self.plan.definition().nodes {
            let edge = match node.kind {
                Kind::Source => {
                    let edge = builder.source(&node.id)?;
                    sources.push(edge.clone());
                    edge
                }
                Kind::Expand => self.expand(&mut builder, node, &edges, protection)?,
                Kind::Project => self.project(&mut builder, node, &edges, protection)?,
                Kind::Join => self.join(&mut builder, node, &edges, protection)?,
                Kind::Statistics => self.statistics(&mut builder, node, &edges, protection)?,
                Kind::Aggregate => self.partition(&mut builder, node, &edges, protection)?,
            };
            edges.insert(node.id.clone(), edge);
        }
        let output =
            edges.get(&self.plan.definition().outputs[0]).context("missing output edge")?;
        Ok(Built { circuit: builder.build(output.output())?, sources })
    }
    fn expand(
        &self,
        builder: &mut Builder<K, V>,
        node: &Node,
        edges: &BTreeMap<String, Edge<K, V>>,
        protection: Option<&Protection>,
    ) -> Result<Edge<K, V>> {
        let operator =
            self.operators.expansions.get(&node.id).context("missing expansion callback")?.clone();
        let execution = Arc::new(self.scoped(protection)?);
        let id = node.id.clone();
        builder.unary(
            &node.id,
            edges.get(&node.inputs[0]).context("missing expansion edge")?,
            move |context, input| {
                let operator = operator.clone();
                let execution = execution.clone();
                let id = id.clone();
                async move {
                    let input = TimedBatch { time: input.time, batch: (*input.batch).clone() };
                    let delta = operator.evaluate(&input, execution.limits)?;
                    execution.stage(&id, &context.prior, delta).await
                }
            },
        )
    }
    fn statistics(
        &self,
        builder: &mut Builder<K, V>,
        node: &Node,
        edges: &BTreeMap<String, Edge<K, V>>,
        protection: Option<&Protection>,
    ) -> Result<Edge<K, V>> {
        let operator =
            self.operators.statistics.get(&node.id).context("missing statistics callback")?.clone();
        let execution = Arc::new(self.scoped(protection)?);
        let id = node.id.clone();
        let parent = node.inputs[0].clone();
        builder.unary(
            &node.id,
            edges.get(&node.inputs[0]).context("missing statistics edge")?,
            move |context, input| {
                let operator = operator.clone();
                let execution = execution.clone();
                let id = id.clone();
                let parent = parent.clone();
                async move {
                    let input = TimedBatch { time: input.time, batch: (*input.batch).clone() };
                    execution
                        .prior(&parent, &context.prior)?
                        .validate_bag_delta(&input.batch)
                        .await?;
                    let delta = operator
                        .evaluate(&input, execution.prior(&id, &context.prior)?, execution.limits)
                        .await?;
                    execution.stage(&id, &context.prior, delta).await
                }
            },
        )
    }
    fn partition(
        &self,
        builder: &mut Builder<K, V>,
        node: &Node,
        edges: &BTreeMap<String, Edge<K, V>>,
        protection: Option<&Protection>,
    ) -> Result<Edge<K, V>> {
        let operator =
            self.operators.partitions.get(&node.id).context("missing partition callback")?.clone();
        let execution = Arc::new(self.scoped(protection)?);
        let parent = node.inputs[0].clone();
        let id = node.id.clone();
        builder.unary(
            &node.id,
            edges.get(&parent).context("missing partition edge")?,
            move |context, input| {
                let operator = operator.clone();
                let execution = execution.clone();
                let parent = parent.clone();
                let id = id.clone();
                async move {
                    let input = TimedBatch { time: input.time, batch: (*input.batch).clone() };
                    let delta = operator
                        .evaluate(
                            &input,
                            execution.prior(&parent, &context.prior)?,
                            execution.limits,
                        )
                        .await?;
                    execution.stage(&id, &context.prior, delta).await
                }
            },
        )
    }
    fn project(
        &self,
        builder: &mut Builder<K, V>,
        node: &Node,
        edges: &BTreeMap<String, Edge<K, V>>,
        protection: Option<&Protection>,
    ) -> Result<Edge<K, V>> {
        let operator = self.operators.projects.get(&node.id).context("missing project")?.clone();
        let execution = Arc::new(self.scoped(protection)?);
        let id = node.id.clone();
        let limits = self.limits;
        builder.unary(
            &node.id,
            edges.get(&node.inputs[0]).context("missing input")?,
            move |context, input| {
                let operator = operator.clone();
                let execution = execution.clone();
                let id = id.clone();
                async move {
                    let delta = operator.evaluate_with_limits(&input, limits)?;
                    execution.stage(&id, context.prior.as_ref(), delta).await
                }
            },
        )
    }
    fn join(
        &self,
        builder: &mut Builder<K, V>,
        node: &Node,
        edges: &BTreeMap<String, Edge<K, V>>,
        protection: Option<&Protection>,
    ) -> Result<Edge<K, V>> {
        let operator = self.operators.joins.get(&node.id).context("missing join mapper")?.clone();
        let execution = Arc::new(self.scoped(protection)?);
        let parents = node.inputs.clone();
        let id = node.id.clone();
        let limits = self.limits;
        let left = edges.get(&parents[0]).context("missing left edge")?;
        let right = edges.get(&parents[1]).context("missing right edge")?;
        builder.binary(&node.id, (left, right), move |context, input| {
            let operator = operator.clone();
            let execution = execution.clone();
            let parents = parents.clone();
            let id = id.clone();
            async move {
                let left = TimedBatch { time: input.time, batch: input.batch.0 };
                let right = TimedBatch { time: input.time, batch: input.batch.1 };
                let joined = Join
                    .evaluate_with_limits(
                        (&left, &right),
                        (
                            execution.prior(&parents[0], &context.prior)?,
                            execution.prior(&parents[1], &context.prior)?,
                        ),
                        limits,
                    )
                    .await?;
                let delta = operator.evaluate_with_limits(&joined, limits)?;
                execution.stage(&id, context.prior.as_ref(), delta).await
            }
        })
    }
    fn scoped(&self, protection: Option<&Protection>) -> Result<Self> {
        let writers = self
            .writers
            .iter()
            .map(|(id, writer)| {
                let writer = if let Some(protection) = protection {
                    writer.clone().with_namespace(protection.namespace()?)
                } else {
                    writer.clone()
                };
                Ok((id.clone(), writer))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            plan: self.plan.clone(),
            operators: self.operators.clone(),
            writers,
            limits: self.limits,
            cache: self.cache.clone(),
        })
    }
    fn prior<'a>(
        &self,
        node: &str,
        state: &'a RelationalState<K, V>,
    ) -> Result<&'a crate::engine::trace::TraceSnapshot<K, V>> {
        let member = self
            .plan
            .definition()
            .arrangements
            .iter()
            .find(|member| member.node == node)
            .context("missing maintained input")?;
        Ok(&state.entries.get(&member.id).context("missing input trace")?.trace)
    }
    async fn stage(
        &self,
        node: &str,
        state: &RelationalState<K, V>,
        delta: TimedBatch<Batch<K, V>>,
    ) -> Result<NodeOutput<RelationalState<K, V>, Batch<K, V>>> {
        let Some(member) =
            self.plan.definition().arrangements.iter().find(|member| member.node == node)
        else {
            return Ok(NodeOutput::pure(delta.batch));
        };
        let writer = self.writers.get(&member.id).context("missing writer")?;
        let prior = state.entries.get(&member.id).context("missing prior state")?;
        let trace = writer.stage(&prior.trace, &delta).await?;
        let id = member.id.clone();
        let schema = member.schema.clone();
        Ok(NodeOutput::staged(delta.batch, move |state: &mut RelationalState<K, V>| {
            state.entries.insert(id, Entry { schema, trace });
        }))
    }
}
