use super::{Entry, Execution, Operator, Operators, RelationalState};
use crate::engine::{
    Batch,
    dataflow::{Join, TimedBatch, Union},
    plan::{Kind, Node, Plan},
    reader::BatchData,
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt as _, stream};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
type Values<K, V> = BTreeMap<String, Arc<Batch<K, V>>>;
#[derive(Clone, Copy)]
pub(super) enum Staging<'a> {
    Objects(Option<&'a str>),
    Memory,
}
type Replacement<K, V> = (String, Entry<K, V>);
pub(super) fn validate<K: BatchData, V: BatchData>(
    plan: &Plan,
    operators: &Operators<K, V>,
) -> Result<()> {
    let definition = plan.definition();
    ensure!(definition.outputs.len() == 1, "relational executor requires one output");
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
    let mut callbacks = 0;
    for node in &definition.nodes {
        if matches!(node.kind, Kind::Source | Kind::Union) {
            continue;
        }
        let operator = operators.get(&node.id).context("missing operator callback")?;
        ensure!(operator.kind() == node.kind, "operator callback family differs from declaration");
        callbacks += 1;
        match operator {
            Operator::Join(_) | Operator::Window(_) => ensure!(
                node.inputs.iter().all(|id| maintained.contains(id)),
                "join inputs require maintained state"
            ),
            Operator::Ordered(_)
            | Operator::Statistics(_)
            | Operator::IndexedStatistics(_)
            | Operator::Group(_) => ensure!(
                maintained.contains(&node.id) && maintained.contains(&node.inputs[0]),
                "operator requires retained input and state"
            ),
            Operator::Partition(operator) => {
                ensure!(
                    !operator.supports_retained_output() || maintained.contains(&node.id),
                    "retained partition requires output state"
                );
                ensure!(
                    maintained.contains(&node.inputs[0]),
                    "partition input requires maintained state"
                );
            }
            Operator::Project(_) | Operator::Expand(_) => {}
        }
    }
    ensure!(callbacks == operators.len(), "relational callbacks differ from declarations");
    Ok(())
}
impl<K: BatchData, V: BatchData> Execution<K, V> {
    pub(super) async fn evaluate(
        &self,
        prior: Arc<RelationalState<K, V>>,
        input: TimedBatch<Vec<Batch<K, V>>>,
        staging: Staging<'_>,
    ) -> Result<(RelationalState<K, V>, Batch<K, V>)> {
        ensure!(
            input.batch.len() == self.plan.definition().sources.len(),
            "incomplete relational input"
        );
        let mut sources = input.batch.into_iter();
        let mut values = BTreeMap::new();
        for node in &self.plan.definition().nodes {
            let batch = match node.kind {
                Kind::Source => sources.next().context("incomplete relational input")?,
                Kind::Join | Kind::Union | Kind::Window => {
                    self.evaluate_binary(node, &prior, &values, input.time).await?
                }
                _ => {
                    let parent = values.get(&node.inputs[0]).context("missing unary input")?;
                    self.evaluate_unary(
                        node,
                        &prior,
                        TimedBatch { time: input.time, batch: Arc::clone(parent) },
                    )
                    .await?
                    .batch
                }
            };
            values.insert(node.id.clone(), Arc::new(batch));
        }
        let candidate = self.stage_all(&prior, &values, input.time, staging).await?;
        let output = values.remove(&self.plan.definition().outputs[0]).context("missing output")?;
        Ok((candidate, Arc::unwrap_or_clone(output)))
    }
    async fn stage_all(
        &self,
        prior: &RelationalState<K, V>,
        values: &Values<K, V>,
        time: u64,
        staging: Staging<'_>,
    ) -> Result<RelationalState<K, V>> {
        let mut uploads = Vec::new();
        for node in &self.plan.definition().nodes {
            if node.kind == Kind::Source {
                continue;
            }
            let batch = values.get(&node.id).context("missing staged output")?;
            uploads.push(self.stage(
                &node.id,
                prior,
                TimedBatch { time, batch: batch.as_ref() },
                staging,
            ));
        }
        // Drain every in-flight upload even on failure before the caller can
        // release its upload protection. No candidate becomes visible here.
        let completed = stream::iter(uploads).buffer_unordered(8).collect::<Vec<_>>().await;
        let mut candidate = prior.clone();
        for result in completed {
            if let Some((id, entry)) = result? {
                candidate.entries.insert(id, entry);
            }
        }
        Ok(candidate)
    }
    async fn evaluate_binary(
        &self,
        node: &Node,
        prior: &RelationalState<K, V>,
        values: &Values<K, V>,
        time: u64,
    ) -> Result<Batch<K, V>> {
        let left = values.get(&node.inputs[0]).context("missing left input")?.clone();
        let right = values.get(&node.inputs[1]).context("missing right input")?.clone();
        let input = TimedBatch { time, batch: (left, right) };
        if node.kind == Kind::Union {
            return Ok(Union.evaluate(&input, self.limits)?.batch);
        }
        if let Some(Operator::Window(operator)) = self.operators.get(&node.id) {
            return Ok(operator
                .evaluate(
                    &TimedBatch { time, batch: (&*input.batch.0, &*input.batch.1) },
                    (self.prior(&node.inputs[0], prior)?, self.prior(&node.inputs[1], prior)?),
                    self.limits,
                )
                .await?
                .batch);
        }
        let Some(Operator::Join(operator)) = self.operators.get(&node.id) else {
            anyhow::bail!("missing join mapper")
        };
        let left = TimedBatch { time, batch: input.batch.0 };
        let right = TimedBatch { time, batch: input.batch.1 };
        let joined = Join
            .evaluate_with_limits(
                (&left, &right),
                (self.prior(&node.inputs[0], prior)?, self.prior(&node.inputs[1], prior)?),
                self.limits,
            )
            .await?;
        Ok(operator.evaluate_with_limits(&joined, self.limits)?.batch)
    }
    async fn evaluate_unary(
        &self,
        node: &Node,
        prior: &RelationalState<K, V>,
        input: TimedBatch<Arc<Batch<K, V>>>,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        let operator = self.operators.get(&node.id).context("missing unary callback")?;
        if let Operator::Project(operator) = operator {
            return operator.evaluate_with_limits(&input, self.limits);
        }
        if let Operator::Group(operator) = operator {
            return operator
                .evaluate(&input, self.prior(&node.inputs[0], prior)?, self.limits)
                .await;
        }
        let input = TimedBatch { time: input.time, batch: (*input.batch).clone() };
        match operator {
            Operator::Expand(operator) => operator.evaluate(&input, self.limits),
            Operator::Ordered(operator) => {
                operator
                    .evaluate(
                        &input,
                        self.prior(&node.inputs[0], prior)?,
                        self.prior(&node.id, prior)?,
                        self.limits,
                    )
                    .await
            }
            Operator::Statistics(operator) => {
                self.prior(&node.inputs[0], prior)?.validate_bag_delta(&input.batch).await?;
                operator.evaluate(&input, self.prior(&node.id, prior)?, self.limits).await
            }
            Operator::IndexedStatistics(operator) => {
                self.prior(&node.inputs[0], prior)?.validate_bag_delta(&input.batch).await?;
                operator.evaluate(&input, self.prior(&node.id, prior)?, self.limits).await
            }
            Operator::Partition(operator) if operator.supports_retained_output() => {
                operator
                    .evaluate_retained(
                        &input,
                        self.prior(&node.inputs[0], prior)?,
                        self.prior(&node.id, prior)?,
                        self.limits,
                    )
                    .await
            }
            Operator::Partition(operator) => {
                operator.evaluate(&input, self.prior(&node.inputs[0], prior)?, self.limits).await
            }
            Operator::Join(_) | Operator::Window(_) | Operator::Project(_) | Operator::Group(_) => {
                anyhow::bail!("binary callback bound to unary node")
            }
        }
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
        prior: &RelationalState<K, V>,
        delta: TimedBatch<&Batch<K, V>>,
        staging: Staging<'_>,
    ) -> Result<Option<Replacement<K, V>>> {
        let Some(member) =
            self.plan.definition().arrangements.iter().find(|member| member.node == node)
        else {
            return Ok(None);
        };
        let mut writer = self.writers.get(&member.id).context("missing writer")?.clone();
        if let Staging::Objects(Some(namespace)) = staging {
            writer = writer.with_namespace(namespace);
        }
        let prior = &prior.entries.get(&member.id).context("missing prior state")?.trace;
        let trace = match staging {
            Staging::Objects(_) => writer.stage(prior, &delta).await?,
            Staging::Memory => {
                ensure!(prior.run_count() < 128, "memtable run limit requires checkpoint");
                prior.validate_delta(delta.batch).await?;
                prior.append_validated(
                    crate::engine::trace::Run::Memory(Arc::new(delta.batch.clone())),
                    delta.time,
                )?
            }
        };
        Ok(Some((member.id.clone(), Entry { schema: member.schema.clone(), trace })))
    }
}
