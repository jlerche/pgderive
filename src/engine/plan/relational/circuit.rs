pub(super) mod bootstrap;
use super::{Entry, Execution, Operator, Operators, RelationalState};
use crate::engine::{
    Batch,
    dataflow::{Join, TimedBatch, Union},
    plan::{Kind, Node, Plan},
    reader::{BatchCursor as _, BatchData, BatchReader as _},
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt as _, stream};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
type Values<K, V> = BTreeMap<String, Arc<crate::engine::execution::Sealed<K, V>>>;
struct ValidatedValues<'a, K: BatchData, V: BatchData> {
    batches: Values<K, V>,
    bags: BTreeMap<String, crate::engine::trace::ValidatedBag<'a, K, V>>,
    statistics: BTreeMap<String, crate::engine::dataflow::StatisticsOutput<'a, K, V>>,
}
#[derive(Clone, Copy)]
pub(super) enum Staging<'a> {
    Objects(Option<&'a str>),
    Memory,
}
struct StagePolicy<'a, 'b, K: BatchData, V: BatchData> {
    staging: Staging<'a>,
    proof: Option<Validation<'a, 'b, K, V>>,
}
enum Validation<'a, 'b, K: BatchData, V: BatchData> {
    Bag(&'a crate::engine::trace::ValidatedBag<'b, K, V>),
    Statistics(&'a crate::engine::dataflow::StatisticsOutput<'b, K, V>),
}
type Replacement<K, V> = (String, Entry<K, V>);
pub(super) fn validate<K: BatchData, V: BatchData>(
    plan: &Plan,
    operators: &Operators<K, V>,
    source_statistics: &BTreeSet<String>,
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
    for id in source_statistics {
        ensure!(
            matches!(operators.get(id), Some(Operator::Statistics(_))),
            "source proof requires linear statistics"
        );
    }
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
            Operator::Statistics(_) if source_statistics.contains(&node.id) => {
                ensure!(maintained.contains(&node.id), "statistics require retained state");
            }
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
        input: TimedBatch<Vec<crate::engine::execution::Sealed<K, V>>>,
        staging: Staging<'_>,
    ) -> Result<(RelationalState<K, V>, Batch<K, V>)> {
        self.evaluate_profiled(prior, input, staging, &mut crate::timing::Profile::new(false)).await
    }
    pub(super) async fn evaluate_profiled(
        &self,
        prior: Arc<RelationalState<K, V>>,
        input: TimedBatch<Vec<crate::engine::execution::Sealed<K, V>>>,
        staging: Staging<'_>,
        profile: &mut crate::timing::Profile,
    ) -> Result<(RelationalState<K, V>, Batch<K, V>)> {
        ensure!(
            input.batch.len() == self.plan.definition().sources.len(),
            "incomplete relational input"
        );
        let mut sources = input.batch.into_iter();
        let mut values = ValidatedValues {
            batches: BTreeMap::new(),
            bags: BTreeMap::new(),
            statistics: BTreeMap::new(),
        };
        for node in &self.plan.definition().nodes {
            self.validate_input_bag(node, &prior, &mut values).await?;
            if matches!(
                self.operators.get(&node.id),
                Some(Operator::Statistics(_) | Operator::IndexedStatistics(_))
            ) {
                profile.mark("input_bag_validation");
            }
            if let Some(Operator::Statistics(operator)) = self.operators.get(&node.id) {
                let parent =
                    values.batches.get(&node.inputs[0]).context("missing statistics input")?;
                let result = operator
                    .evaluate_sealed(parent, self.prior(&node.id, &prior)?, self.limits)
                    .await?;
                values.batches.insert(node.id.clone(), Arc::clone(result.batch()));
                values.statistics.insert(node.id.clone(), result);
                profile.operator(&node.id);
                continue;
            }
            let batch = match node.kind {
                Kind::Source => sources.next().context("incomplete relational input")?,
                Kind::Join | Kind::Union | Kind::Window => {
                    self.binary_sealed(node, &prior, &values.batches, input.time).await?
                }
                _ => {
                    let parent =
                        values.batches.get(&node.inputs[0]).context("missing unary input")?;
                    self.unary_sealed(node, &prior, parent, input.time).await?
                }
            };
            values.batches.insert(node.id.clone(), Arc::new(batch));
            profile.operator(&node.id);
        }
        let candidate = self.stage_all(&prior, &values, input.time, staging).await?;
        profile.mark("stage_arrangements");
        let output =
            values.batches.remove(&self.plan.definition().outputs[0]).context("missing output")?;
        Ok((candidate, output.materialize(self.limits).await?))
    }
    async fn binary_sealed(
        &self,
        node: &Node,
        prior: &RelationalState<K, V>,
        values: &Values<K, V>,
        time: u64,
    ) -> Result<crate::engine::execution::Sealed<K, V>> {
        Ok(if node.kind == Kind::Join {
            self.join_sealed(
                node,
                prior,
                TimedBatch {
                    time,
                    batch: (values[&node.inputs[0]].as_ref(), values[&node.inputs[1]].as_ref()),
                },
            )
            .await?
        } else if node.kind == Kind::Union {
            self.union_sealed(values[&node.inputs[0]].as_ref(), values[&node.inputs[1]].as_ref())
                .await?
        } else {
            self.evaluate_binary(node, prior, values, time).await?.into()
        })
    }
    async fn validate_input_bag<'a>(
        &self,
        node: &Node,
        prior: &'a RelationalState<K, V>,
        values: &mut ValidatedValues<'a, K, V>,
    ) -> Result<()> {
        if !matches!(
            self.operators.get(&node.id),
            Some(Operator::Statistics(_) | Operator::IndexedStatistics(_))
        ) || values.bags.contains_key(&node.inputs[0])
        {
            return Ok(());
        }
        let parent = values.batches.get(&node.inputs[0]).context("missing aggregate input")?;
        let Some(parent) = parent.resident() else {
            if self.source_statistics.contains(&node.id) {
                let mut cursor = parent.cursor().await?;
                while let Some((_, _, weight)) = cursor.current() {
                    ensure!(
                        matches!(weight, -1 | 1),
                        "source-key proof requires unit tuple deltas"
                    );
                    cursor.advance().await?;
                }
            } else {
                self.validate_sealed(self.prior(&node.inputs[0], prior)?, parent, true).await?;
            }
            return Ok(());
        };
        if self.source_statistics.contains(&node.id) {
            ensure!(
                parent.iter().all(|(_, weight)| matches!(weight, -1 | 1)),
                "source-key proof requires unit tuple deltas"
            );
            // Another consumer may still require this input arrangement. Its
            // source-key invariant proves the same full-tuple coefficient bound.
            if self
                .plan
                .definition()
                .arrangements
                .iter()
                .any(|member| member.node == node.inputs[0])
            {
                let proof =
                    self.prior(&node.inputs[0], prior)?.certify_source_batch(Arc::clone(parent))?;
                values.bags.insert(node.inputs[0].clone(), proof);
            }
            return Ok(());
        }
        let proof =
            self.prior(&node.inputs[0], prior)?.validate_bag_batch(Arc::clone(parent)).await?;
        values.bags.insert(node.inputs[0].clone(), proof);
        Ok(())
    }
    async fn stage_all(
        &self,
        prior: &RelationalState<K, V>,
        values: &ValidatedValues<'_, K, V>,
        time: u64,
        staging: Staging<'_>,
    ) -> Result<RelationalState<K, V>> {
        let mut uploads = Vec::new();
        for node in &self.plan.definition().nodes {
            if !self.plan.definition().arrangements.iter().any(|a| a.node == node.id) {
                continue;
            }
            let batch = values.batches.get(&node.id).context("missing staged output")?;
            uploads.push(
                self.stage(
                    &node.id,
                    prior,
                    TimedBatch { time, batch: batch.as_ref() },
                    StagePolicy {
                        staging,
                        proof: values
                            .statistics
                            .get(&node.id)
                            .map(Validation::Statistics)
                            .or_else(|| values.bags.get(&node.id).map(Validation::Bag)),
                    },
                ),
            );
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
        let left = Arc::new(
            values
                .get(&node.inputs[0])
                .context("missing left input")?
                .materialize(self.limits)
                .await?,
        );
        let right = Arc::new(
            values
                .get(&node.inputs[1])
                .context("missing right input")?
                .materialize(self.limits)
                .await?,
        );
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
    pub(super) fn prior<'a>(
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
        delta: TimedBatch<&crate::engine::execution::Sealed<K, V>>,
        policy: StagePolicy<'_, '_, K, V>,
    ) -> Result<Option<Replacement<K, V>>> {
        let StagePolicy { staging, proof } = policy;
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
        ensure!(prior.time().checked_add(1) == Some(delta.time), "out-of-order staged tick");
        ensure!(
            prior.run_count() < crate::engine::trace::MAX_RUNS,
            "arrangement run limit requires checkpoint"
        );
        match proof {
            Some(Validation::Statistics(proof)) => {
                ensure!(proof.matches(prior, delta.batch), "statistics boundary mismatch");
            }
            Some(Validation::Bag(proof)) => ensure!(
                delta.batch.resident().is_some_and(|batch| proof.matches(prior, batch)),
                "validated batch boundary mismatch"
            ),
            None => {
                if let Some(batch) = delta.batch.resident() {
                    prior.validate_delta(batch).await?;
                } else {
                    self.validate_sealed(prior, delta.batch, false).await?;
                }
            }
        }
        let trace = match staging {
            Staging::Memory => {
                let batch = if let Some(batch) = delta.batch.resident() {
                    Arc::clone(batch)
                } else {
                    Arc::new(delta.batch.materialize(self.limits).await?)
                };
                prior.append_validated(crate::engine::trace::Run::Memory(batch), delta.time)?
            }
            Staging::Objects(_) => {
                let mut cursor = delta.batch.cursor().await?;
                let runs = writer.upload_cursor(&mut cursor).await?;
                prior.append_runs_validated(runs, delta.time)?
            }
        };
        Ok(Some((member.id.clone(), Entry { schema: member.schema.clone(), trace })))
    }
}

#[cfg(test)]
mod tests;
