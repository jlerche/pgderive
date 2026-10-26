//! Positive snapshot chunks flow through pure nodes into bounded operator state.
use super::*;
use crate::engine::execution::{Consolidator, Limits, Sealed};
use futures_util::future::BoxFuture;

type Fold<K, V> = Box<dyn crate::engine::dataflow::SnapshotFold<K, V, V>>;
type Buffers<K, V> = BTreeMap<String, Consolidator<(K, V)>>;
pub struct Bootstrap<K: BatchData, V: BatchData> {
    execution: Arc<Execution<K, V>>,
    owner: Arc<Execution<K, V>>,
    prior: Arc<RelationalState<K, V>>,
    buffers: Buffers<K, V>,
    folds: BTreeMap<String, Fold<K, V>>,
    completed: Values<K, V>,
    limits: Limits,
    observed: BTreeMap<String, usize>,
    emitted: BTreeMap<String, usize>,
    failed: bool,
}
impl<K: BatchData, V: BatchData> Bootstrap<K, V> {
    pub(super) fn new(
        execution: Arc<Execution<K, V>>,
        prior: Arc<RelationalState<K, V>>,
        budget: usize,
    ) -> Result<Self> {
        let plan = execution.plan.definition();
        let mut required =
            plan.arrangements.iter().map(|a| a.node.clone()).collect::<BTreeSet<_>>();
        let nonlinear = plan
            .nodes
            .iter()
            .filter(|n| {
                !matches!(n.kind, Kind::Source | Kind::Project | Kind::Expand | Kind::Union)
            })
            .collect::<Vec<_>>();
        for node in &nonlinear {
            if !matches!(execution.operators.get(&node.id), Some(Operator::Statistics(_))) {
                required.extend(node.inputs.iter().cloned());
            }
        }
        let count = plan.nodes.len().checked_mul(4).context("snapshot workspace count overflow")?;
        let limits = Limits {
            resident_bytes: execution.limits.resident_bytes.min(budget / count.max(1)),
            scratch_bytes: execution.limits.scratch_bytes / u64::try_from(count.max(1))?,
            ..execution.limits
        }
        .validate()?;
        let buffers = required
            .into_iter()
            .map(|id| Ok((id, Consolidator::new(limits)?)))
            .collect::<Result<_>>()?;
        let fold_count = nonlinear
            .iter()
            .filter(|n| matches!(execution.operators.get(&n.id), Some(Operator::Statistics(_))))
            .count()
            .max(1);
        let fold_limits = Limits {
            resident_bytes: budget / 2 / fold_count,
            resident_entries: execution.limits.output_entries / fold_count,
            scratch_bytes: execution.limits.scratch_bytes / u64::try_from(fold_count * 4)?,
            ..execution.limits
        }
        .validate()?;
        let mut folds = BTreeMap::new();
        for node in nonlinear {
            if let Some(Operator::Statistics(op)) = execution.operators.get(&node.id) {
                folds.insert(node.id.clone(), op.snapshot_fold(fold_limits)?);
            }
        }
        let owner = execution;
        let execution = Arc::new(Execution { limits, ..(*owner).clone() });
        Ok(Self {
            execution,
            owner,
            prior,
            buffers,
            folds,
            completed: BTreeMap::new(),
            limits,
            observed: BTreeMap::new(),
            emitted: BTreeMap::new(),
            failed: false,
        })
    }
    pub(crate) const fn input_limits(&self) -> Limits {
        self.limits
    }
    pub(crate) async fn append(&mut self, sources: Vec<Batch<K, V>>) -> Result<()> {
        let _workspace = self.execution.workspaces.acquire().await?;
        ensure!(!self.failed, "failed snapshot workspace cannot be reused");
        self.failed = true;
        ensure!(
            sources.len() == self.execution.plan.definition().sources.len(),
            "snapshot source count mismatch"
        );
        let nodes = self
            .execution
            .plan
            .definition()
            .nodes
            .iter()
            .filter(|n| n.kind == Kind::Source)
            .map(|n| n.id.clone())
            .collect::<Vec<_>>();
        for (id, batch) in nodes.into_iter().zip(sources) {
            ensure!(batch.iter().all(|(_, w)| *w > 0), "snapshot requires positive source rows");
            self.feed(id, batch.into()).await?;
        }
        self.failed = false;
        Ok(())
    }
    fn feed(&mut self, id: String, batch: Sealed<K, V>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let count = self.observed.entry(id.clone()).or_default();
            *count =
                count.checked_add(batch.len()).context("snapshot contribution count overflow")?;
            ensure!(
                *count <= self.execution.limits.contributions,
                crate::engine::execution::CapacityExceeded("snapshot contribution limit exceeded")
            );
            if let Some(buffer) = self.buffers.get_mut(&id) {
                let mut cursor = batch.cursor().await?;
                while let Some((key, value, weight)) = cursor.current() {
                    buffer.add((key.clone(), value.clone()), weight)?;
                    cursor.advance().await?;
                }
            }
            let children = self
                .execution
                .plan
                .definition()
                .nodes
                .iter()
                .filter(|n| n.inputs.contains(&id))
                .cloned()
                .collect::<Vec<_>>();
            for node in children {
                self.deliver(&node, &id, &batch).await?;
            }
            Ok(())
        })
    }
    async fn deliver(&mut self, node: &Node, parent: &str, batch: &Sealed<K, V>) -> Result<()> {
        if let Some(fold) = self.folds.get_mut(&node.id) {
            observe(fold.as_mut(), batch).await?;
            return Ok(());
        }
        if node.kind == Kind::Union {
            for _ in node.inputs.iter().filter(|input| **input == *parent) {
                self.feed(node.id.clone(), batch.clone()).await?;
            }
            return Ok(());
        }
        let contributions = self.emitted.entry(node.id.clone()).or_default();
        let output = match self.execution.operators.get(&node.id) {
            Some(Operator::Project(op)) => {
                Some(op.evaluate_sealed_counted(batch, self.limits, contributions).await?)
            }
            Some(Operator::Expand(op)) => {
                Some(op.evaluate_sealed_counted(batch, self.limits, contributions).await?)
            }
            _ => None,
        };
        if let Some(output) = output {
            self.feed(node.id.clone(), output).await?;
        }
        Ok(())
    }
    fn seal(&mut self, id: &str) -> Result<()> {
        if !self.completed.contains_key(id) {
            let input =
                self.buffers.remove(id).context("missing snapshot operator input")?.seal()?;
            self.completed.insert(id.to_owned(), Arc::new(input));
        }
        Ok(())
    }
    async fn finish(
        mut self,
        namespace: Option<&str>,
    ) -> Result<(RelationalState<K, V>, Batch<K, V>)> {
        let workspace = self.execution.workspaces.acquire().await?;
        ensure!(!self.failed, "failed snapshot workspace cannot finish");
        let nodes = self.execution.plan.definition().nodes.clone();
        for node in nodes.iter().filter(|n| {
            !matches!(n.kind, Kind::Source | Kind::Project | Kind::Expand | Kind::Union)
        }) {
            let output = if let Some(fold) = self.folds.remove(&node.id) {
                fold.finish()?
            } else {
                for input in &node.inputs {
                    self.seal(input)?;
                }
                if matches!(node.kind, Kind::Join | Kind::Window) {
                    self.execution.binary_sealed(node, &self.prior, &self.completed, 1).await?
                } else {
                    let input =
                        self.completed.get(&node.inputs[0]).context("missing snapshot input")?;
                    self.execution.unary_sealed(node, &self.prior, input, 1).await?
                }
            };
            self.feed(node.id.clone(), output).await?;
        }
        let required = self
            .execution
            .plan
            .definition()
            .arrangements
            .iter()
            .map(|a| a.node.clone())
            .collect::<Vec<_>>();
        for id in required {
            self.seal(&id)?;
        }
        let values = ValidatedValues {
            batches: self.completed,
            bags: BTreeMap::new(),
            statistics: BTreeMap::new(),
        };
        drop(workspace);
        let state =
            self.execution.stage_all(&self.prior, &values, 1, Staging::Objects(namespace)).await?;
        let _workspace = self.execution.workspaces.acquire().await?;
        let output = values
            .batches
            .get(&self.execution.plan.definition().outputs[0])
            .context("missing snapshot result")?
            .materialize(self.execution.limits)
            .await?;
        Ok((state, output))
    }
}
impl<K: BatchData, V: BatchData> super::super::Relational<K, V> {
    pub(crate) fn bootstrap_workspace(&self, budget: usize) -> Result<Bootstrap<K, V>> {
        ensure!(self.time() == 0, "snapshot bootstrap requires empty runtime");
        Bootstrap::new(self.execution.clone(), self.snapshot(), budget)
    }
    pub(crate) async fn prepare_bootstrap(
        &self,
        work: Bootstrap<K, V>,
        protection: &crate::catalog::Protection,
    ) -> Result<super::super::Prepared<K, V>> {
        ensure!(
            Arc::ptr_eq(&work.owner, &self.execution) && Arc::ptr_eq(&work.prior, &self.snapshot()),
            "foreign snapshot workspace"
        );
        let namespace = protection.namespace()?;
        self.engine
            .prepare_using(TimedBatch { time: 1, batch: Vec::new() }, |_, _| {
                work.finish(Some(namespace))
            })
            .await
    }
}

async fn observe<K: BatchData, V: BatchData>(
    fold: &mut dyn crate::engine::dataflow::SnapshotFold<K, V, V>,
    batch: &Sealed<K, V>,
) -> Result<()> {
    let mut cursor = batch.cursor().await?;
    while let Some((key, value, weight)) = cursor.current() {
        fold.observe(key, value, weight)?;
        cursor.advance().await?;
    }
    Ok(())
}

#[cfg(test)]
impl<K: BatchData, V: BatchData> super::super::Relational<K, V> {
    pub(crate) async fn prepare_bootstrap_unprotected(
        &self,
        work: Bootstrap<K, V>,
    ) -> Result<super::super::Prepared<K, V>> {
        ensure!(
            Arc::ptr_eq(&work.owner, &self.execution) && Arc::ptr_eq(&work.prior, &self.snapshot()),
            "foreign snapshot workspace"
        );
        self.engine
            .prepare_using(TimedBatch { time: 1, batch: Vec::new() }, |_, _| work.finish(None))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        dataflow::Expand,
        plan::{Arrangement, Definition, Settings, Source},
    };
    fn query(
        kind: Kind,
        inputs: Vec<String>,
        operators: Operators<i64, i64>,
        limits: Limits,
    ) -> Result<super::super::super::Relational<i64, i64>> {
        let plan = Plan::new(Definition {
            revision: "snapshot-edges".into(),
            sources: vec![Source { id: "rows".into(), schema: "int".into() }],
            nodes: vec![
                Node {
                    id: "source".into(),
                    kind: Kind::Source,
                    inputs: vec!["rows".into()],
                    schema: "int".into(),
                },
                Node { id: "output".into(), kind, inputs, schema: "int".into() },
            ],
            arrangements: vec![Arrangement {
                id: "state".into(),
                node: "output".into(),
                schema: "int".into(),
            }],
            outputs: vec!["output".into()],
        })?;
        let settings = Settings {
            store: Arc::new(object_store::memory::InMemory::new()),
            block_rows: 4096,
            limits: Limits { workspace_slots: 1, ..limits },
            encoded_cache: None,
        };
        super::super::super::Relational::new(plan, operators, &settings)
    }
    #[tokio::test]
    async fn repeated_union_edges_preserve_full_tuple_weight() -> Result<()> {
        let query = query(
            Kind::Union,
            vec!["source".into(), "source".into()],
            BTreeMap::new(),
            Limits::default(),
        )?;
        let mut workspace = query.bootstrap_workspace(64 * 1024 * 1024)?;
        workspace.append(vec![Batch::from_updates([((1, 2), 1)])?]).await?;
        let work = query.prepare_bootstrap_unprotected(workspace).await?;
        assert_eq!(work.output().batch, Batch::from_updates([((1, 2), 2)])?);
        assert_eq!(query.time(), 0);
        Ok(())
    }
    #[tokio::test]
    async fn duplicate_fanout_budget_spans_all_snapshot_chunks() -> Result<()> {
        for limit in [5, 6] {
            let operators = BTreeMap::from([(
                "output".into(),
                Operator::Expand(Expand::new(|_, _, emit| {
                    emit((1, 2))?;
                    emit((1, 2))?;
                    emit((1, 2))
                })),
            )]);
            let query = query(
                Kind::Expand,
                vec!["source".into()],
                operators,
                Limits { contributions: limit, ..Limits::default() },
            )?;
            let mut workspace = query.bootstrap_workspace(64 * 1024 * 1024)?;
            workspace.append(vec![Batch::from_updates([((1, 1), 1)])?]).await?;
            let result = workspace.append(vec![Batch::from_updates([((2, 2), 1)])?]).await;
            if limit == 5 {
                assert!(result.is_err());
                assert!(query.prepare_bootstrap_unprotected(workspace).await.is_err());
            } else {
                result?;
                let work = query.prepare_bootstrap_unprotected(workspace).await?;
                assert_eq!(work.output().batch, Batch::from_updates([((1, 2), 6)])?);
            }
            assert_eq!(query.time(), 0);
        }
        Ok(())
    }
}
