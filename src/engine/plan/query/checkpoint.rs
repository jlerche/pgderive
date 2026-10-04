use super::{Execution, GroupedJoin, Operators, QueryState, Settings};
use crate::engine::{
    plan::{Checkpoint, Membership, Plan},
    reader::BatchData,
};
use anyhow::{Context, Result};
use std::sync::Arc;
impl<K: BatchData, L: BatchData, R: BatchData, G: BatchData> QueryState<K, L, R, G> {
    fn memberships(&self) -> Result<Vec<Membership>> {
        let traces = [
            self.left.manifest()?,
            self.right.manifest()?,
            self.sums.manifest()?,
            self.output.manifest()?,
        ];
        let mut members = ["left", "right", "sums", "output"]
            .into_iter()
            .zip(&self.schemas)
            .zip(traces)
            .map(|((id, schema), trace)| Membership {
                id: id.into(),
                schema: schema.clone(),
                trace,
            })
            .collect::<Vec<_>>();
        members.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(members)
    }
}
impl<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> GroupedJoin<K, A, B, L, R, G, V>
{
    /// Export complete registered coarse membership from the local committed root.
    /// This checkpoint alone does not publish a sink or authorize slot acknowledgement.
    ///
    /// # Errors
    /// Rejects non-object state or a registration/schema/clock mismatch.
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        let checkpoint = Checkpoint {
            version: 1,
            plan_identity: self.plan().identity().into(),
            time: self.time(),
            arrangements: self.snapshot().memberships()?,
        };
        checkpoint.validate(self.plan())?;
        Ok(checkpoint)
    }
    /// Export unpublished memberships only after checking this runtime owns them.
    /// The caller must retain exclusive runtime ownership through durable publication
    /// and subsequent local commit; this method alone does not authorize ACK.
    ///
    /// # Errors
    /// Rejects stale/foreign candidates or incompatible arrangement membership.
    pub fn prepared_checkpoint(
        &self,
        prepared: &super::Prepared<K, L, R, G>,
    ) -> Result<Checkpoint> {
        self.engine.validate_prepared(prepared)?;
        let checkpoint = Checkpoint {
            version: 1,
            plan_identity: self.plan().identity().into(),
            time: prepared.output().time,
            arrangements: prepared.candidate().memberships()?,
        };
        checkpoint.validate(self.plan())?;
        Ok(checkpoint)
    }
    /// Replace local visibility only after cold-validating an authoritative checkpoint.
    /// Recovery invalidates all previously prepared transactions and maintenance.
    ///
    /// # Errors
    /// Rejects incompatible or older boundaries and any missing/corrupt object.
    pub async fn restore_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        checkpoint.validate(self.plan())?;
        anyhow::ensure!(
            checkpoint.time >= self.time(),
            "recovery cannot roll back local logical time"
        );
        let state = self.execution.reopen_state(&checkpoint).await?;
        let restored =
            Self::bind(self.plan().clone(), self.execution.clone(), state, checkpoint.time)?;
        self.engine = restored.engine;
        Ok(())
    }
    /// Reopen all four arrangements from a complete compatible durable checkpoint.
    /// Every root/index/block and logical coefficient is cold-validated first.
    ///
    /// # Errors
    /// Returns incompatible registration, missing/corrupt state, or runtime failures.
    pub async fn reopen(
        plan: Plan,
        operators: Operators<K, A, B, L, R, G, V>,
        settings: Settings,
        checkpoint: Checkpoint,
    ) -> Result<Self> {
        checkpoint.validate(&plan)?;
        let execution = Execution::build(&plan, operators, settings)?;
        let state = execution.reopen_state(&checkpoint).await?;
        Self::bind(plan, execution, state, checkpoint.time)
    }
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
    async fn reopen_state(&self, checkpoint: &Checkpoint) -> Result<QueryState<K, L, R, G>> {
        let trace = |id: &str| {
            checkpoint
                .arrangements
                .iter()
                .find(|member| member.id == id)
                .map(|member| member.trace.clone())
                .context("missing grouped join checkpoint arrangement")
        };
        Ok(QueryState {
            left: self.left.reopen(trace("left")?).await?,
            right: self.right.reopen(trace("right")?).await?,
            sums: self.sums.reopen(trace("sums")?).await?,
            output: self.output.reopen(trace("output")?).await?,
            schemas: self.schemas.clone(),
        })
    }
    pub(super) fn build(
        plan: &Plan,
        operators: Operators<K, A, B, L, R, G, V>,
        settings: Settings,
    ) -> Result<Arc<Self>> {
        super::validate_shape(plan)?;
        let Settings { store, block_rows, limits } = settings;
        let limits = limits.validate()?;
        let schema = |id: &str| {
            plan.definition()
                .arrangements
                .iter()
                .find(|item| item.id == id)
                .map(|item| item.schema.clone())
                .context("missing grouped join arrangement")
        };
        let schemas = [schema("left")?, schema("right")?, schema("sums")?, schema("output")?];
        let cache = Arc::new(crate::engine::reader::BlockCache::new(
            limits.cache_bytes,
            limits.cache_entries,
        ));
        Ok(Arc::new(Self {
            operators,
            left: crate::engine::dataflow::Arrangement::new(
                store.clone(),
                schemas[0].clone(),
                block_rows,
            )?
            .with_cache(cache.clone()),
            right: crate::engine::dataflow::Arrangement::new(
                store.clone(),
                schemas[1].clone(),
                block_rows,
            )?
            .with_cache(cache.clone()),
            sums: crate::engine::dataflow::Arrangement::new(
                store.clone(),
                schemas[2].clone(),
                block_rows,
            )?
            .with_cache(cache.clone()),
            output: crate::engine::dataflow::Arrangement::new(
                store,
                schemas[3].clone(),
                block_rows,
            )?
            .with_cache(cache.clone()),
            schemas,
            limits,
            cache,
        }))
    }
}
