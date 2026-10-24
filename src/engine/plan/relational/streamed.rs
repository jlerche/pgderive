//! Evaluate sealed edges once; transport chunks never become logical ticks.
use super::{Execution, Operator, RelationalState};
use crate::engine::{
    Batch,
    dataflow::{Join, TimedBatch},
    execution::{Consolidator, Sealed, group},
    reader::{BatchCursor, BatchData, BatchReader},
};
use anyhow::{Context, Result};
type BinaryEdges<'a, K, V> = TimedBatch<(&'a Sealed<K, V>, &'a Sealed<K, V>)>;
impl<K: BatchData, V: BatchData> Execution<K, V> {
    pub(super) async fn unary_sealed(
        &self,
        node: &super::super::Node,
        prior: &RelationalState<K, V>,
        input: &Sealed<K, V>,
        time: u64,
    ) -> Result<Sealed<K, V>> {
        let mut work = crate::engine::dataflow::PartitionWork::new(self.limits.contributions);
        match self.operators.get(&node.id).context("missing unary callback")? {
            Operator::Project(operator) => {
                return operator.evaluate_sealed(input, self.limits).await;
            }
            Operator::Expand(operator) => {
                return operator.evaluate_sealed(input, self.limits).await;
            }
            Operator::Statistics(operator) => {
                return operator
                    .evaluate_sealed(input, self.prior(&node.id, prior)?, self.limits)
                    .await;
            }
            // Prefix summaries can depend on several navigation keys. Keep their
            // complete logical input together under their explicit existing limit.
            Operator::IndexedStatistics(operator) => {
                return Ok(operator
                    .evaluate(
                        &TimedBatch { time, batch: input.materialize(self.limits).await? },
                        self.prior(&node.id, prior)?,
                        self.limits,
                    )
                    .await?
                    .batch
                    .into());
            }
            _ => {}
        }
        let mut cursor = input.cursor().await?;
        let mut output = Consolidator::new(self.limits)?;
        while cursor.current().is_some() {
            let batch = group(&mut cursor, self.limits).await?;
            self.unary_key(node, prior, TimedBatch { time, batch }, (&mut work, &mut output))
                .await?;
        }
        output.seal()
    }
    async fn unary_key(
        &self,
        node: &super::super::Node,
        prior: &RelationalState<K, V>,
        input: TimedBatch<Batch<K, V>>,
        resources: crate::engine::dataflow::WorkOutput<'_, K, V>,
    ) -> Result<()> {
        let before = self.prior(&node.inputs[0], prior)?;
        match self.operators.get(&node.id).context("missing unary callback")? {
            Operator::Group(operator) => {
                operator.accumulate(&input, before, self.limits, resources).await
            }
            Operator::Ordered(operator) => {
                operator
                    .accumulate(
                        &input,
                        (before, self.prior(&node.id, prior)?),
                        self.limits,
                        resources,
                    )
                    .await
            }
            Operator::Partition(operator) => {
                let retained = if operator.supports_retained_output() {
                    Some(self.prior(&node.id, prior)?)
                } else {
                    None
                };
                operator.accumulate(&input, (before, retained), self.limits, resources).await
            }
            _ => anyhow::bail!("unexpected key-local callback"),
        }
    }
    pub(super) async fn join_sealed(
        &self,
        node: &super::super::Node,
        prior: &RelationalState<K, V>,
        input: BinaryEdges<'_, K, V>,
    ) -> Result<Sealed<K, V>> {
        let TimedBatch { time, batch: (left, right) } = input;
        let mut left = left.cursor().await?;
        let mut right = right.cursor().await?;
        let mut joined = Consolidator::new(self.limits)?;
        loop {
            let key = left
                .current()
                .map(|(key, _, _)| key)
                .into_iter()
                .chain(right.current().map(|(key, _, _)| key))
                .min()
                .cloned();
            let Some(key) = key else {
                break;
            };
            let l = if left.current().is_some_and(|(found, _, _)| *found == key) {
                group(&mut left, self.limits).await?
            } else {
                Batch::default()
            };
            let r = if right.current().is_some_and(|(found, _, _)| *found == key) {
                group(&mut right, self.limits).await?
            } else {
                Batch::default()
            };
            Join.accumulate(
                (&TimedBatch { time, batch: l }, &TimedBatch { time, batch: r }),
                (self.prior(&node.inputs[0], prior)?, self.prior(&node.inputs[1], prior)?),
                (self.limits, &mut joined),
            )
            .await?;
        }
        let Some(Operator::Join(map)) = self.operators.get(&node.id) else {
            anyhow::bail!("missing join callback");
        };
        map.evaluate_sealed(&joined.seal()?, self.limits).await
    }
    pub(super) async fn union_sealed(
        &self,
        left: &Sealed<K, V>,
        right: &Sealed<K, V>,
    ) -> Result<Sealed<K, V>> {
        let mut output = Consolidator::new(self.limits)?;
        for input in [left, right] {
            let mut cursor = input.cursor().await?;
            while let Some((key, value, weight)) = cursor.current() {
                output.add((key.clone(), value.clone()), weight)?;
                cursor.advance().await?;
            }
        }
        output.seal()
    }
    pub(super) async fn validate_sealed(
        &self,
        prior: &crate::engine::trace::TraceSnapshot<K, V>,
        input: &Sealed<K, V>,
        bag: bool,
    ) -> Result<()> {
        let mut cursor = input.cursor().await?;
        while cursor.current().is_some() {
            let mut batch = Consolidator::new(self.limits)?;
            let mut count = 0;
            while let Some((key, value, weight)) = cursor.current() {
                batch.add((key.clone(), value.clone()), weight)?;
                cursor.advance().await?;
                count += 1;
                if count == self.limits.resident_entries.min(1024) {
                    break;
                }
            }
            let batch = batch.finish_batch()?;
            if bag {
                prior.validate_bag_delta(&batch).await?;
            } else {
                prior.validate_delta(&batch).await?;
            }
        }
        Ok(())
    }
}
