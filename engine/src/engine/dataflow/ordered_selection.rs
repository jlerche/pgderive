//! Retractable ordered selection counting row occurrences, including cutoff weights.
use super::{PartitionWork, Project, TimedBatch};
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::{BatchData, Entry},
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::sync::Arc;
type SelectionPrior<'a, K, V> = (&'a TraceSnapshot<K, V>, &'a TraceSnapshot<K, V>);
type Selection<K, V> = dyn Fn(&K, &V, usize, Limits) -> Result<Option<(K, V)>> + Send + Sync;
type Peer<V> = dyn Fn(&V) -> Result<String> + Send + Sync;
/// Retractable first K row occurrences per key in physical codec order,
/// optionally extended through the cutoff peer group.
/// SQL callers bind explicit ordering and the codec to durable plan identity.
#[derive(Clone)]
pub struct OrderedSelection<K: BatchData, V: BatchData> {
    limit: usize,
    first: bool,
    peer: Option<Arc<Peer<V>>>,
    select: Arc<Selection<K, V>>,
}
impl<K: BatchData, V: BatchData> OrderedSelection<K, V> {
    /// Subtract the first `offset` occurrences from the selected prefix, as SQL
    /// OFFSET does. Complete input-delta membership validation remains required.
    #[must_use]
    pub fn skip(mut self, offset: usize) -> Self {
        let select = self.select;
        self.select = Arc::new(move |key, value, position, limits| {
            if position <= offset { Ok(None) } else { select(key, value, position, limits) }
        });
        self
    }
    /// Include rows with the same peer token as the cutoff occurrence.
    /// Equal tokens must form contiguous runs in the physical ordering.
    #[must_use]
    pub fn with_ties(
        mut self,
        peer: impl Fn(&V) -> Result<String> + Send + Sync + 'static,
    ) -> Self {
        self.first = false;
        self.peer = Some(Arc::new(peer));
        self
    }
    /// Bind a pure selection that preserves the input key and receives a
    /// one-based position for each selected occurrence.
    #[must_use]
    pub fn new(
        limit: usize,
        select: impl Fn(&K, &V, usize) -> Result<(K, V)> + Send + Sync + 'static,
    ) -> Self {
        Self {
            limit,
            first: false,
            peer: None,
            select: Arc::new(move |key, value, position, _| select(key, value, position).map(Some)),
        }
    }
    /// Select one full identity, then optionally project it with unit membership.
    #[must_use]
    pub fn first(project: Project<K, V, K, V>) -> Self {
        Self {
            limit: 1,
            first: true,
            peer: None,
            select: Arc::new(move |key, value, _, limits| {
                let input = TimedBatch {
                    time: 0,
                    batch: Batch::from_updates([((key.clone(), value.clone()), 1)])?,
                };
                let selected = project.evaluate_with_limits(&input, limits)?.batch;
                Ok(selected.iter().next().map(|(tuple, _)| tuple.clone()))
            }),
        }
    }
    /// Differentiate complete synchronized input changes against retained selection.
    /// Prior input is a nonnegative bag and retained output is the exact previous
    /// selection at the same boundary. Successful evaluation preserves this
    /// inductive contract. Cold restore validates its published lineage; unchanged
    /// unselected tails are not scanned. Every changed identity is validated before
    /// selection can stop early, including for a zero cutoff.
    ///
    /// # Errors
    /// Rejects clocks, invalid memberships, I/O, projection and resource failures.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        output: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        let mut work = PartitionWork::new(limits.contributions);
        let mut changes = Consolidator::new(limits)?;
        self.accumulate(input, (prior, output), limits, (&mut work, &mut changes)).await?;
        Ok(TimedBatch { time: input.time, batch: changes.finish_batch()? })
    }
    pub(crate) async fn accumulate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        state: SelectionPrior<'_, K, V>,
        limits: Limits,
        resources: super::WorkOutput<'_, K, V>,
    ) -> Result<()> {
        let (work, changes) = resources;
        let (prior, output) = state;
        ensure!(
            prior.time() == output.time() && prior.time().checked_add(1) == Some(input.time),
            "out-of-order top-k tick"
        );
        limits.check_batch(&input.batch)?;
        prior.validate_bag_delta(&input.batch).await?;
        let entries = input.batch.shared_entries();
        let mut remaining = entries.as_slice();
        let mut progress = Progress { changes, work, limits };
        while let Some(((key, _), _)) = remaining.first() {
            let length = remaining.partition_point(|((candidate, _), _)| candidate == key);
            let (delta, rest) = remaining.split_at(length);
            let mut cursor = output.key_cursor(key).await?;
            let mut retained = 0usize;
            while let Some((_, value, weight)) = cursor.current() {
                if !self.first {
                    progress.work.charge(1)?;
                }
                ensure!(weight > 0, "negative retained top-k membership");
                retained = retained
                    .checked_add(usize::try_from(weight)?)
                    .context("top-k retained count overflow")?;
                ensure!(
                    self.peer.is_some() || retained <= self.limit,
                    "retained top-k exceeds limit"
                );
                progress.changes.add((key.clone(), value.clone()), -weight)?;
                cursor.advance().await?;
            }
            self.selected(prior, key, delta, &mut progress).await?;
            remaining = rest;
        }
        Ok(())
    }
    async fn selected(
        &self,
        prior: &TraceSnapshot<K, V>,
        key: &K,
        delta: &[Entry<K, V>],
        progress: &mut Progress<'_, K, V>,
    ) -> Result<()> {
        let mut cursor = prior.key_cursor(key).await?;
        let mut changed = delta.iter().peekable();
        let mut position = 0;
        let mut cutoff = None;
        while position < self.limit || cutoff.is_some() {
            progress.work.charge(1)?;
            let old = cursor.current().map(|(_, value, weight)| (value, weight));
            let new = changed.peek().map(|((_, value), weight)| (value, *weight));
            let Some(value) = old
                .as_ref()
                .map(|entry| entry.0)
                .into_iter()
                .chain(new.as_ref().map(|entry| entry.0))
                .min()
            else {
                break;
            };
            let mut coefficient = BigInt::from(0);
            let advance_prior = old.as_ref().is_some_and(|entry| entry.0 == value);
            if advance_prior {
                let weight = old.as_ref().context("missing old top-k row")?.1;
                ensure!(weight > 0, "negative prior top-k membership");
                coefficient += weight;
            }
            let advance_changed = new.as_ref().is_some_and(|entry| entry.0 == value);
            if advance_changed {
                coefficient += new.as_ref().context("missing changed top-k row")?.1;
            }
            ensure!(coefficient >= BigInt::from(0), "negative top-k membership");
            let Some(count) = self.count(value, coefficient, position, &mut cutoff)? else {
                break;
            };
            for _ in 0..count {
                if !self.first {
                    progress.work.charge(1)?;
                }
                position += 1;
                if let Some(selected) = (self.select)(key, value, position, progress.limits)? {
                    ensure!(&selected.0 == key, "ordered selection must preserve key");
                    progress.changes.add(selected, 1)?;
                }
            }
            if advance_changed {
                changed.next();
            }
            if advance_prior && (position < self.limit || cutoff.is_some()) {
                cursor.advance().await?;
            }
        }
        Ok(())
    }
    fn count(
        &self,
        value: &V,
        coefficient: BigInt,
        position: usize,
        cutoff: &mut Option<String>,
    ) -> Result<Option<usize>> {
        let remaining = BigInt::from(self.limit.saturating_sub(position));
        if let Some(peer) = &self.peer
            && coefficient >= remaining
            && coefficient != BigInt::from(0)
        {
            let token = peer(value)?;
            if cutoff.as_ref().is_some_and(|prior| *prior != token) {
                return Ok(None);
            }
            *cutoff = Some(token);
            // The ordinary per-occurrence work charge bounds the peer expansion.
            return Ok(Some(usize::try_from(coefficient)?));
        }
        Ok(Some(usize::try_from(coefficient.min(remaining))?))
    }
}
struct Progress<'a, K: BatchData, V: BatchData> {
    changes: &'a mut Consolidator<(K, V)>,
    work: &'a mut PartitionWork,
    limits: Limits,
}
