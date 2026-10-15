//! Object-backed qualification graphs use the same transaction runtime as execution.
use crate::engine::{
    Batch,
    dataflow::{Arrangement, Graph, Join, Project, Statistics, TimedBatch},
    execution::Limits,
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;

type Input<K, L, R> = TimedBatch<(Batch<K, L>, Batch<K, R>)>;
type Writers<K, L, R, G> = (Arrangement<K, L>, Arrangement<K, R>, Arrangement<G, i64>);
pub type Counts<K, L, R, G> =
    Graph<Snapshot<K, L, R, G>, (Batch<K, L>, Batch<K, R>), Batch<G, i64>>;
#[derive(Clone)]
pub struct Snapshot<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    pub left: TraceSnapshot<K, L>,
    pub right: TraceSnapshot<K, R>,
    pub counts: TraceSnapshot<G, i64>,
    writers: Arc<Writers<K, L, R, G>>,
}
impl<K: BatchData, L: BatchData, R: BatchData, G: BatchData> Snapshot<K, L, R, G> {
    #[cfg(test)]
    pub async fn compact(&self) -> Result<Self> {
        Ok(Self {
            left: self.writers.0.compact(&self.left).await?,
            right: self.writers.1.compact(&self.right).await?,
            counts: self.writers.2.compact(&self.counts).await?,
            writers: self.writers.clone(),
        })
    }
}
pub fn counts<K: BatchData, L: BatchData, R: BatchData, G: BatchData>(
    store: Arc<dyn ObjectStore>,
    schema: &str,
    block_rows: usize,
    group: impl Fn(&K, &L, &R) -> Result<Option<G>> + Send + Sync + 'static,
) -> Result<Counts<K, L, R, G>> {
    anyhow::ensure!(!schema.is_empty(), "empty graph schema");
    let writers = Arc::new((
        Arrangement::new(store.clone(), format!("{schema}:left"), block_rows)?,
        Arrangement::new(store.clone(), format!("{schema}:right"), block_rows)?,
        Arrangement::new(store, format!("{schema}:counts"), block_rows)?,
    ));
    let initial = Snapshot {
        left: writers.0.empty(),
        right: writers.1.empty(),
        counts: writers.2.empty(),
        writers,
    };
    let project = Arc::new(Project::new(move |key, pair: &(L, R)| {
        Ok(group(key, &pair.0, &pair.1)?.map(|key| (key, pair.clone())))
    }));
    let statistics = Arc::new(Statistics::with_measure(
        1,
        |_, _: &(L, R)| Ok(vec![1.into()]),
        |count: &i64| Ok(vec![(*count).into()]),
        |_: &G, values| {
            let count = i64::try_from(values[0].clone()).context("group count overflow")?;
            Ok((count != 0).then_some(count))
        },
    ));
    Ok(Graph::new(initial, move |prior: Arc<Snapshot<K, L, R, G>>, input: Input<K, L, R>| {
        let project = project.clone();
        let statistics = statistics.clone();
        async move {
            let left = TimedBatch { time: input.time, batch: input.batch.0 };
            let right = TimedBatch { time: input.time, batch: input.batch.1 };
            let joined = Join.evaluate(&left, &right, &prior.left, &prior.right).await?;
            let grouped = project.evaluate(&joined)?;
            let output = statistics.evaluate(&grouped, &prior.counts, Limits::default()).await?;
            let next = Snapshot {
                left: prior.writers.0.stage(&prior.left, &left).await?,
                right: prior.writers.1.stage(&prior.right, &right).await?,
                counts: prior.writers.2.stage(&prior.counts, &output).await?,
                writers: prior.writers.clone(),
            };
            Ok((next, output.batch))
        }
    }))
}

pub fn inputs<K: BatchData, L: BatchData, R: BatchData>(
    left: &TimedBatch<Batch<K, L>>,
    right: &TimedBatch<Batch<K, R>>,
) -> Result<Input<K, L, R>> {
    anyhow::ensure!(left.time == right.time, "inconsistent source ticks");
    Ok(TimedBatch { time: left.time, batch: (left.batch.clone(), right.batch.clone()) })
}
