use super::{Bag, Rows, Snapshot, Writers, definition, recompute};
use crate::engine::{
    Batch,
    dataflow::{
        Arrangement, Circuit, CircuitBuilder, GroupSum, Join, NodeOutput, Project, StateUpdate,
        Stream, SumState, TimedBatch,
    },
    execution::Limits,
    plan::Plan,
    trace::TraceSnapshot,
};
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc};

type Read = fn(&Snapshot) -> &TraceSnapshot<i64, i64>;
type Assign = fn(&mut Snapshot, TraceSnapshot<i64, i64>);
type Streams = (Stream<Rows>, Stream<Rows>, Stream<Rows>);
type Sums = Batch<i64, SumState>;
type Built = (Circuit<Snapshot, Sums>, Streams);
fn source(
    builder: &mut CircuitBuilder<Snapshot>,
    id: &str,
    writer: Arrangement<i64, i64>,
    read: Read,
    assign: Assign,
) -> Result<Stream<Rows>> {
    builder.source_with(id, move |context, delta: Arc<Rows>| {
        let writer = writer.clone();
        async move {
            let next = writer
                .stage(read(&context.prior), &TimedBatch { time: context.time, batch: delta })
                .await?;
            Ok(StateUpdate::new(move |state| assign(state, next)))
        }
    })
}
fn build(writers: &Writers) -> Result<Built> {
    let mut builder = CircuitBuilder::<Snapshot>::new(Plan::new(definition())?);
    let a = source(&mut builder, "a", writers.a.clone(), |s| &s.a, |s, next| s.a = next)?;
    let b = source(&mut builder, "b", writers.b.clone(), |s| &s.b, |s, next| s.b = next)?;
    let c = source(&mut builder, "c", writers.c.clone(), |s| &s.c, |s, next| s.c = next)?;
    let writer = writers.ab.clone();
    let ab = builder.binary("ab", (&a, &b), move |context, input| {
        let writer = writer.clone();
        async move {
            let inputs = (
                TimedBatch { time: input.time, batch: input.batch.0 },
                TimedBatch { time: input.time, batch: input.batch.1 },
            );
            let delta = Join
                .evaluate_with_limits(
                    (&inputs.0, &inputs.1),
                    (&context.prior.a, &context.prior.b),
                    Limits::default(),
                )
                .await?;
            let next = writer.stage(&context.prior.ab, &delta).await?;
            Ok(NodeOutput::staged(delta.batch, move |state: &mut Snapshot| state.ab = next))
        }
    })?;
    let abc = builder.binary("abc", (&ab, &c), |context, input| async move {
        let inputs = (
            TimedBatch { time: input.time, batch: input.batch.0 },
            TimedBatch { time: input.time, batch: input.batch.1 },
        );
        Ok(NodeOutput::pure(
            Join.evaluate_with_limits(
                (&inputs.0, &inputs.1),
                (&context.prior.ab, &context.prior.c),
                Limits::default(),
            )
            .await?
            .batch,
        ))
    })?;
    let projected = builder.unary("project", &abc, |_, input| async move {
        Ok(NodeOutput::pure(
            Project::new(|key: &i64, ((a, b), c): &((i64, i64), i64)| Ok(Some((*key, a + b + c))))
                .evaluate_with_limits(&input, Limits::default())?
                .batch,
        ))
    })?;
    let writer = writers.sum.clone();
    let sum = builder.unary("sum", &projected, move |context, input| {
        let writer = writer.clone();
        async move {
            let delta = GroupSum::new(|_: &i64, value: &i64| Ok(Some(*value)))
                .evaluate_with_limits(&input, &context.prior.sum, Limits::default())
                .await?;
            let next = writer.stage(&context.prior.sum, &delta.state).await?;
            Ok(NodeOutput::staged(delta.state.batch, move |state: &mut Snapshot| state.sum = next))
        }
    })?;
    Ok((builder.build(sum.output())?, (a, b, c)))
}
fn expected(bags: &[Bag; 3]) -> Result<Sums> {
    Batch::from_updates(
        recompute(bags)
            .into_iter()
            .filter(|(_, (rows, _))| *rows != 0)
            .map(|(key, (rows, sum))| ((key, SumState { rows, non_null: rows, sum }), 1)),
    )
}
async fn cold(writers: &Writers, prior: &Snapshot) -> Result<Snapshot> {
    Ok(Snapshot {
        a: writers.a.reopen(prior.a.manifest()?).await?,
        b: writers.b.reopen(prior.b.manifest()?).await?,
        c: writers.c.reopen(prior.c.manifest()?).await?,
        ab: writers.ab.reopen(prior.ab.manifest()?).await?,
        sum: writers.sum.reopen(prior.sum.manifest()?).await?,
    })
}
#[tokio::test]
async fn typed_chained_joins_emit_snapshot_differences_and_restore() -> Result<()> {
    let writers = Writers::new()?;
    let (circuit, sources) = build(&writers)?;
    let circuit = Arc::new(circuit);
    let mut engine = circuit.clone().engine(writers.empty())?;
    let mut bags = [BTreeMap::new(), BTreeMap::new(), BTreeMap::new()];
    for time in 1_i64..=12 {
        let before = expected(&bags)?;
        let (row, weight) = if time <= 6 { (time, 1) } else { (time - 6, -1) };
        let updates = [
            vec![((row % 2, row % 3), weight)],
            vec![((row % 2, row % 4), weight)],
            vec![((row % 2, row % 5), weight)],
        ];
        for (bag, changes) in bags.iter_mut().zip(&updates) {
            for (row, weight) in changes {
                *bag.entry(*row).or_default() += weight;
            }
        }
        let after = expected(&bags)?;
        let delta = Batch::from_updates(
            before
                .iter()
                .map(|(row, w)| (row.clone(), -w))
                .chain(after.iter().map(|(row, w)| (row.clone(), *w))),
        )?;
        let mut inputs = circuit.inputs();
        for (source, changes) in [&sources.0, &sources.1, &sources.2].into_iter().zip(updates) {
            inputs.insert(source, Batch::from_updates(changes)?)?;
        }
        let prepared =
            engine.prepare(TimedBatch { time: u64::try_from(time)?, batch: inputs }).await?;
        ensure!(prepared.output().batch == delta, "wrong relational aggregate delta");
        engine.commit(prepared)?;
        ensure!(engine.snapshot().sum.materialize().await? == after, "wrong integrated output");
        if time == 6 {
            engine = circuit
                .clone()
                .engine_at(cold(&writers, &engine.snapshot()).await?, engine.time())?;
        }
    }
    let mut inputs = circuit.inputs();
    for source in [&sources.0, &sources.1, &sources.2] {
        inputs.insert(source, Batch::from_updates([((0, 99), 1), ((0, 99), -1)])?)?;
    }
    let empty = engine.prepare(TimedBatch { time: 13, batch: inputs }).await?;
    assert!(empty.output().batch.iter().next().is_none());
    engine.commit(empty)?;
    assert_eq!(engine.time(), 13);
    assert!(engine.snapshot().sum.materialize().await?.iter().next().is_none());
    Ok(())
}
