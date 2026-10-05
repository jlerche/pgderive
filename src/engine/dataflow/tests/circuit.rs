use super::{Circuit, CircuitBuilder, NodeOutput, StateUpdate, Stream, TimedBatch};
use crate::engine::plan::{Arrangement, Binding, Definition, Kind, Node, Plan, Source, State};
use anyhow::{Result, ensure};
use std::sync::Arc;

#[derive(Clone, Default)]
struct Snapshot {
    value: i64,
    time: u64,
}
impl State for Snapshot {
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding { id: "state".into(), schema: "state-v1".into(), time: self.time }]
    }
}
fn plan(branches: bool) -> Result<Plan> {
    let mut nodes = vec![Node {
        id: "a".into(),
        kind: Kind::Source,
        inputs: vec!["rows".into()],
        schema: "number-v1".into(),
    }];
    if branches {
        nodes.extend(
            [
                ("project", Kind::Project, vec!["a"]),
                ("other", Kind::Project, vec!["a"]),
                ("join", Kind::Join, vec!["project", "project"]),
                ("final", Kind::Project, vec!["join"]),
            ]
            .into_iter()
            .map(|(id, kind, inputs)| Node {
                id: id.into(),
                kind,
                inputs: inputs.into_iter().map(Into::into).collect(),
                schema: format!("{id}-v1"),
            }),
        );
    }
    Plan::new(Definition {
        revision: "typed-test-v1".into(),
        sources: vec![Source { id: "rows".into(), schema: "number-v1".into() }],
        nodes,
        arrangements: vec![Arrangement {
            id: "state".into(),
            node: if branches { "project" } else { "a" }.into(),
            schema: "state-v1".into(),
        }],
        outputs: if branches { vec!["final".into(), "other".into()] } else { vec!["a".into()] },
    })
}
type Branches = (Circuit<Snapshot, (i64, String)>, Stream<i64>, Stream<i64>);
fn branches(reverse: bool) -> Result<Branches> {
    let mut builder = CircuitBuilder::<Snapshot>::new(plan(true)?);
    let source = builder.source::<i64>("a")?;
    // Bind independent nodes in the reverse of the declaration's order.
    let other = builder.unary("other", &source, |context, input| async move {
        Ok(NodeOutput::pure(format!("{}:{}", input.batch, context.prior.value)))
    })?;
    let project = builder.unary("project", &source, |context, input| async move {
        let next = context.prior.value + *input.batch;
        Ok(NodeOutput::staged(*input.batch, move |state: &mut Snapshot| {
            state.value = next;
            state.time = context.time;
        }))
    })?;
    let joined = builder.binary("join", (&project, &project), |_, input| async move {
        // Fan-out consumers share the exact same immutable value, evaluated once.
        ensure!(Arc::ptr_eq(&input.batch.0, &input.batch.1), "fan-out copied value");
        Ok(NodeOutput::pure(*input.batch.0 * *input.batch.1))
    })?;
    let output = builder.unary("final", &joined, |context, input| async move {
        ensure!(*input.batch != 25, "downstream failure");
        Ok(NodeOutput::pure(*input.batch + context.prior.value))
    })?;
    let selection = if reverse {
        // Keep the Rust tuple type while deliberately selecting the wrong edge first.
        project.output().zip(other.output())?
    } else {
        output.output().zip(other.output())?
    };
    Ok((builder.build(selection)?, source, project))
}
#[tokio::test]
async fn fan_out_multiple_outputs_prior_isolation_failure_and_retry() -> Result<()> {
    let (circuit, source, project) = branches(false)?;
    let circuit = Arc::new(circuit);
    assert_eq!(circuit.plan().identity(), plan(true)?.identity());
    let mut engine = circuit.clone().engine(Snapshot::default())?;
    let mut inputs = circuit.inputs();
    assert!(inputs.insert(&project, 1).is_err());
    inputs.insert(&source, 3)?;
    assert!(inputs.insert(&source, 7).is_err());
    let first = engine.prepare(TimedBatch { time: 1, batch: inputs }).await?;
    assert_eq!(engine.time(), 0);
    assert_eq!(engine.snapshot().value, 0);
    assert_eq!(first.output().batch, (9, "3:0".into()));
    engine.commit(first)?;
    let pinned = engine.snapshot();
    let mut inputs = circuit.inputs();
    inputs.insert(&source, 5)?;
    assert!(engine.prepare(TimedBatch { time: 2, batch: inputs }).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &engine.snapshot()));
    assert_eq!(pinned.value, 3);
    let mut retry = circuit.inputs();
    retry.insert(&source, 0)?;
    let prepared = engine.prepare(TimedBatch { time: 2, batch: retry }).await?;
    assert_eq!(prepared.output().batch, (3, "0:3".into()));
    engine.commit(prepared)?;
    assert_eq!(engine.time(), 2);
    assert_eq!(engine.snapshot().value, 3);
    assert!(branches(true).is_err());
    Ok(())
}
type SourceCircuit = (Circuit<Snapshot, i64>, Stream<i64>);
fn source_circuit() -> Result<SourceCircuit> {
    let mut builder = CircuitBuilder::<Snapshot>::new(plan(false)?);
    let source = builder.source_with("a", |context, input: Arc<i64>| async move {
        ensure!(*input != -1, "source staging failure");
        let next = context.prior.value + *input;
        Ok(StateUpdate::new(move |state: &mut Snapshot| {
            state.time = context.time;
            state.value = next;
        }))
    })?;
    Ok((builder.build(source.output())?, source))
}
#[tokio::test]
async fn sources_reject_incomplete_foreign_ticks_and_stage_without_publishing() -> Result<()> {
    let (circuit, source) = source_circuit()?;
    let circuit = Arc::new(circuit);
    let engine = circuit.clone().engine(Snapshot::default())?;
    let pinned = engine.snapshot();
    assert!(engine.prepare(TimedBatch { time: 1, batch: circuit.inputs() }).await.is_err());
    let (other, foreign) = source_circuit()?;
    let mut inputs = circuit.inputs();
    assert!(inputs.insert(&foreign, 4).is_err());
    assert!(engine.prepare(TimedBatch { time: 1, batch: other.inputs() }).await.is_err());
    inputs.insert(&source, -1)?;
    assert!(engine.prepare(TimedBatch { time: 1, batch: inputs }).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &engine.snapshot()));
    let mut inputs = circuit.inputs();
    inputs.insert(&source, 4)?;
    let prepared = engine.prepare(TimedBatch { time: 1, batch: inputs }).await?;
    assert_eq!(prepared.candidate().value, 4);
    assert_eq!(engine.snapshot().value, 0);
    let mut input = circuit.inputs();
    input.insert(&source, 0)?;
    assert!(circuit.evaluate(pinned.clone(), TimedBatch { time: 0, batch: input }).await.is_err());
    let mut input = circuit.inputs();
    input.insert(&source, 0)?;
    assert!(circuit.evaluate(pinned, TimedBatch { time: 2, batch: input }).await.is_err());
    Ok(())
}
#[test]
fn builder_rejects_mismatched_graph_bindings() -> Result<()> {
    let mut builder = CircuitBuilder::<Snapshot>::new(plan(true)?);
    let source = builder.source::<i64>("a")?;
    assert!(builder.source::<i64>("a").is_err());
    assert!(builder.source::<i64>("missing").is_err());
    assert!(builder.source::<i64>("project").is_err());
    let (_, foreign) = source_circuit()?;
    assert!(source.output().zip(foreign.output()).is_err());
    assert!(
        builder.unary("project", &foreign, |_, _| async { Ok(NodeOutput::pure(1_i64)) }).is_err()
    );
    assert!(builder.unary("join", &source, |_, _| async { Ok(NodeOutput::pure(1_i64)) }).is_err());
    assert!(
        builder
            .binary("project", (&source, &source), |_, _| async { Ok(NodeOutput::pure(1_i64)) })
            .is_err()
    );
    assert!(builder.build(source.output()).is_err());
    Ok(())
}
#[tokio::test]
async fn candidate_state_must_cover_every_registered_arrangement_at_the_tick() -> Result<()> {
    let mut builder = CircuitBuilder::<Snapshot>::new(plan(false)?);
    let source = builder.source::<i64>("a")?; // Deliberately forget its arrangement update.
    let circuit = Arc::new(builder.build(source.output())?);
    let engine = circuit.clone().engine(Snapshot::default())?;
    let mut inputs = circuit.inputs();
    inputs.insert(&source, 1)?;
    assert!(engine.prepare(TimedBatch { time: 1, batch: inputs }).await.is_err());
    assert_eq!(engine.time(), 0);
    Ok(())
}
