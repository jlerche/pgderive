use crate::{
    catalog::Deltas,
    compiler::{
        Compiled, Projected,
        relational::{Node, Relational, field},
    },
    engine::{
        Batch,
        dataflow::{Project, TimedBatch},
        plan::{
            self, Plan,
            query::Settings,
            relational::{Operators, Relational as Circuit},
        },
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
pub(in crate::worker) type Key = Vec<String>;
type Inputs = TimedBatch<Vec<Batch<Key, Row>>>;
pub(in crate::worker) type Query = Circuit<Key, Row>;
pub(in crate::worker) fn build(
    contract: &Contract,
    compiled: &Compiled,
    settings: &Settings,
) -> Result<Query> {
    let ir = compiled.relational().context("expected relational program")?;
    let mut operators = Operators {
        projects: BTreeMap::new(),
        expansions: BTreeMap::new(),
        joins: BTreeMap::new(),
        partitions: BTreeMap::new(),
        statistics: BTreeMap::new(),
    };
    for node in &ir.nodes {
        match node {
            Node::Source { .. } => {}
            Node::Expand { id, series, .. } => {
                operators.expansions.insert(id.clone(), Arc::new(expand(series, compiled)));
            }
            Node::Filter { id, predicate, .. } => {
                operators.projects.insert(id.clone(), Arc::new(filter(predicate)));
            }
            Node::Map { id, computed, .. } => {
                operators.projects.insert(id.clone(), Arc::new(map(computed, compiled)));
            }
            Node::KeyBy { id, key, .. } => {
                operators.projects.insert(id.clone(), Arc::new(key_by(key)));
            }
            Node::Join { id, .. } => {
                operators.joins.insert(id.clone(), Arc::new(join_map()));
            }
            Node::PartitionBy { id, keys, .. } => {
                operators.projects.insert(id.clone(), Arc::new(partition_by(keys, compiled)));
            }
            Node::Statistics { id, spec, .. } => {
                operators
                    .statistics
                    .insert(id.clone(), Arc::new(super::statistics::operator(spec)));
            }
            Node::Finalize { id, spec, .. } => {
                operators.projects.insert(id.clone(), Arc::new(super::statistics::finalize(spec)));
            }
            Node::Partition { id, spec, .. } => {
                let spec = spec.clone();
                let limits = settings.limits;
                operators.partitions.insert(
                    id.clone(),
                    Arc::new(crate::engine::dataflow::Partition::new(
                        move |_: &Key, rows: &Batch<(), Row>, work: &mut crate::engine::dataflow::PartitionWork| {
                            super::partition::evaluate(&spec, rows, (limits, work))
                        },
                    )),
                );
            }
            Node::Project { id, .. } | Node::Output { id, .. } => {
                let compiled = compiled.clone();
                let filter = matches!(node, Node::Project { .. });
                let output = ir.output.clone();
                operators.projects.insert(
                    id.clone(),
                    Arc::new(Project::new(move |_: &Key, row: &Row| {
                        if filter && !compiled.qualifies((row, row))? {
                            return Ok(None);
                        }
                        let selected = output
                            .columns
                            .iter()
                            .map(|column| {
                                let name = &column.column.name;
                                Ok((
                                    name.clone(),
                                    row.get(name).context("missing output column")?.clone(),
                                ))
                            })
                            .collect::<Result<Row>>()?;
                        Ok(Some((Vec::new(), selected)))
                    })),
                );
            }
        }
    }
    Query::new(plan(contract, compiled, ir)?, operators, settings)
}

fn filter(predicate: &crate::compiler::Predicate) -> Project<Key, Row, Key, Row> {
    let predicate = predicate.clone();
    Project::new(move |key: &Key, row: &Row| {
        Ok(predicate.qualifies(row)?.then(|| (key.clone(), row.clone())))
    })
}

fn expand(
    series: &crate::compiler::expansion::Series,
    compiled: &Compiled,
) -> crate::engine::dataflow::Expand<Key, Row, Key, Row> {
    let series = series.clone();
    let compiled = compiled.clone();
    crate::engine::dataflow::Expand::new(move |key: &Key, row: &Row, emit| {
        if series.start > series.end || !compiled.qualifies((row, row))? {
            return Ok(());
        }
        for ordinal in series.start..=series.end {
            let mut expanded = row.clone();
            ensure!(
                expanded
                    .insert(crate::compiler::expansion::FIELD.into(), Some(ordinal.to_string()))
                    .is_none(),
                "duplicate generated ordinal"
            );
            emit((key.clone(), expanded))?;
        }
        Ok(())
    })
}

fn join_map() -> Project<Key, (Row, Row), Key, Row> {
    Project::new(|key: &Key, pair: &(Row, Row)| {
        let mut row = pair.0.clone();
        for (name, value) in &pair.1 {
            ensure!(row.insert(name.clone(), value.clone()).is_none(), "duplicate joined field");
        }
        Ok(Some((key.clone(), row)))
    })
}

fn map(
    computed: &[crate::compiler::scalar::Computed],
    compiled: &Compiled,
) -> Project<Key, Row, Key, Row> {
    let computed = computed.to_vec();
    let filter = compiled.clone();
    Project::new(move |key: &Key, row: &Row| {
        if !filter.qualifies((row, row))? {
            return Ok(None);
        }
        let mut mapped = row.clone();
        for value in &computed {
            ensure!(
                mapped.insert(value.column.name.clone(), value.expression.evaluate(row)?).is_none(),
                "duplicate computed field"
            );
        }
        Ok(Some((key.clone(), mapped)))
    })
}
fn key_by(key: &crate::compiler::ColumnRef) -> Project<Key, Row, Key, Row> {
    let name = key.name.clone();
    Project::new(move |_: &Key, row: &Row| {
        Ok(row
            .get(&name)
            .context("missing join column")?
            .as_ref()
            .map(|value| (vec![value.clone()], row.clone())))
    })
}
pub(in crate::worker) fn plan(
    contract: &Contract,
    compiled: &Compiled,
    ir: &Relational,
) -> Result<Plan> {
    contract.validate()?;
    let identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&(contract, compiled))?));
    let schema = |id: &str| format!("worker-relational-v1:{identity}:{id}");
    let nodes = ir
        .nodes
        .iter()
        .map(|node| {
            let (id, kind, inputs) = match node {
                Node::Source { id, source } => {
                    (id, plan::Kind::Source, vec![format!("source_{source}")])
                }
                Node::Filter { id, input, .. }
                | Node::Map { id, input, .. }
                | Node::KeyBy { id, input, .. }
                | Node::Project { id, input }
                | Node::Output { id, input }
                | Node::PartitionBy { id, input, .. }
                | Node::Finalize { id, input, .. } => {
                    (id, plan::Kind::Project, vec![input.clone()])
                }
                Node::Expand { id, input, .. } => (id, plan::Kind::Expand, vec![input.clone()]),
                Node::Statistics { id, input, .. } => {
                    (id, plan::Kind::Statistics, vec![input.clone()])
                }
                Node::Partition { id, input, .. } => {
                    (id, plan::Kind::Aggregate, vec![input.clone()])
                }
                Node::Join { id, left, right } => {
                    (id, plan::Kind::Join, vec![left.clone(), right.clone()])
                }
            };
            plan::Node { id: id.clone(), kind, inputs, schema: schema(id) }
        })
        .collect();
    let sources = ir
        .nodes
        .iter()
        .filter_map(|node| match node {
            Node::Source { id, source } => {
                Some(plan::Source { id: format!("source_{source}"), schema: schema(id) })
            }
            _ => None,
        })
        .collect();
    let mut maintained = BTreeMap::new();
    for node in &ir.nodes {
        if let Node::Statistics { id, input, .. } = node {
            maintained.insert(id.clone(), id.clone());
            maintained.insert(input.clone(), input.clone());
        }
        if let Node::Partition { input, .. } = node {
            maintained.insert(input.clone(), input.clone());
        }
        if let Node::Join { left, right, .. } = node {
            for id in [left, right] {
                maintained.insert(id.clone(), id.clone());
            }
        }
    }
    maintained.insert("output".into(), "project".into());
    let arrangements = maintained
        .into_iter()
        .map(|(id, node)| plan::Arrangement { id, schema: schema(&node), node })
        .collect();
    Plan::new(plan::Definition {
        revision: format!("worker-relational-v1:{identity}"),
        sources,
        nodes,
        arrangements,
        outputs: vec!["project".into()],
    })
}
pub(in crate::worker) fn inputs(
    batch: &weighted::Batch,
    ir: &Relational,
    time: u64,
) -> Result<Inputs> {
    let mut inputs = Vec::new();
    for node in &ir.nodes {
        if let Node::Source { source, .. } = node {
            let relation = ir.sources.get(*source).context("missing relational source")?;
            inputs.push(Batch::from_updates(
                batch
                    .updates
                    .iter()
                    .filter(|update| {
                        update.tuple.schema == relation.schema
                            && update.tuple.table == relation.table
                    })
                    .map(|update| {
                        let row = update
                            .tuple
                            .row
                            .iter()
                            .map(|(name, value)| (field(*source, name), value.clone()))
                            .collect();
                        ((Vec::new(), row), update.weight)
                    }),
            )?);
        }
    }
    Ok(TimedBatch { time, batch: inputs })
}
pub(in crate::worker) fn deltas(batch: &Batch<Key, Row>, output: &Projected) -> Result<Deltas> {
    let updates = batch
        .iter()
        .map(|((_, row), weight)| Ok((((), output.row(row)?), *weight)))
        .collect::<Result<Vec<_>>>()?;
    Deltas::bag(&Batch::from_updates(updates)?)
}

fn partition_by(
    keys: &[crate::compiler::ColumnRef],
    compiled: &Compiled,
) -> Project<Key, Row, Key, Row> {
    let keys = keys.to_vec();
    let compiled = compiled.clone();
    Project::new(move |_: &Key, row: &Row| {
        if !compiled.qualifies((row, row))? {
            return Ok(None);
        }
        let key = keys
            .iter()
            .map(|key| {
                serde_json::to_string(row.get(&key.name).context("missing partition key")?)
                    .map_err(Into::into)
            })
            .collect::<Result<_>>()?;
        Ok(Some((key, row.clone())))
    })
}
