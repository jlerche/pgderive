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
            self, Plan, Settings,
            relational::{Operator, Operators, Relational as Circuit},
        },
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
pub(in crate::worker) type Key = Vec<String>;
type Inputs = TimedBatch<Vec<Batch<Key, Row>>>;
pub(in crate::worker) type Query = Circuit<Key, Row>;
pub(in crate::worker) fn build(
    contract: &Contract,
    compiled: &Compiled,
    settings: &Settings,
) -> Result<Query> {
    let ir = compiled.relational();
    Query::new(plan(contract, compiled, ir)?, operators(ir, settings), settings)
}
type Bound<'a> = (&'a str, Operator<Key, Row>);
fn operators(ir: &Relational, settings: &Settings) -> Operators<Key, Row> {
    ir.nodes
        .iter()
        .filter_map(|node| bind(node, &ir.output, settings))
        .map(|(id, operator)| (id.to_owned(), operator))
        .collect()
}
fn bind<'a>(node: &'a Node, projected: &Projected, settings: &Settings) -> Option<Bound<'a>> {
    Some(match node {
        Node::Source { .. } | Node::Union { .. } => return None,
        Node::PrefixInput { id, spec, .. } => (id, Operator::Project(super::prefix::input(spec))),
        Node::PrefixStatistics { id, spec, .. } => {
            (id, Operator::IndexedStatistics(super::prefix::statistics(spec)))
        }
        Node::PrefixWindow { id, spec, .. } => (id, Operator::Window(super::prefix::window(spec))),
        Node::LookupInput { id, spec, .. } => (id, Operator::Project(super::lookup::input(spec))),
        Node::Lookup { id, spec, .. } => (id, Operator::Partition(lookup_operator(spec, settings))),
        Node::Expand { id, series, .. } => (id, Operator::Expand(expand(series))),
        Node::Filter { id, predicate, .. } => (id, Operator::Project(filter(predicate))),
        Node::Map { id, computed, .. } => (id, Operator::Project(map(computed))),
        Node::PartitionBy { id, keys, .. } => (id, Operator::Project(partition_by(keys))),
        Node::Rebind { id, fields, .. } => (id, Operator::Project(super::bindings::rebind(fields))),
        Node::CrossKey { id, .. } => (id, Operator::Project(super::bindings::cross_key())),
        Node::EquijoinKey { id, keys, .. } => {
            (id, Operator::Project(super::bindings::key_by(keys)))
        }
        Node::Join { id, .. } => (id, Operator::Join(join_map())),
        Node::Partition { id, spec, .. } => {
            (id, Operator::Partition(partition_operator(spec, settings)))
        }
        Node::Output { id, .. } => (id, Operator::Project(output(projected))),
        Node::SetDistinct { id, columns, .. } => {
            (id, Operator::Group(super::group_cursor::distinct(columns)))
        }
        Node::RightCounts { id, .. } => {
            (id, Operator::Statistics(super::membership_counts::counts()))
        }
        Node::RightPresence { id, .. } => {
            (id, Operator::Project(super::membership_counts::presence()))
        }
        Node::Existence { id, anti, .. } => {
            (id, Operator::Group(super::group_cursor::existence(*anti)))
        }
        Node::LeftJoinInput { id, spec, .. } => {
            (id, Operator::Project(super::left_join::input(spec)))
        }
        Node::LeftJoin { id, spec, .. } => {
            (id, Operator::Group(super::group_cursor::left_join(spec)))
        }
        Node::DistinctInput { .. }
        | Node::DistinctCounts { .. }
        | Node::DistinctMembership { .. }
        | Node::Statistics { .. }
        | Node::Finalize { .. } => return bind_statistics(node),
        Node::TopKInput { .. }
        | Node::TopK { .. }
        | Node::OrderedInput { .. }
        | Node::OrderedFirst { .. }
        | Node::OrderedGroup { .. }
        | Node::OrderedFinalize { .. } => return bind_ordered(node),
    })
}
fn bind_statistics(node: &Node) -> Option<Bound<'_>> {
    Some(match node {
        Node::DistinctInput { id, spec, .. } => {
            (id, Operator::Expand(super::distinct_statistics::input(spec)))
        }
        Node::DistinctCounts { id, spec, .. } => {
            (id, Operator::Statistics(super::distinct_statistics::counts(spec)))
        }
        Node::DistinctMembership { id, spec, .. } => {
            (id, Operator::Project(super::distinct_statistics::membership(spec)))
        }
        Node::Statistics { id, spec, .. } => {
            (id, Operator::Statistics(super::statistics::operator(spec)))
        }
        Node::Finalize { id, spec, .. } => {
            (id, Operator::Project(super::statistics::finalize(spec)))
        }
        _ => return None,
    })
}
fn bind_ordered(node: &Node) -> Option<Bound<'_>> {
    Some(match node {
        Node::TopKInput { id, spec, .. } => (id, Operator::Project(super::topk::input(spec))),
        Node::TopK { id, spec, .. } => (id, Operator::Ordered(super::topk::operator(spec))),
        Node::OrderedInput { id, spec, .. } => (id, Operator::Expand(super::ordered::input(spec))),
        Node::OrderedFirst { id, spec, .. } => (id, Operator::Ordered(super::ordered::first(spec))),
        Node::OrderedGroup { id, .. } => (id, Operator::Project(super::ordered::group())),
        Node::OrderedFinalize { id, spec, .. } => {
            (id, Operator::Partition(super::ordered::finalize(spec)))
        }
        _ => return None,
    })
}
fn partition_operator(
    spec: &crate::compiler::partition::Partition,
    settings: &Settings,
) -> crate::engine::dataflow::Partition<Key, Row> {
    let spec = spec.clone();
    let limits = settings.limits;
    crate::engine::dataflow::Partition::new_zero_preserving(
        move |_: &Key, rows: &Batch<(), Row>, work| {
            super::partition::evaluate(&spec, rows, (limits, work))
        },
    )
}
fn lookup_operator(
    spec: &crate::compiler::lookup::Lookup,
    settings: &Settings,
) -> crate::engine::dataflow::Partition<Key, Row> {
    let spec = spec.clone();
    let limits = settings.limits;
    crate::engine::dataflow::Partition::new_zero_preserving(
        move |_: &Key, rows: &Batch<(), Row>, work| {
            super::lookup::evaluate(&spec, rows, limits, work)
        },
    )
}

fn output(output: &Projected) -> Project<Key, Row, Key, Row> {
    let output = output.clone();
    Project::new(move |_: &Key, row: &Row| {
        let selected = output
            .columns
            .iter()
            .map(|column| {
                let name = &column.column.name;
                Ok((name.clone(), row.get(name).context("missing output column")?.clone()))
            })
            .collect::<Result<Row>>()?;
        Ok(Some((Vec::new(), selected)))
    })
}

fn filter(predicate: &crate::compiler::Predicate) -> Project<Key, Row, Key, Row> {
    let predicate = predicate.clone();
    Project::new(move |key: &Key, row: &Row| {
        Ok(predicate.qualifies(row)?.then(|| (key.clone(), row.clone())))
    })
}

fn expand(
    series: &crate::compiler::expansion::Series,
) -> crate::engine::dataflow::Expand<Key, Row, Key, Row> {
    let series = series.clone();
    crate::engine::dataflow::Expand::new(move |key: &Key, row: &Row, emit| {
        if series.start > series.end {
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

fn map(computed: &[crate::compiler::scalar::Computed]) -> Project<Key, Row, Key, Row> {
    let computed = computed.to_vec();
    Project::new(move |key: &Key, row: &Row| {
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
fn declaration(node: &Node, schema: &impl Fn(&str) -> String) -> plan::Node {
    let (id, kind, inputs) = match node {
        Node::Source { id, source } => (id, plan::Kind::Source, vec![format!("source_{source}")]),
        Node::Filter { id, input, .. }
        | Node::Map { id, input, .. }
        | Node::Rebind { id, input, .. }
        | Node::CrossKey { id, input }
        | Node::EquijoinKey { id, input, .. }
        | Node::Output { id, input }
        | Node::PartitionBy { id, input, .. }
        | Node::Finalize { id, input, .. }
        | Node::TopKInput { id, input, .. }
        | Node::LeftJoinInput { id, input, .. }
        | Node::LookupInput { id, input, .. }
        | Node::OrderedGroup { id, input }
        | Node::DistinctMembership { id, input, .. }
        | Node::PrefixInput { id, input, .. }
        | Node::RightPresence { id, input } => (id, plan::Kind::Project, vec![input.clone()]),
        Node::Union { id, left, right } => {
            (id, plan::Kind::Union, vec![left.clone(), right.clone()])
        }
        Node::Expand { id, input, .. }
        | Node::OrderedInput { id, input, .. }
        | Node::DistinctInput { id, input, .. } => (id, plan::Kind::Expand, vec![input.clone()]),
        Node::Statistics { id, input, .. }
        | Node::DistinctCounts { id, input, .. }
        | Node::PrefixStatistics { id, input, .. }
        | Node::RightCounts { id, input } => (id, plan::Kind::Statistics, vec![input.clone()]),
        Node::OrderedFirst { id, input, .. } | Node::TopK { id, input, .. } => {
            (id, plan::Kind::Ordered, vec![input.clone()])
        }
        Node::OrderedFinalize { id, input, .. }
        | Node::Partition { id, input, .. }
        | Node::LeftJoin { id, input, .. }
        | Node::SetDistinct { id, input, .. }
        | Node::Existence { id, input, .. }
        | Node::Lookup { id, input, .. } => (id, plan::Kind::Aggregate, vec![input.clone()]),
        Node::Join { id, left, right } => (id, plan::Kind::Join, vec![left.clone(), right.clone()]),
        Node::PrefixWindow { id, left, right, .. } => {
            (id, plan::Kind::Window, vec![left.clone(), right.clone()])
        }
    };
    plan::Node { id: id.clone(), kind, inputs, schema: schema(id) }
}
pub(in crate::worker) fn plan(
    contract: &Contract,
    compiled: &Compiled,
    ir: &Relational,
) -> Result<Plan> {
    contract.validate()?;
    let identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&(contract, compiled))?));
    let revision = "relational";
    let Some(Node::Output { id: output, .. }) = ir.nodes.last() else {
        anyhow::bail!("relational program must end in an output node");
    };
    let schema = |id: &str| format!("{revision}:{identity}:{id}");
    let nodes: Vec<_> = ir.nodes.iter().map(|node| declaration(node, &schema)).collect();
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
    for node in &nodes {
        let inputs = match node.kind {
            plan::Kind::Join | plan::Kind::Window => node.inputs.as_slice(),
            plan::Kind::Statistics | plan::Kind::Ordered | plan::Kind::Aggregate => {
                maintained.insert(node.id.clone(), node.id.clone());
                node.inputs.as_slice()
            }
            _ => continue,
        };
        for id in inputs {
            maintained.insert(id.clone(), id.clone());
        }
    }
    maintained.insert("output".into(), output.clone());
    let arrangements = maintained
        .into_iter()
        .map(|(id, node)| plan::Arrangement { id, schema: schema(&node), node })
        .collect();
    Plan::new(plan::Definition {
        revision: format!("{revision}:{identity}"),
        sources,
        nodes,
        arrangements,
        outputs: vec![output.clone()],
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

fn partition_by(keys: &[crate::compiler::ColumnRef]) -> Project<Key, Row, Key, Row> {
    let keys = keys.to_vec();
    Project::new(move |_: &Key, row: &Row| {
        let key = keys
            .iter()
            .map(|key| {
                serde_json::to_string(
                    &row.get(&key.name)
                        .context("missing partition key")?
                        .as_deref()
                        .map(|v| {
                            if key.oid == 1700 {
                                crate::numeric::Numeric::parse(v).map(|v| v.key())
                            } else {
                                Ok(v.to_owned())
                            }
                        })
                        .transpose()?,
                )
                .map_err(Into::into)
            })
            .collect::<Result<_>>()?;
        Ok(Some((key, row.clone())))
    })
}

#[cfg(test)]
pub(in crate::worker) fn build_scanned(
    contract: &Contract,
    compiled: &Compiled,
    settings: &Settings,
) -> Result<Query> {
    if compiled.relational.nodes.iter().any(|node| matches!(node, Node::PrefixWindow { .. })) {
        return build_scanned(contract, &scan_prefix(compiled), settings);
    }
    let ir = compiled.relational();
    let mut operators = operators(ir, settings);
    for node in &ir.nodes {
        if let Node::Partition { id, spec, .. } = node {
            let spec = spec.clone();
            let limits = settings.limits;
            operators.insert(
                id.clone(),
                Operator::Partition(crate::engine::dataflow::Partition::new_zero_preserving(
                    move |_, rows, work| {
                        super::partition::evaluate_scan(&spec, rows, (limits, work))
                    },
                )),
            );
        }
    }
    for node in &ir.nodes {
        let (id, operator) = match node {
            Node::SetDistinct { id, columns, .. } => (id, super::membership::distinct(columns)),
            Node::Existence { id, anti, .. } => {
                (id, super::membership::existence(*anti, settings.limits))
            }
            Node::LeftJoin { id, spec, .. } => {
                (id, super::left_join::operator(spec, settings.limits))
            }
            _ => continue,
        };
        operators.insert(id.clone(), Operator::Partition(operator));
    }
    Query::new(plan(contract, compiled, ir)?, operators, settings)
}

#[cfg(test)]
pub(in crate::worker) fn scan_prefix(compiled: &Compiled) -> Compiled {
    let mut baseline = compiled.clone();
    let inputs = baseline
        .relational
        .nodes
        .iter()
        .filter_map(|node| {
            if let Node::PrefixInput { id, input, .. } = node {
                Some((id.clone(), input.clone()))
            } else {
                None
            }
        })
        .collect::<BTreeMap<_, _>>();
    baseline.relational.nodes = baseline
        .relational
        .nodes
        .into_iter()
        .filter_map(|node| match node {
            Node::PrefixInput { .. } | Node::PrefixStatistics { .. } => None,
            Node::PrefixWindow { id, left, spec, .. } => Some(Node::Partition {
                id,
                input: inputs.get(&left).cloned().unwrap_or(left),
                spec,
            }),
            other => Some(other),
        })
        .collect();
    baseline
}
