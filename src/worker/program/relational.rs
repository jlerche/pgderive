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
use std::collections::{BTreeMap, BTreeSet};
pub(in crate::worker) type Key = crate::engine::value::Value<Vec<String>>;
pub(in crate::worker) type Inputs = TimedBatch<Vec<crate::engine::execution::Sealed<Key, Row>>>;
pub(in crate::worker) type Query = Circuit<Key, Row>;
#[cfg(test)]
pub(in crate::worker) fn build(
    contract: &Contract,
    compiled: &Compiled,
    settings: &Settings,
) -> Result<Query> {
    let ir = compiled.relational();
    Query::new(plan(contract, compiled, ir)?, operators(ir, settings), settings)
}
/// Bind only an imported `PostgreSQL` snapshot and complete ordered CDC transactions.
/// Generic/manual weighted callers must use `build`, retaining strict validation.
pub(in crate::worker) fn build_committed(
    contract: &Contract,
    compiled: &Compiled,
    settings: &Settings,
) -> Result<Query> {
    let ir = compiled.relational();
    let certified = super::source_keys::statistics(contract, ir)?;
    Query::from_source_keys(
        declared_plan(contract, compiled, ir, &certified)?,
        operators(ir, settings),
        settings,
        certified,
    )
}
pub(in crate::worker) fn committed_plan(contract: &Contract, compiled: &Compiled) -> Result<Plan> {
    let ir = compiled.relational();
    declared_plan(contract, compiled, ir, &super::source_keys::statistics(contract, ir)?)
}
fn operators(ir: &Relational, settings: &Settings) -> Operators<Key, Row> {
    let native = crate::engine::value::NativeBudget::new(settings.limits.native_cache_bytes);
    ir.nodes
        .iter()
        .filter_map(|node| {
            bind(node, &ir.output, settings, &native)
                .map(|operator| (node.id().to_owned(), operator))
        })
        .collect()
}
fn bind(
    node: &Node,
    projected: &Projected,
    settings: &Settings,
    native: &std::sync::Arc<crate::engine::value::NativeBudget>,
) -> Option<Operator<Key, Row>> {
    Some(match node {
        Node::Source { .. } | Node::Union { .. } => return None,
        Node::PrefixInput { spec, .. } => Operator::Project(super::prefix::input(spec)),
        Node::PrefixStatistics { spec, .. } => {
            Operator::IndexedStatistics(super::prefix::statistics(spec, native.clone()))
        }
        Node::PrefixWindow { spec, .. } => {
            Operator::Window(super::prefix::window(spec, native.clone()))
        }
        Node::LookupInput { spec, .. } => Operator::Project(super::lookup::input(spec)),
        Node::Lookup { spec, .. } => Operator::Partition(lookup_operator(spec, settings)),
        Node::Expand { series, .. } => Operator::Expand(expand(series)),
        Node::Filter { predicate, .. } => Operator::Project(filter(predicate)),
        Node::Map { computed, .. } => Operator::Project(map(computed)),
        Node::PartitionBy { keys, .. } => Operator::Project(partition_by(keys, native.clone())),
        Node::Rebind { fields, .. } => Operator::Project(super::bindings::rebind(fields)),
        Node::CrossKey { .. } => Operator::Project(super::bindings::cross_key()),
        Node::EquijoinKey { keys, .. } => Operator::Project(super::bindings::key_by(keys)),
        Node::Join { .. } => Operator::Join(join_map()),
        Node::Partition { spec, .. } => Operator::Partition(partition_operator(spec, settings)),
        Node::Output { .. } => Operator::Project(output(projected)),
        Node::SetDistinct { columns, .. } => {
            Operator::Group(super::group_cursor::distinct(columns))
        }
        Node::RightCounts { .. } => Operator::Statistics(super::membership_counts::counts()),
        Node::RightPresence { .. } => Operator::Project(super::membership_counts::presence()),
        Node::Existence { anti, .. } => Operator::Group(super::group_cursor::existence(*anti)),
        Node::LeftJoinInput { spec, .. } => Operator::Project(super::left_join::input(spec)),
        Node::LeftJoin { spec, .. } => Operator::Group(super::group_cursor::left_join(spec)),
        Node::DistinctInput { .. }
        | Node::DistinctCounts { .. }
        | Node::DistinctMembership { .. }
        | Node::Statistics { .. }
        | Node::Finalize { .. } => return bind_statistics(node, native),
        Node::TopKInput { .. }
        | Node::TopK { .. }
        | Node::OrderedInput { .. }
        | Node::OrderedFirst { .. }
        | Node::OrderedGroup { .. }
        | Node::OrderedFinalize { .. } => return bind_ordered(node),
    })
}
fn bind_statistics(
    node: &Node,
    native: &std::sync::Arc<crate::engine::value::NativeBudget>,
) -> Option<Operator<Key, Row>> {
    Some(match node {
        Node::DistinctInput { spec, .. } => {
            Operator::Expand(super::distinct_statistics::input(spec))
        }
        Node::DistinctCounts { spec, .. } => {
            Operator::Statistics(super::distinct_statistics::counts(spec))
        }
        Node::DistinctMembership { spec, .. } => {
            Operator::Project(super::distinct_statistics::membership(spec))
        }
        Node::Statistics { spec, .. } => {
            Operator::Statistics(super::statistics::operator(spec, native.clone()))
        }
        Node::Finalize { spec, .. } => {
            Operator::Project(super::statistics::finalize(spec, native.clone()))
        }
        _ => return None,
    })
}
fn bind_ordered(node: &Node) -> Option<Operator<Key, Row>> {
    Some(match node {
        Node::TopKInput { spec, .. } => Operator::Project(super::topk::input(spec)),
        Node::TopK { spec, .. } => Operator::Ordered(super::topk::operator(spec)),
        Node::OrderedInput { spec, .. } => Operator::Expand(super::ordered::input(spec)),
        Node::OrderedFirst { spec, .. } => Operator::Ordered(super::ordered::first(spec)),
        Node::OrderedGroup { .. } => Operator::Project(super::ordered::group()),
        Node::OrderedFinalize { spec, .. } => Operator::Partition(super::ordered::finalize(spec)),
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
        Ok(Some((Key::new(), selected)))
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
#[cfg(test)]
pub(in crate::worker) fn plan(
    contract: &Contract,
    compiled: &Compiled,
    ir: &Relational,
) -> Result<Plan> {
    declared_plan(contract, compiled, ir, &BTreeSet::new())
}
fn declared_plan(
    contract: &Contract,
    compiled: &Compiled,
    ir: &Relational,
    certified: &BTreeSet<String>,
) -> Result<Plan> {
    contract.validate()?;
    let identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&(contract, compiled))?));
    let revision =
        if certified.is_empty() { "relational-additive-v2" } else { "source-key-statistics-v2" };
    let Some(Node::Output { id: output, .. }) = ir.nodes.last() else {
        anyhow::bail!("relational program must end in an output node");
    };
    let schema = |id: &str| format!("{revision}:{identity}:{id}");
    let nodes: Vec<_> = ir.nodes.iter().map(|node| node.declaration(&schema)).collect();
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
            plan::Kind::Statistics if certified.contains(&node.id) => {
                maintained.insert(node.id.clone(), node.id.clone());
                &[]
            }
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
fn visit_sources(
    batch: &weighted::Batch,
    ir: &Relational,
    mut visit: impl FnMut(usize, Key, Row, i64) -> Result<()>,
) -> Result<()> {
    for (ordinal, source) in ir
        .nodes
        .iter()
        .filter_map(|node| match node {
            Node::Source { source, .. } => Some(*source),
            _ => None,
        })
        .enumerate()
    {
        let relation = ir.sources.get(source).context("missing relational source")?;
        for update in &batch.updates {
            if update.tuple.schema == relation.schema && update.tuple.table == relation.table {
                let row = update
                    .tuple
                    .row
                    .iter()
                    .filter(|(name, _)| ir.projection[source].contains(*name))
                    .map(|(name, value)| (field(source, name), value.clone()))
                    .collect();
                visit(ordinal, Key::new(), row, update.weight)?;
            }
        }
    }
    Ok(())
}
pub(in crate::worker) fn chunk(
    batch: &weighted::Batch,
    ir: &Relational,
) -> Result<Vec<Batch<Key, Row>>> {
    let count = ir.nodes.iter().filter(|n| matches!(n, Node::Source { .. })).count();
    let mut rows = vec![Vec::new(); count];
    visit_sources(batch, ir, |source, key, row, weight| {
        rows[source].push(((key, row), weight));
        Ok(())
    })?;
    rows.into_iter().map(Batch::from_updates).collect()
}
pub(in crate::worker) struct Input {
    pending: crate::engine::execution::OpenInput<Key, Row>,
}
impl Input {
    pub(in crate::worker) fn new(
        ir: &Relational,
        limits: crate::engine::execution::Limits,
    ) -> Result<Self> {
        let count = ir.nodes.iter().filter(|node| matches!(node, Node::Source { .. })).count();
        Ok(Self { pending: crate::engine::execution::OpenInput::new(count, limits)? })
    }
    pub(in crate::worker) fn append(
        &mut self,
        batch: &weighted::Batch,
        ir: &Relational,
    ) -> Result<()> {
        visit_sources(batch, ir, |source, key, row, weight| {
            self.pending.push(source, key, row, weight)
        })
    }
    pub(in crate::worker) fn seal(self, time: u64) -> Result<Inputs> {
        self.pending.seal(time)
    }
}
#[cfg(test)]
pub(in crate::worker) fn inputs(
    batch: &weighted::Batch,
    ir: &Relational,
    time: u64,
) -> Result<Inputs> {
    let mut input = Input::new(ir, crate::engine::execution::Limits::default())?;
    input.append(batch, ir)?;
    input.seal(time)
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
    native: std::sync::Arc<crate::engine::value::NativeBudget>,
) -> Project<Key, Row, Key, Row> {
    let keys = keys.to_vec();
    let codec = super::keys::GroupCodec::new(native);
    Project::new(move |_: &Key, row: &Row| {
        let values = keys
            .iter()
            .map(|key| {
                row.get(&key.name)
                    .context("missing partition key")?
                    .as_deref()
                    .map(|v| {
                        if key.oid == 1700 {
                            crate::numeric::Numeric::parse(v).map(|v| v.key())
                        } else {
                            Ok(v.to_owned())
                        }
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let key = codec.group(values)?;
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
