use super::{ColumnRef, Compiled, resolve, scope};
use crate::{
    compiler::{
        parser::partition as syntax,
        partition::{Aggregate, Function, Mode, Order, Partition},
        projection::OutputColumn,
        relational::{Node, Relational, Source, field},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
mod argument;
pub(in crate::compiler::bind) mod composition;
mod distinct;
mod grouping;
mod having;
mod ordered;
mod prefix;
mod range;
mod windows;
#[derive(Clone)]
struct Body {
    expansion: Option<crate::compiler::parser::expansion::Series>,
    targets: Vec<syntax::Target>,
    visible: usize,
    groups: Vec<crate::compiler::parser::scalar::Key>,
    predicate: Option<crate::compiler::syntax::Expr>,
    having: Option<crate::compiler::syntax::Expr>,
}
#[derive(Clone)]
struct Stage<'a> {
    contract: &'a Contract,
    unit: bool,
    sources: Vec<Source>,
    occurrence_keys: &'a crate::compiler::properties::Properties,
    aggregate_base: usize,
    scalar_scope: Option<usize>,
}
pub(super) fn bind(parsed: syntax::Parsed, contract: &Contract) -> Result<Compiled> {
    let syntax::Parsed { source, expansion, targets, visible, groups, predicate, having } = parsed;
    let body = Body { expansion, targets, visible, groups, predicate, having };
    match source {
        syntax::Source::Unit => {
            let relation = contract
                .relations
                .first()
                .context("constant query requires a publication driver")?;
            let facts = crate::compiler::properties::Analysis::source_relation(relation, 0)?;
            build(
                &body,
                &|_| Err(super::UnknownColumn("column reference requires FROM".into()).into()),
                Stage {
                    contract,
                    unit: true,
                    sources: vec![Source {
                        schema: relation.schema.clone(),
                        table: relation.table.clone(),
                    }],
                    occurrence_keys: &facts,
                    aggregate_base: 0,
                    scalar_scope: None,
                },
            )
        }
        syntax::Source::Native(table) => bind_native(table, &body, contract),
        syntax::Source::Join(tree) => {
            let scope = super::native_join::scope(tree, contract)?;
            let mut outer =
                composition::stage(&scope.ir, &body, &|name| scope.resolve(name), contract, None)?;
            let mut relational = scope.ir;
            composition::compose(&mut relational, &mut outer)?;
            Ok(Compiled { result_columns: Vec::new(), relational })
        }
        syntax::Source::Derived { inner, alias } => {
            composition::bind(*inner, &alias, &body, contract)
        }
    }
}
fn bind_native(
    table: crate::compiler::parser::Table,
    parsed: &Body,
    contract: &Contract,
) -> Result<Compiled> {
    let scopes = [scope(table, contract)?];
    let native = |name: &crate::compiler::parser::Name| {
        let mut column = resolve(name, &scopes)?;
        column.name = field(0, &column.name);
        Ok(column)
    };
    validate_expansion(parsed.expansion.as_ref(), &scopes[0])?;
    let resolve = |name: &crate::compiler::parser::Name| {
        crate::compiler::expansion::resolve(name, parsed.expansion.as_ref(), native(name))
    };
    let occurrence_keys =
        crate::compiler::properties::Analysis::source_relation(scopes[0].relation, 0)?;
    build(
        parsed,
        &resolve,
        Stage {
            contract,
            unit: false,
            sources: vec![Source {
                schema: scopes[0].relation.schema.clone(),
                table: scopes[0].relation.table.clone(),
            }],
            occurrence_keys: &occurrence_keys,
            aggregate_base: 0,
            scalar_scope: None,
        },
    )
}
fn build(
    parsed: &Body,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: Stage<'_>,
) -> Result<Compiled> {
    let groups = grouping::resolve(parsed, resolve, context.scalar_scope)?;
    if having::required(parsed) {
        return having::build(parsed, groups, resolve, context);
    }
    build_direct(parsed, groups, resolve, context)
}
fn build_direct(
    parsed: &Body,
    groups: Vec<crate::compiler::parser::scalar::Key>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: Stage<'_>,
) -> Result<Compiled> {
    let mut computed = Vec::new();
    let group_keys = keys(groups, resolve, &mut computed, context.scalar_scope)?;
    let mut state = Targets {
        next: context.aggregate_base,
        grouped: Vec::new(),
        windows: windows::Windows::default(),
    };
    let columns = bind_targets(
        parsed.targets.clone(),
        resolve,
        &mut computed,
        TargetContext {
            groups: &group_keys,
            occurrence_keys: context.occurrence_keys,
            scalar_scope: context.scalar_scope,
        },
        &mut state,
    )?;
    state.windows.validate_occurrences(parsed.expansion.is_some())?;
    validate_keys(&group_keys)?;
    let averages = state
        .grouped
        .iter()
        .chain(state.windows.aggregates())
        .filter(|value| matches!(value.function.ordinary(), Function::Average))
        .map(|value| value.field.clone())
        .collect::<Vec<_>>();
    let output = crate::compiler::Projected { columns };
    let mut nodes = if state.windows.is_empty() {
        grouped_nodes(group_keys, state.grouped, computed, &output)?
    } else {
        ensure!(state.grouped.is_empty(), "grouped window composition requires subquery lowering");
        state.windows.nodes(computed)?
    };
    if context.unit {
        for node in &mut nodes {
            node.rewrite(&|id| id.into(), &|edge| {
                if edge == "source" { "unit".into() } else { edge.into() }
            });
        }
        nodes.insert(
            1,
            Node::Constant {
                id: "unit".into(),
                input: "source".into(),
                rows: vec![crate::transaction::Row::new()],
                columns: Vec::new(),
            },
        );
    }
    attach_expansion(&mut nodes, parsed.expansion.as_ref())?;
    let predicates = source_predicate(parsed.predicate.clone(), resolve)?;
    let sources = context.sources;
    let mut relational = Relational { sources, nodes, output, projection: Vec::new() };
    composition::lower_source_filter(&mut relational, predicates)?;
    super::boundary::finalize(&mut relational, &averages)?;
    Ok(Compiled { result_columns: Vec::new(), relational })
}
fn grouped_nodes(
    keys: Vec<ColumnRef>,
    aggregates: Vec<Aggregate>,
    computed: Vec<crate::compiler::scalar::Computed>,
    output: &crate::compiler::Projected,
) -> Result<Vec<Node>> {
    if aggregates.is_empty() && keys.is_empty() {
        let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
        let input = if computed.is_empty() {
            "source"
        } else {
            nodes.push(Node::Map { id: "mapped".into(), input: "source".into(), computed });
            "mapped"
        };
        nodes.push(Node::Output { id: "project".into(), input: input.into() });
        return Ok(nodes);
    }
    ensure!(
        output
            .columns
            .iter()
            .all(|output| aggregates.iter().any(|value| value.field == output.column.name)
                || keys.contains(&output.column)),
        "output column must appear in GROUP BY"
    );
    let spec = Partition {
        mode: Mode::Grouped { keys: keys.clone() },
        aggregates,
        previous_windows: None,
    };
    Ok(nodes(keys, spec, computed))
}

fn source_predicate(
    predicate: Option<crate::compiler::syntax::Expr>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
) -> Result<Option<crate::compiler::expression::Expr>> {
    let predicates = predicate
        .map(|expr| {
            crate::compiler::expression::bind_quals(&expr, &|name| {
                let column = resolve(name)?;
                ensure!(
                    column.name != crate::compiler::expansion::FIELD,
                    "WHERE on generated series columns requires post-expansion filtering"
                );
                Ok(column)
            })
        })
        .transpose()?;
    Ok(predicates)
}
#[derive(Clone, Copy)]
struct TargetContext<'a> {
    groups: &'a [ColumnRef],
    occurrence_keys: &'a crate::compiler::properties::Properties,
    scalar_scope: Option<usize>,
}
fn bind_targets(
    targets: Vec<syntax::Target>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<crate::compiler::scalar::Computed>,
    context: TargetContext<'_>,
    state: &mut Targets,
) -> Result<Vec<OutputColumn>> {
    let mut columns = Vec::new();
    for target in targets {
        let column = match target.value {
            syntax::Value::Typed(value) => {
                crate::compiler::typed::computed(&value, resolve, computed, context.scalar_scope)?
            }
            syntax::Value::Aggregate(value) => {
                aggregate(*value, resolve, context, state, computed)?
            }
        };
        columns.push(OutputColumn { column, label: target.label });
    }
    Ok(columns)
}
fn validate_expansion(
    series: Option<&crate::compiler::parser::expansion::Series>,
    scope: &super::Scope<'_>,
) -> Result<()> {
    if let Some(series) = series {
        ensure!(series.alias != super::qualifier(scope), "duplicate source/series alias");
    }
    Ok(())
}
fn attach_expansion(
    nodes: &mut Vec<Node>,
    series: Option<&crate::compiler::parser::expansion::Series>,
) -> Result<()> {
    let Some(series) = series else {
        return Ok(());
    };
    for node in nodes.iter() {
        if let Node::Map { computed, .. } = node {
            for value in computed {
                if let Some((_, duration)) = value.expression.shift() {
                    ensure!(
                        duration
                            .checked_mul(i64::from(series.end))
                            .is_some_and(|micros| micros <= 9_007_199_254_740_992),
                        "hopping offset exceeds exact PostgreSQL interval multiplication range"
                    );
                }
            }
        }
    }
    crate::compiler::expansion::attach(nodes, series);
    Ok(())
}
fn keys(
    parsed: Vec<crate::compiler::parser::scalar::Key>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<crate::compiler::scalar::Computed>,
    scalar_scope: Option<usize>,
) -> Result<Vec<ColumnRef>> {
    parsed.into_iter().map(|key| bind_key(key, resolve, computed, scalar_scope)).collect()
}
fn bind_key(
    key: crate::compiler::parser::scalar::Key,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<crate::compiler::scalar::Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    match key {
        crate::compiler::parser::scalar::Key::Column(name) => resolve(&name),
        crate::compiler::parser::scalar::Key::Typed(value) => {
            crate::compiler::typed::computed(&value, resolve, computed, scope)
        }
    }
}

struct Targets {
    next: usize,
    grouped: Vec<Aggregate>,
    windows: windows::Windows,
}
fn aggregate(
    value: syntax::Aggregate,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: TargetContext<'_>,
    state: &mut Targets,
    computed: &mut Vec<crate::compiler::scalar::Computed>,
) -> Result<ColumnRef> {
    let window = value
        .window
        .map(|window| {
            ensure!(
                context.groups.is_empty(),
                "grouped window composition requires subquery lowering"
            );
            bind_window(
                window,
                resolve,
                (context.occurrence_keys, value.function.occurrence_order()),
                computed,
                context.scalar_scope,
            )
        })
        .transpose()?;
    let argument::Bound { column: argument, filter, expression } = argument::bind(
        value.argument,
        value.filter,
        resolve,
        computed,
        argument::Context {
            scope: context.scalar_scope,
            function: &value.function,
            framed: window.is_some(),
        },
    )?;
    let navigation = value
        .navigation
        .as_ref()
        .map(|parsed| {
            crate::compiler::navigation::bind(
                parsed,
                argument.as_ref().context("missing navigation input")?,
                resolve,
            )
        })
        .transpose()?;
    let position = value
        .position
        .as_deref()
        .map(|ast| argument::integer_window_argument(ast, resolve))
        .transpose()?;
    let mut aggregate = Aggregate {
        function: value.function,
        expression,
        argument,
        filter,
        field: format!("@aggregate_{}", state.next),
        navigation,
        position,
    };
    let output = aggregate.output_column()?;
    // Integral extrema are invariant to duplicate inputs.
    if matches!(aggregate.function, Function::MinDistinct | Function::MaxDistinct) {
        aggregate.function = aggregate.function.ordinary();
    }
    state.next += 1;
    if let Some((mode, keys)) = window {
        state.windows.add(mode, keys, aggregate)?;
    } else {
        state.grouped.push(aggregate);
    }
    Ok(output)
}

fn validate_keys(keys: &[ColumnRef]) -> Result<()> {
    ensure!(
        keys.iter().all(|key| matches!(
            key.oid,
            16 | 20 | 21 | 23 | 1082 | 1700 | 25 | 1043 | 2950 | 1114 | 1184
        )),
        "partition key requires native deterministic scalar equality"
    );
    Ok(())
}
fn validate_order(
    order: &[Order],
    identity: &crate::compiler::properties::Properties,
    partition: &[ColumnRef],
) -> Result<()> {
    ensure!(
        order.iter().all(|value| matches!(value.column.oid, 20 | 21 | 23 | 1114 | 1184)),
        "positional window ordering requires integral or timestamp columns"
    );
    let names = partition
        .iter()
        .map(|c| c.name.clone())
        .chain(order.iter().map(|c| c.column.name.clone()))
        .collect::<Vec<_>>();
    ensure!(
        identity.proves_key(&names),
        "positional window partition and ordering must include complete logical occurrence identity"
    );
    Ok(())
}

fn bind_window(
    window: syntax::Window,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: (&crate::compiler::properties::Properties, bool),
    computed: &mut Vec<crate::compiler::scalar::Computed>,
    scope: Option<usize>,
) -> Result<(Mode, Vec<ColumnRef>)> {
    let window_keys = keys(window.keys, resolve, computed, scope)?;
    let order = window
        .order
        .into_iter()
        .map(|(name, descending, nulls_first)| {
            Ok(Order { column: bind_key(name, resolve, computed, scope)?, descending, nulls_first })
        })
        .collect::<Result<Vec<_>>>()?;
    let mode = match window.frame {
        syntax::WindowFrame::Range { start, end, exclude_current } => {
            if context.1 {
                validate_order(&order, context.0, &window_keys)?;
            }
            range::bind(start, end, order, (window.ranking, exclude_current))?
        }
        syntax::WindowFrame::Position(frame) => {
            if window.ranking || window.peers.is_some() {
                ensure!(
                    order
                        .iter()
                        .all(|value| matches!(value.column.oid, 20 | 21 | 23 | 1114 | 1184)),
                    "peer ordering requires integral or timestamp columns"
                );
            }
            if context.1 || (!window.ranking && window.peers.is_none()) {
                validate_order(&order, context.0, &window_keys)?;
            }
            if window.ranking {
                Mode::Ranking { order }
            } else if let Some(groups) = window.peers {
                Mode::Peers { order, frame, groups }
            } else {
                Mode::Rows { order, frame }
            }
        }
    };
    Ok((mode, window_keys))
}

fn nodes(
    keys: Vec<ColumnRef>,
    spec: Partition,
    computed: Vec<crate::compiler::scalar::Computed>,
) -> Vec<Node> {
    if matches!(&spec.mode, Mode::Grouped { keys } if keys.is_empty()) {
        return global_nodes(spec, computed);
    }
    if prefix::supports(&spec) {
        return prefix::nodes(keys, spec, computed);
    }
    if ordered::supports(&spec) {
        return ordered::nodes(keys, spec, computed);
    }
    if distinct::supports(&spec) {
        return distinct::nodes(
            keys,
            crate::compiler::partition::statistics::with_key_scales(spec),
            computed,
        );
    }
    let linear = crate::compiler::partition::statistics::fits(&spec)
        && spec.aggregates.iter().all(crate::compiler::partition::statistics::supports);
    let spec =
        if linear { crate::compiler::partition::statistics::with_key_scales(spec) } else { spec };
    let aggregate = if linear {
        Node::Statistics {
            id: "statistics".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        }
    } else {
        Node::Partition {
            id: "partition".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        }
    };
    let mut nodes = input_nodes(keys, computed);
    nodes.push(aggregate);
    if linear {
        nodes.push(Node::Finalize { id: "partition".into(), input: "statistics".into(), spec });
    }
    nodes.push(Node::Output { id: "project".into(), input: "partition".into() });
    nodes
}

fn input_nodes(
    keys: Vec<ColumnRef>,
    computed: Vec<crate::compiler::scalar::Computed>,
) -> Vec<Node> {
    let mapped = !computed.is_empty();
    let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
    if mapped {
        nodes.push(Node::Map { id: "mapped".into(), input: "source".into(), computed });
    }
    nodes.push(Node::PartitionBy {
        id: "partition_input".into(),
        input: if mapped { "mapped" } else { "source" }.into(),
        keys,
    });
    nodes
}

fn global_nodes(spec: Partition, computed: Vec<crate::compiler::scalar::Computed>) -> Vec<Node> {
    let mut nodes = input_nodes(Vec::new(), computed);
    if let Some(Node::PartitionBy { id, .. }) = nodes.last_mut() {
        *id = "global_data".into();
    }
    nodes.push(Node::Constant {
        id: "global_seed".into(),
        input: "source".into(),
        rows: vec![[(crate::compiler::partition::SEED.into(), Some("t".into()))].into()],
        columns: Vec::new(),
    });
    nodes.push(Node::Union {
        id: "partition_input".into(),
        left: "global_data".into(),
        right: "global_seed".into(),
    });
    if spec
        .aggregates
        .iter()
        .all(|a| matches!(a.function, Function::Count | Function::Sum | Function::Average))
    {
        nodes.push(Node::Statistics {
            id: "statistics".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        });
        nodes.push(Node::Finalize { id: "partition".into(), input: "statistics".into(), spec });
    } else {
        nodes.push(Node::Partition {
            id: "partition".into(),
            input: "partition_input".into(),
            spec,
        });
    }
    nodes.push(Node::Output { id: "project".into(), input: "partition".into() });
    nodes
}
