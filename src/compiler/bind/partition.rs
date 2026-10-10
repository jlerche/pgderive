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
mod composition;
struct Body {
    expansion: Option<crate::compiler::parser::expansion::Series>,
    targets: Vec<syntax::Target>,
    groups: Vec<crate::compiler::parser::scalar::Key>,
    predicate: Option<crate::compiler::syntax::Expr>,
}
#[derive(Clone, Copy)]
struct Stage<'a> {
    relation: &'a crate::source::Relation,
    aggregate_base: usize,
    scalar_scope: Option<usize>,
}
pub(super) fn bind(parsed: syntax::Parsed, contract: &Contract) -> Result<Compiled> {
    let syntax::Parsed { source, expansion, targets, groups, predicate } = parsed;
    let body = Body { expansion, targets, groups, predicate };
    match source {
        syntax::Source::Native(table) => bind_native(table, &body, contract),
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
    build(
        parsed,
        &resolve,
        Stage { relation: scopes[0].relation, aggregate_base: 0, scalar_scope: None },
    )
}
fn build(
    parsed: &Body,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: Stage<'_>,
) -> Result<Compiled> {
    let mut computed = Vec::new();
    let group_keys = keys(parsed.groups.clone(), resolve, &mut computed, context.scalar_scope)?;
    let mut state = Targets {
        mode: None,
        keys: group_keys.clone(),
        aggregates: Vec::new(),
        base: context.aggregate_base,
    };
    let BoundTargets { columns, transforms } = bind_targets(
        parsed.targets.clone(),
        resolve,
        &mut computed,
        (&group_keys, context.relation, context.scalar_scope),
        &mut state,
    )?;
    let projection = state.aggregates.is_empty() && group_keys.is_empty();
    ensure!(
        !state.aggregates.is_empty()
            || projection && (!computed.is_empty() || parsed.expansion.is_some()),
        "partition requires an aggregate"
    );
    validate_occurrences(parsed.expansion.as_ref(), &state)?;
    validate_keys(&state.keys)?;
    if state.mode.is_none() && !projection {
        ensure!(
            columns.iter().all(|output| state
                .aggregates
                .iter()
                .any(|aggregate| aggregate.field == output.column.name)
                || group_keys.contains(&output.column)),
            "output column must appear in GROUP BY"
        );
    }
    let spec = Partition {
        mode: state.mode.unwrap_or(Mode::Grouped { keys: group_keys }),
        aggregates: state.aggregates,
    };
    let relation = context.relation;
    let terminal = super::terminal(transforms, &columns)?;
    let output = crate::compiler::Projected {
        schema: relation.schema.clone(),
        table: relation.table.clone(),
        columns,
        terminal,
    };
    let mapped = !computed.is_empty();
    let mut nodes = if projection {
        vec![
            Node::Source { id: "source".into(), source: 0 },
            Node::Map { id: "mapped".into(), input: "source".into(), computed },
            Node::Output { id: "project".into(), input: "mapped".into() },
        ]
    } else {
        nodes(state.keys, spec, computed)
    };
    attach_expansion(&mut nodes, parsed.expansion.as_ref())?;
    let predicates = source_predicate(parsed.predicate.clone(), resolve)?;
    let sources = vec![Source { schema: relation.schema.clone(), table: relation.table.clone() }];
    let numeric = output.terminal.is_some();
    let revision = plan_revision(numeric, mapped, &nodes);
    Ok(Compiled {
        program: crate::compiler::Program::Relational {
            relational: Relational { sources, nodes, output },
        },
        predicates,
        revision: Some(revision),
    })
}
fn source_predicate(
    predicate: Option<crate::compiler::syntax::Expr>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
) -> Result<Option<crate::compiler::expression::Expr>> {
    let predicates = predicate
        .map(|expr| {
            crate::compiler::expression::bind(expr, &|name| {
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
struct BoundTargets {
    columns: Vec<OutputColumn>,
    transforms: Vec<crate::catalog::terminal::Transform>,
}
fn bind_targets(
    targets: Vec<syntax::Target>,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<crate::compiler::scalar::Computed>,
    context: (&[ColumnRef], &crate::source::Relation, Option<usize>),
    state: &mut Targets,
) -> Result<BoundTargets> {
    let mut columns = Vec::new();
    let mut transforms = Vec::new();
    for target in targets {
        let mut transform = crate::catalog::terminal::Transform::Identity;
        let column = match target.value {
            syntax::Value::Column(name) => resolve(&name)?,
            syntax::Value::Bin(bin) => {
                crate::compiler::scalar::bind(&bin, resolve, computed, context.2)?
            }
            syntax::Value::Case(case) => {
                crate::compiler::scalar::bind_case(&case, resolve, computed, context.2)?
            }
            syntax::Value::Aggregate(value) => {
                let result = aggregate(*value, resolve, (context.0, context.1), state)?;
                transform = result.1;
                result.0
            }
        };
        transforms.push(transform);
        columns.push(OutputColumn { column, label: target.label });
    }
    Ok(BoundTargets { columns, transforms })
}
fn validate_occurrences(
    series: Option<&crate::compiler::parser::expansion::Series>,
    state: &Targets,
) -> Result<()> {
    if series.is_some()
        && let Some(Mode::Rows { order, .. } | Mode::Ranking { order }) = state.mode.as_ref()
        && (matches!(state.mode, Some(Mode::Rows { .. }))
            || state.aggregates.iter().any(|value| value.function.occurrence_order()))
    {
        ensure!(
            order.iter().any(|order| order.column.name == crate::compiler::expansion::FIELD),
            "expanded positional windows must order by generated ordinal"
        );
    }
    Ok(())
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
    parsed
        .into_iter()
        .map(|key| match key {
            crate::compiler::parser::scalar::Key::Column(name) => resolve(&name),
            crate::compiler::parser::scalar::Key::Bin(bin) => {
                crate::compiler::scalar::bind(&bin, resolve, computed, scalar_scope)
            }
        })
        .collect()
}

fn plan_revision(numeric: bool, mapped: bool, nodes: &[Node]) -> String {
    let bin = nodes.iter().any(|node| matches!(node, Node::Map { computed, .. } if computed.iter().any(|value| matches!(value.expression, crate::compiler::scalar::Native::Bin(_)))));
    let mut revision = revision(numeric, mapped, bin);
    if nodes.iter().any(|node| matches!(node, Node::Statistics { .. })) {
        revision.push_str(":exact-linear-statistics-v1");
    }
    if nodes.iter().any(|node| matches!(node, Node::Partition { spec, .. } if matches!(spec.mode, Mode::Ranking { .. }))) {
        revision.push_str(":sql-peer-ranking-v1");
    }
    if nodes.iter().any(|node| matches!(node, Node::Partition { spec, .. } if matches!(spec.mode, Mode::Peers { .. }))) {
        revision.push_str(":sql-peer-frames-v1");
    }
    if nodes.iter().any(|node| matches!(node, Node::Partition { spec, .. } if spec.aggregates.iter().any(|value| value.navigation.is_some()))) {
        revision.push_str(":pg-native-navigation-v1");
    }
    if nodes.iter().any(|node| matches!(node, Node::Map { computed, .. } if computed.iter().any(|value| matches!(value.expression, crate::compiler::scalar::Native::Case { .. })))) {
        revision.push_str(":pg-lazy-integral-case-v1");
    }
    if nodes.iter().any(|node| matches!(node, Node::Expand { .. })) {
        revision.push_str(":bounded-series-expansion-v1:fixed-duration-offset-v1");
    }
    revision
}
fn revision(numeric: bool, mapped: bool, bin: bool) -> String {
    let base = if numeric {
        "sql-partition-v2:pg-query-6.2.1:pg-17.7:row-text-v1:json-exact-number-v1:native-bag-v1:affected-partition-v1:integer-numeric-aggregate-v1:rows-frame-v1"
    } else {
        "sql-partition-v1:pg-query-6.2.1:pg-17.7:row-text-v1:json-v2:native-bag-v1:affected-partition-v1:integral-aggregate-v1:rows-frame-v1"
    };
    let mut revision = base.to_string();
    if mapped {
        revision.push_str(":native-scalar-map-v1");
    }
    if bin {
        revision.push_str(":date-bin-microseconds-v1");
    }
    revision
}

struct Targets {
    base: usize,
    mode: Option<Mode>,
    keys: Vec<ColumnRef>,
    aggregates: Vec<Aggregate>,
}
fn aggregate(
    value: syntax::Aggregate,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: (&[ColumnRef], &crate::source::Relation),
    state: &mut Targets,
) -> Result<(ColumnRef, crate::catalog::terminal::Transform)> {
    use crate::catalog::terminal::Transform;
    if let Some(window) = value.window {
        ensure!(context.0.is_empty(), "grouped window composition requires subquery lowering");
        let (candidate, window_keys) =
            bind_window(window, resolve, (context.1, value.function.occurrence_order()))?;
        if let Some(prior) = &state.mode {
            ensure!(
                serde_json::to_vec(&(prior, &state.keys))?
                    == serde_json::to_vec(&(&candidate, &window_keys))?,
                "multiple window specifications unsupported"
            );
        }
        state.keys = window_keys;
        state.mode = Some(candidate);
    } else {
        ensure!(!context.0.is_empty(), "ungrouped aggregates unsupported");
    }
    let argument = value.argument.as_ref().map(resolve).transpose()?;
    let navigation = value
        .navigation
        .map(|parsed| {
            crate::compiler::navigation::bind(
                parsed,
                argument.as_ref().context("missing navigation input")?,
                resolve,
            )
        })
        .transpose()?;
    let oid = navigation
        .as_ref()
        .map_or_else(|| result_type(&value.function, argument.as_ref()), |value| Ok(value.oid))?;
    let transform = match (&value.function, oid) {
        (Function::Average, _) => Transform::Average,
        (Function::Sum | Function::Lag | Function::Lead, 1700) => Transform::Numeric,
        _ => Transform::Identity,
    };
    let name = format!("@aggregate_{}", state.base + state.aggregates.len());
    let filter =
        value.filter.map(|expr| crate::compiler::expression::bind(expr, resolve)).transpose()?;
    let nullable = !value.function.ranking();
    state.aggregates.push(Aggregate {
        function: value.function,
        argument,
        filter: filter.map(crate::compiler::Predicate::new),
        field: name.clone(),
        navigation,
    });
    Ok((ColumnRef { name, right: false, oid, nullable }, transform))
}

fn validate_keys(keys: &[ColumnRef]) -> Result<()> {
    ensure!(
        keys.iter().all(|key| matches!(key.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184)),
        "partition key requires native bool/integral/UUID/timestamp equality"
    );
    Ok(())
}
fn validate_order(order: &[Order], relation: &crate::source::Relation) -> Result<()> {
    ensure!(
        order.iter().all(|value| matches!(value.column.oid, 20 | 21 | 23 | 1114 | 1184)),
        "positional window ordering requires integral or timestamp columns"
    );
    ensure!(
        relation
            .columns
            .iter()
            .filter(|column| column.primary)
            .all(|column| order.iter().any(|order| order.column.name == field(0, &column.name))),
        "positional window ordering must include the complete source primary key for deterministic occurrence order"
    );
    Ok(())
}
fn result_type(function: &Function, argument: Option<&ColumnRef>) -> Result<u32> {
    match function {
        Function::Count | Function::Rank | Function::DenseRank | Function::RowNumber => Ok(20),
        Function::Sum => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23)),
                "SUM requires an integral argument"
            );
            Ok(if argument.is_some_and(|arg| arg.oid == 20) { 1700 } else { 20 })
        }
        Function::Average => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23)),
                "AVG requires integral input"
            );
            Ok(1700)
        }
        Function::Lag | Function::Lead => {
            anyhow::bail!("navigation requires typed operand binding")
        }
        Function::Min | Function::Max => {
            let column = argument.context("missing aggregate argument")?;
            ensure!(matches!(column.oid, 20 | 21 | 23), "MIN/MAX require integral columns");
            Ok(column.oid)
        }
    }
}

fn bind_window(
    window: syntax::Window,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    context: (&crate::source::Relation, bool),
) -> Result<(Mode, Vec<ColumnRef>)> {
    let keys = window.keys.iter().map(resolve).collect::<Result<Vec<_>>>()?;
    let order = window
        .order
        .into_iter()
        .map(|(name, descending, nulls_first)| {
            Ok(Order { column: resolve(&name)?, descending, nulls_first })
        })
        .collect::<Result<Vec<_>>>()?;
    if window.ranking || window.peers.is_some() {
        ensure!(
            order.iter().all(|value| matches!(value.column.oid, 20 | 21 | 23 | 1114 | 1184)),
            "peer ordering requires integral or timestamp columns"
        );
        if context.1 {
            validate_order(&order, context.0)?;
        }
        let mode = if let Some(groups) = window.peers {
            Mode::Peers { order, frame: window.frame, groups }
        } else {
            Mode::Ranking { order }
        };
        Ok((mode, keys))
    } else {
        validate_order(&order, context.0)?;
        Ok((Mode::Rows { order, frame: window.frame }, keys))
    }
}

fn nodes(
    keys: Vec<ColumnRef>,
    spec: Partition,
    computed: Vec<crate::compiler::scalar::Computed>,
) -> Vec<Node> {
    let mapped = !computed.is_empty();
    let linear = matches!(spec.mode, Mode::Grouped { .. })
        && spec.aggregates.iter().all(|aggregate| {
            matches!(aggregate.function, Function::Count | Function::Sum | Function::Average)
        });
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
    let mut nodes = vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::PartitionBy {
            id: "partition_input".into(),
            input: if mapped { "mapped" } else { "source" }.into(),
            keys,
        },
        aggregate,
        Node::Output { id: "project".into(), input: "partition".into() },
    ];
    if linear {
        nodes
            .insert(3, Node::Finalize { id: "partition".into(), input: "statistics".into(), spec });
    }
    if mapped {
        nodes.insert(1, Node::Map { id: "mapped".into(), input: "source".into(), computed });
    }
    nodes
}
