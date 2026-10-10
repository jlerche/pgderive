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
pub(super) fn bind(parsed: syntax::Parsed, contract: &Contract) -> Result<Compiled> {
    let scopes = [scope(parsed.source, contract)?];
    let resolve = |name: &crate::compiler::parser::Name| {
        let mut column = resolve(name, &scopes)?;
        column.name = field(0, &column.name);
        Ok(column)
    };
    let group_keys = parsed.groups.iter().map(&resolve).collect::<Result<Vec<_>>>()?;
    let mut mode = None;
    let mut keys = group_keys.clone();
    let mut aggregates = Vec::new();
    let mut columns = Vec::new();
    for target in parsed.targets {
        let column = match target.value {
            syntax::Value::Column(name) => resolve(&name)?,
            syntax::Value::Aggregate(value) => {
                if let Some(window) = value.window {
                    ensure!(
                        group_keys.is_empty(),
                        "grouped window composition requires subquery lowering"
                    );
                    let (candidate, window_keys) =
                        bind_window(window, &resolve, scopes[0].relation)?;
                    if let Some(prior) = &mode {
                        ensure!(
                            serde_json::to_vec(&(prior, &keys))?
                                == serde_json::to_vec(&(&candidate, &window_keys))?,
                            "multiple window specifications unsupported"
                        );
                    }
                    keys = window_keys;
                    mode = Some(candidate);
                } else {
                    ensure!(!group_keys.is_empty(), "ungrouped aggregates unsupported");
                }
                let argument = value.argument.as_ref().map(&resolve).transpose()?;
                let oid = result_type(&value.function, argument.as_ref())?;
                let name = format!("@aggregate_{}", aggregates.len());
                let filter = value
                    .filter
                    .map(|expr| crate::compiler::expression::bind(expr, &resolve))
                    .transpose()?;
                aggregates.push(Aggregate {
                    function: value.function,
                    argument,
                    filter: filter.map(crate::compiler::Predicate::new),
                    field: name.clone(),
                });
                ColumnRef { name, right: false, oid, nullable: true }
            }
        };
        columns.push(OutputColumn { column, label: target.label });
    }
    ensure!(!aggregates.is_empty(), "partition requires an aggregate");
    validate_keys(&keys)?;
    if mode.is_none() {
        ensure!(
            columns
                .iter()
                .all(|output| output.column.name.starts_with('@')
                    || group_keys.contains(&output.column)),
            "output column must appear in GROUP BY"
        );
    }
    let spec = Partition { mode: mode.unwrap_or(Mode::Grouped { keys: group_keys }), aggregates };
    let relation = scopes[0].relation;
    let output = crate::compiler::Projected {
        schema: relation.schema.clone(),
        table: relation.table.clone(),
        columns,
        terminal: None,
    };
    let nodes = nodes(keys, spec);
    let predicates = parsed
        .predicate
        .map(|expr| crate::compiler::expression::bind(expr, &resolve))
        .transpose()?;
    let sources = vec![Source { schema: relation.schema.clone(), table: relation.table.clone() }];
    Ok(Compiled { program: crate::compiler::Program::Relational { relational: Relational { sources, nodes, output } }, predicates, revision: Some("sql-partition-v1:pg-query-6.2.1:pg-17.7:row-text-v1:json-v2:native-bag-v1:affected-partition-v1:integral-aggregate-v1:rows-frame-v1".into()) })
}
fn validate_keys(keys: &[ColumnRef]) -> Result<()> {
    ensure!(
        keys.iter().all(|key| matches!(key.oid, 16 | 20 | 21 | 23 | 2950)),
        "partition key requires native bool/integral/UUID equality"
    );
    Ok(())
}
fn validate_order(order: &[Order], relation: &crate::source::Relation) -> Result<()> {
    ensure!(
        order.iter().all(|value| matches!(value.column.oid, 20 | 21 | 23)),
        "ROWS ordering requires integral columns"
    );
    ensure!(
        relation
            .columns
            .iter()
            .filter(|column| column.primary)
            .all(|column| order.iter().any(|order| order.column.name == field(0, &column.name))),
        "ROWS ordering must include the complete source primary key for deterministic occurrence order"
    );
    Ok(())
}
fn result_type(function: &Function, argument: Option<&ColumnRef>) -> Result<u32> {
    match function {
        Function::Count => Ok(20),
        Function::Sum => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 21 | 23)),
                "SUM requires int2/int4; numeric SUM(int8) is deferred"
            );
            Ok(20)
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
    relation: &crate::source::Relation,
) -> Result<(Mode, Vec<ColumnRef>)> {
    let keys = window.keys.iter().map(resolve).collect::<Result<Vec<_>>>()?;
    let order = window
        .order
        .into_iter()
        .map(|(name, descending, nulls_first)| {
            Ok(Order { column: resolve(&name)?, descending, nulls_first })
        })
        .collect::<Result<Vec<_>>>()?;
    validate_order(&order, relation)?;
    Ok((Mode::Rows { order, frame: window.frame }, keys))
}

fn nodes(keys: Vec<ColumnRef>, spec: Partition) -> Vec<Node> {
    vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::PartitionBy { id: "partition_input".into(), input: "source".into(), keys },
        Node::Partition { id: "partition".into(), input: "partition_input".into(), spec },
        Node::Output { id: "project".into(), input: "partition".into() },
    ]
}
