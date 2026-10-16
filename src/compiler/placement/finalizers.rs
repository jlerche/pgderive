//! Physical dependencies of the existing additive finalizer, including validation.
use crate::compiler::{
    ColumnRef,
    partition::{Function, Mode, Partition, statistics},
    relational::Node,
};
use anyhow::{Result, bail};
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Finalizer {
    stage: String,
    input: String,
    grouping: Vec<ColumnRef>,
    state_encoding: &'static str,
    validation_fields: Vec<String>,
    outputs: Vec<Output>,
    obligations: Vec<&'static str>,
}
#[derive(Serialize)]
struct Output {
    field: String,
    sql_type_oid: u32,
    encoding: &'static str,
    value_fields: Vec<String>,
}

pub(super) fn analyze(nodes: &[Node]) -> Result<Vec<Finalizer>> {
    nodes
        .iter()
        .filter_map(|node| {
            let Node::Finalize { id, input, spec } = node else {
                return None;
            };
            Some(describe(id, input, spec))
        })
        .collect()
}
fn describe(id: &str, input: &str, spec: &Partition) -> Result<Finalizer> {
    let Mode::Grouped { keys } = &spec.mode else {
        bail!("finalizer placement requires grouped statistics");
    };
    let outputs = spec
        .aggregates
        .iter()
        .enumerate()
        .map(|(index, aggregate)| {
            let (sql_type_oid, encoding) = match aggregate.function {
                Function::Count | Function::CountDistinct => (20, "native_text"),
                Function::Sum => (
                    if aggregate
                        .argument
                        .as_ref()
                        .is_some_and(|column| matches!(column.oid, 20 | 1700))
                    {
                        1700
                    } else {
                        20
                    },
                    "native_text",
                ),
                Function::Average => (1700, "sum_count_ratio"),
                _ => bail!("nonadditive statistics finalizer"),
            };
            Ok(Output {
                field: aggregate.field.clone(),
                sql_type_oid,
                encoding,
                value_fields: value_fields(index, &aggregate.function),
            })
        })
        .collect::<Result<_>>()?;
    Ok(Finalizer {
        stage: id.into(),
        input: input.into(),
        grouping: keys.clone(),
        state_encoding: "signed_arbitrary_precision_decimal",
        validation_fields: (0..statistics::width(spec.aggregates.len()))
            .map(statistics::field)
            .collect(),
        outputs,
        obligations: vec![
            "whole_statistics_row_validation",
            "finalizer_overflow_domain",
            "group_presence_and_multiplicity",
            "average_ratio_requires_terminal_conversion",
            "retraction_reproducibility",
        ],
    })
}
fn value_fields(index: usize, function: &Function) -> Vec<String> {
    let mut fields = vec![statistics::field(statistics::count(index))];
    if matches!(function, Function::Sum | Function::Average) {
        fields.push(statistics::field(statistics::sum(index)));
    }
    fields
}

pub(super) fn operands(spec: &Partition, column: &str) -> Vec<String> {
    if let Mode::Grouped { keys } = &spec.mode
        && keys.iter().any(|key| key.name == column)
    {
        return vec![column.into()];
    }
    spec.aggregates
        .iter()
        .enumerate()
        .filter(|(_, aggregate)| aggregate.field == column)
        .flat_map(|(index, aggregate)| value_fields(index, &aggregate.function))
        .collect()
}

pub(super) fn measurements(spec: &Partition, column: &str) -> Vec<ColumnRef> {
    if let Mode::Grouped { keys } = &spec.mode
        && keys.iter().any(|key| key.name == column)
    {
        return keys.iter().filter(|key| key.name == column).cloned().collect();
    }
    let mut columns = Vec::new();
    for (index, aggregate) in spec.aggregates.iter().enumerate() {
        if [statistics::field(statistics::count(index)), statistics::field(statistics::sum(index))]
            .iter()
            .any(|name| name == column)
        {
            columns.extend(aggregate.argument.clone());
            if let Some(filter) = &aggregate.filter {
                columns.extend(filter.columns());
            }
        }
    }
    columns
}
