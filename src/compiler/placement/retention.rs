//! Executable output layout is the retained operand/reuse contract, not advisory liveness.
use crate::{
    catalog::terminal::program::Expression,
    compiler::{ColumnRef, Compiled},
};
use anyhow::Result;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Retention {
    arrangement: &'static str,
    owner: &'static str,
    identity: &'static str,
    replay: &'static str,
    shared_terminal_definitions: &'static str,
    operands: Vec<Operand>,
}
#[derive(Serialize)]
struct Operand {
    index: usize,
    column: ColumnRef,
    encoding: &'static str,
    terminal_nodes: Vec<usize>,
}
pub(super) fn analyze(compiled: &Compiled) -> Result<Retention> {
    let program = compiled
        .output()
        .terminal
        .as_ref()
        .map(|terminal| terminal.placement_program())
        .transpose()?
        .map(|(program, _)| program);
    let operands = compiled.output().columns.iter().enumerate().map(|(index, output)| {
        let terminal_nodes = program.map_or_else(Vec::new, |program| program.expressions.iter().enumerate().filter_map(|(node, expression)| {
            matches!(expression, Expression::Input(input) | Expression::Average(input) if *input == index).then_some(node)
        }).collect());
        let ratio = program.is_some_and(|program| program.expressions.iter().any(|expression| matches!(expression, Expression::Average(input) if *input == index)));
        Operand { index, column: output.column.clone(), encoding: if ratio { "sum_count_ratio_text" } else { "native_text" }, terminal_nodes }
    }).collect();
    Ok(Retention {
        arrangement: "output",
        owner: "engine_and_object_storage",
        identity: "full_tuple_weighted",
        replay: "same_bound_terminal_program_for_insertions_and_retractions",
        shared_terminal_definitions: "shared_dag_nodes_may_be_evaluated_more_than_once",
        operands,
    })
}
