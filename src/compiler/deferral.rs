//! The first movement rule: total integral widening immediately before Output.
use super::{Compiled, projection::OutputColumn, relational::Node};
use crate::catalog::{
    Terminal,
    terminal::program::{Expression, Function, Output, Program},
};
use anyhow::Result;

pub(super) fn apply(compiled: &mut Compiled) -> Result<()> {
    if compiled.output().terminal.is_some() {
        return Ok(());
    }
    let nodes = &compiled.relational.nodes;
    let [.., Node::Map { id, computed, .. }, Node::Output { input, .. }] = nodes.as_slice() else {
        return Ok(());
    };
    if id != input {
        return Ok(());
    }
    let eligible = computed
        .iter()
        .filter_map(|value| {
            let super::scalar::Native::Typed { typed } = &value.expression else {
                return None;
            };
            let operand = typed.widening_input()?;
            // Sibling maps execute against the input row, never against each other's results.
            if computed.iter().any(|other| {
                other.expression.columns().iter().any(|column| column.name == value.column.name)
            }) {
                return None;
            }
            compiled
                .output()
                .columns
                .iter()
                .any(|output| output.column == value.column)
                .then_some((value.column.clone(), operand.clone()))
        })
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(());
    }
    let mut columns: Vec<OutputColumn> = Vec::new();
    let mut program = Program { expressions: vec![], outputs: vec![], predicate: None };
    for output in &compiled.output().columns {
        let replacement = eligible.iter().find(|(column, _)| column == &output.column);
        let column = replacement.map_or(&output.column, |(_, operand)| operand);
        let index =
            columns.iter().position(|output| &output.column == column).unwrap_or_else(|| {
                columns.push(OutputColumn { column: column.clone(), label: output.label.clone() });
                columns.len() - 1
            });
        let input = append(&mut program, Expression::Input(index));
        let expression = if replacement.is_some() {
            let function = if output.column.oid == 20 { Function::Int8 } else { Function::Int4 };
            append(&mut program, Expression::Call { function, arguments: vec![input] })
        } else {
            input
        };
        program.outputs.push(Output { label: output.label.clone(), expression });
    }
    let terminal =
        Terminal::from_program(program, columns.iter().map(|output| output.column.oid).collect())?;
    remove(&mut compiled.relational.nodes, &eligible);
    compiled.relational.output.columns = columns;
    compiled.relational.output.terminal = Some(terminal);
    Ok(())
}
fn remove(nodes: &mut Vec<Node>, eligible: &[(super::ColumnRef, super::ColumnRef)]) {
    let length = nodes.len();
    let Node::Map { computed, input, .. } = &mut nodes[length - 2] else {
        return;
    };
    computed.retain(|value| !eligible.iter().any(|(column, _)| column == &value.column));
    if computed.is_empty() {
        let bypass = input.clone();
        if let Node::Output { input, .. } = &mut nodes[length - 1] {
            *input = bypass;
        }
        nodes.remove(length - 2);
    }
}
fn append(program: &mut Program, expression: Expression) -> usize {
    if let Some(index) = program.expressions.iter().position(|prior| prior == &expression) {
        return index;
    }
    program.expressions.push(expression);
    program.expressions.len() - 1
}

#[cfg(test)]
mod tests;
