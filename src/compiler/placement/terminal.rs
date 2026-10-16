use super::{Consumer, ConsumerKind, Dependency, Execution, Expression as Entry, Status};
use crate::{
    catalog::terminal::program::Expression,
    compiler::{Compiled, relational::Node},
};
use anyhow::{Context, Result};
use std::collections::BTreeSet;

pub(super) fn append(compiled: &Compiled, entries: &mut Vec<Entry>) -> Result<()> {
    let output = compiled.output();
    let Some(terminal) = &output.terminal else {
        return Ok(());
    };
    let (program, types) = terminal.placement_program()?;
    let Some(Node::Output { input, .. }) = compiled.relational.nodes.last() else {
        anyhow::bail!("terminal program requires final output stage");
    };
    let mut consumers = vec![BTreeSet::new(); program.expressions.len()];
    let mut roots = program
        .outputs
        .iter()
        .enumerate()
        .map(|(index, output)| {
            (
                output.expression,
                Consumer {
                    node: compiled
                        .result_columns
                        .get(index)
                        .map_or_else(|| output.label.clone(), |column| column.name.clone()),
                    kind: ConsumerKind::Output,
                },
            )
        })
        .collect::<Vec<_>>();
    if let Some(predicate) = program.predicate {
        roots.push((
            predicate,
            Consumer { node: "terminal_predicate".into(), kind: ConsumerKind::SinkPredicate },
        ));
    }
    for (root, consumer) in roots {
        let mut pending = vec![root];
        while let Some(index) = pending.pop() {
            if consumers[index].insert(consumer.clone()) {
                pending.extend(program.expressions[index].dependencies());
            }
        }
    }
    for (index, expression) in program.expressions.iter().enumerate() {
        let dependencies = if let Expression::Input(index) | Expression::Average(index) = expression
        {
            vec![Dependency::Operand {
                stage: input.clone(),
                column: output
                    .columns
                    .get(*index)
                    .context("terminal placement operand absent")?
                    .column
                    .clone(),
            }]
        } else {
            expression
                .dependencies()
                .into_iter()
                .map(|index| Dependency::Expression { id: format!("terminal:{index}") })
                .collect()
        };
        entries.push(Entry {
            id: format!("terminal:{index}"),
            type_oid: types[index],
            current_execution: Execution::SinkDeferred,
            status: if consumers[index].is_empty() {
                Status::Unreferenced
            } else {
                Status::QualifiedExistingTerminal
            },
            dependencies,
            consumers: consumers[index].iter().cloned().collect(),
            field_consumers: consumers[index].iter().cloned().collect(),
            definition: serde_json::to_value(expression)?,
            obligations: vec![],
        });
    }
    Ok(())
}
