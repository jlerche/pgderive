//! Run ordinary aggregates over each grouping set and union their weighted output.
use super::{Body, Stage, build_direct, composition, having::GroupingSlot};
use crate::compiler::{
    ColumnRef, Compiled,
    parser::{Name, grouping_sets::Sets, scalar::Key},
    projection::OutputColumn,
    relational::{Binding, Node, Relational},
    scalar::Computed,
    typed::Typed,
};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(super) struct Layout<'a> {
    pub sets: &'a Sets,
    pub keys: &'a [ColumnRef],
    pub constant_slots: &'a [GroupingSlot],
    pub masks: &'a [Vec<usize>],
}

pub(super) fn build(
    body: &Body,
    groups: &[Key],
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    context: &Stage<'_>,
    layout: Layout<'_>,
) -> Result<Compiled> {
    let canonical = layout
        .keys
        .iter()
        .zip(layout.constant_slots)
        .map(|(key, slot)| {
            layout
                .keys
                .iter()
                .zip(layout.constant_slots)
                .position(|(candidate, other)| candidate == key && other == slot)
                .context("missing grouping key")
        })
        .collect::<Result<Vec<_>>>()?;
    let mut seen = BTreeSet::new();
    let mut merged = None;
    for set in &layout.sets.groups {
        let members = set.iter().map(|index| canonical[*index]).collect::<BTreeSet<_>>();
        if layout.sets.distinct && !seen.insert(members.clone()) {
            continue;
        }
        let mut branch = body.clone();
        branch.targets = body
            .targets
            .iter()
            .enumerate()
            .filter(|(index, _)| *index >= canonical.len() || members.contains(&canonical[*index]))
            .map(|(_, target)| target.clone())
            .collect();
        let mut physical = BTreeSet::new();
        let keys = members
            .iter()
            .filter(|index| physical.insert(&layout.keys[**index]))
            .map(|index| groups[*index].clone())
            .collect();
        let mut compiled = build_direct(&branch, keys, resolve, context.clone())?;
        align(&mut compiled.relational, &layout, &canonical, &members)?;
        if let Some(merged) = &mut merged {
            merge(merged, compiled.relational)?;
        } else {
            merged = Some(compiled.relational);
        }
    }
    Ok(Compiled { result_columns: Vec::new(), relational: merged.context("missing grouping set")? })
}

fn align(
    ir: &mut Relational,
    layout: &Layout<'_>,
    canonical: &[usize],
    members: &BTreeSet<usize>,
) -> Result<()> {
    let Some(Node::Output { input, .. }) = ir.nodes.pop() else {
        anyhow::bail!("missing grouping set branch output");
    };
    let mut fields = Vec::new();
    let mut computed = Vec::new();
    let mut actual = ir.output.columns.iter();
    for (index, key) in layout.keys.iter().enumerate() {
        let included = members.contains(&canonical[index]);
        let value = if included {
            actual.next().context("missing grouping key output")?.column.clone()
        } else {
            let mut column = key.clone();
            column.name = format!("@grouping_null_{index}");
            column.nullable = true;
            computed
                .push(Computed { column: column.clone(), expression: Typed::grouping_null(key)? });
            column
        };
        fields.push(binding(value, index));
    }
    for value in actual {
        fields.push(binding(value.column.clone(), fields.len()));
    }
    for (index, arguments) in layout.masks.iter().enumerate().rev() {
        let mask = arguments
            .iter()
            .fold(0_i64, |mask, arg| (mask << 1) | i64::from(!members.contains(&canonical[*arg])));
        let column = ColumnRef {
            name: format!("@grouping_mask_{index}"),
            oid: 23,
            nullable: false,
            collation: 0,
            modifier: None,
            right: false,
        };
        computed.push(Computed { column: column.clone(), expression: Typed::grouping_mask(mask) });
        fields.push(binding(column, fields.len()));
    }
    ir.output.columns = fields
        .iter()
        .map(|field| OutputColumn {
            column: field.output.clone(),
            label: field.output.name.clone(),
        })
        .collect();
    ir.nodes.push(Node::Map { id: "grouping_values".into(), input, computed });
    ir.nodes.push(Node::Rebind {
        id: "grouping_aligned".into(),
        input: "grouping_values".into(),
        fields,
    });
    ir.nodes.push(Node::Output { id: "project".into(), input: "grouping_aligned".into() });
    Ok(())
}

fn binding(input: ColumnRef, index: usize) -> Binding {
    let output = ColumnRef { name: format!("@grouping_output_{index}"), ..input };
    Binding { input, output }
}

fn merge(ir: &mut Relational, mut branch: Relational) -> Result<()> {
    let Some(Node::Output { input: left, .. }) = ir.nodes.pop() else {
        anyhow::bail!("missing prior grouping set output");
    };
    let Some(Node::Output { input: right, .. }) = branch.nodes.pop() else {
        anyhow::bail!("missing next grouping set output");
    };
    let prefix = format!("stage_{}_", ir.nodes.len());
    composition::append(&mut ir.nodes, branch.nodes, "source");
    for (out, next) in ir.output.columns.iter_mut().zip(branch.output.columns) {
        ensure!(out.column.oid == next.column.oid, "grouping set output type mismatch");
        out.column.nullable |= next.column.nullable;
    }
    let id = format!("{prefix}grouping_union");
    ir.nodes.push(Node::Union { id: id.clone(), left, right: format!("{prefix}{right}") });
    ir.nodes.push(Node::Output { id: "project".into(), input: id });
    Ok(())
}
