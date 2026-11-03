//! Drop unread computed columns and left joins nothing above can observe.
use super::graph::Graph;
use crate::compiler::{
    left_join::Spec,
    partition::{Function, Mode, Partition, statistics::key_scale},
    properties::Analysis,
    relational::{Node, barrier, column_names},
};
use crate::transaction::Row;
use anyhow::{Context, Result};
use std::collections::BTreeSet;

type Names = BTreeSet<String>;

/// Removals fold what they discard, so they wait until filters have substituted and settled.
pub(super) fn run(graph: &mut Graph<'_>, settled: bool) -> Result<bool> {
    graph.sort()?;
    let origin = computed_origin(graph, true);
    let unflattened = unflattened_arms(graph)?;
    let single = &graph.single_globals;
    let (mut globals, mut unread_marks) = (Vec::new(), Vec::new());
    let mut required =
        graph.ir.output.columns.iter().map(|output| output.column.name.clone()).collect::<Names>();
    let mut changed = false;
    let mut removable = Vec::new();
    for node in graph.ir.nodes.iter_mut().rev() {
        if let Node::LeftJoin { id, spec, .. } = node
            && !spec.full
            && spec.right.iter().all(|column| !required.contains(&column.name))
        {
            removable.push(id.clone());
        }
        if unread_global(node, &required, single) {
            globals.push(node.id().to_owned());
        }
        if let Node::Mark { id, spec, .. } = node
            && !spec.semijoin
            && !required.contains(&spec.output.name)
        {
            unread_marks.push(id.clone());
        }
        if !unflattened.contains(node.id()) {
            changed |= trim(node, &origin, &required);
        }
        required.extend(reads(node)?);
    }
    changed |= remove_empty(graph)?;
    if settled && !changed {
        changed |= remove_joins(graph, &removable)?;
        changed |= replace_globals(graph, &globals)?;
    }
    changed |= remove_marks(graph, &unread_marks)?;
    graph.sort()?;
    Ok(changed)
}
/// `is_simple_union_all` refuses arms of differing types; such a set operation stays a subquery,
/// whose outputs `remove_unused_subquery_outputs` keeps.
fn unflattened_arms(graph: &Graph<'_>) -> Result<BTreeSet<String>> {
    let mut pinned = BTreeSet::new();
    for id in graph.ids(|node| matches!(node, Node::Union { .. })) {
        let mut arms = Vec::new();
        arm_rebinds(graph, &id, &mut arms)?;
        let mut mixed = false;
        for arm in &arms {
            mixed |= super::push::coerced_rebind(graph, arm)?;
        }
        for arm in arms {
            if mixed || failing_output(graph, &arm)? {
                pinned.insert(arm);
            }
        }
    }
    Ok(pinned)
}
/// `is_safe_append_member`: an arm of one relation with no WHERE flattens into an appendrel, whose
/// outputs fold read or not. Taken from the plan as bound, before filters move onto arms.
pub(super) fn flat_arms(graph: &Graph<'_>) -> Result<Names> {
    let mut flat = Names::new();
    for id in graph.ids(|node| matches!(node, Node::Union { .. })) {
        let mut arms = Vec::new();
        arm_rebinds(graph, &id, &mut arms)?;
        for arm in arms {
            if one_relation(graph, &arm)? {
                flat.insert(arm);
            }
        }
    }
    Ok(flat)
}
fn one_relation(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    match graph.node(edge)? {
        Node::Source { .. } | Node::Constant { .. } => Ok(true),
        Node::Rebind { id, .. } if barrier(id) => Ok(false),
        Node::Map { input, .. } | Node::Rebind { input, .. } => one_relation(graph, input),
        _ => Ok(false),
    }
}
/// A flattened arm's failing output stays until `fold_all`, which reports it if it is planned.
fn failing_output(graph: &Graph<'_>, arm: &str) -> Result<bool> {
    if !graph.flat_arms.contains(arm) {
        return Ok(false);
    }
    let Node::Rebind { input, fields, .. } = graph.node(arm)? else { return Ok(false) };
    Ok(fields
        .iter()
        .any(|field| super::push::probe_expression(graph, input, &field.input).fold().is_err()))
}
fn arm_rebinds(graph: &Graph<'_>, edge: &str, out: &mut Vec<String>) -> Result<()> {
    match graph.node(edge)? {
        Node::Union { left, right, .. } => {
            arm_rebinds(graph, left, out)?;
            arm_rebinds(graph, right, out)?;
        }
        Node::Filter { input, .. } => arm_rebinds(graph, input, out)?,
        Node::Rebind { id, fields, .. }
            if fields.iter().any(|field| field.output.name.starts_with("@union_")) =>
        {
            out.push(id.clone());
        }
        _ => {}
    }
    Ok(())
}
/// A global aggregate none of whose results are read returns its one row without its input.
fn unread_global(node: &Node, required: &Names, single: &Names) -> bool {
    let (Node::Partition { spec, .. } | Node::Finalize { spec, .. }) = node else { return false };
    single.contains(node.id())
        && matches!(&spec.mode, Mode::Grouped { keys } if keys.is_empty())
        && spec.aggregates.iter().all(|value| !required.contains(&value.field))
}
/// `PostgreSQL` skips only an input that flattens to one relation or appendrel, which it decides
/// before removing joins, so this is taken from the plan as bound.
pub(super) fn single_relation_globals(graph: &Graph<'_>) -> Result<Names> {
    let mut single = Names::new();
    for id in graph.ids(|node| matches!(node, Node::Partition { .. } | Node::Finalize { .. })) {
        let input = graph.node(&id)?.inputs().into_iter().map(str::to_owned).collect::<Vec<_>>();
        let mut flat = true;
        for edge in input {
            flat &= flattens(graph, &edge)?;
        }
        if flat {
            single.insert(id);
        }
    }
    Ok(single)
}
fn flattens(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    match graph.node(edge)? {
        Node::Source { .. } | Node::Constant { .. } => Ok(true),
        Node::Rebind { id, .. } if barrier(id) => Ok(false),
        Node::Mark { input, spec, .. } if !spec.semijoin => {
            let Node::Union { left, .. } = graph.node(input)? else { return Ok(false) };
            flattens(graph, left)
        }
        Node::Rebind { id, .. } if super::push::coerced_rebind(graph, id)? => Ok(false),
        Node::Filter { input, .. }
        | Node::Map { input, .. }
        | Node::Rebind { input, .. }
        | Node::PartitionBy { input, .. }
        | Node::LeftJoinInput { input, .. }
        | Node::Statistics { input, .. } => flattens(graph, input),
        Node::Union { left, right, .. } => {
            if is_set_arm(graph, left)? && is_set_arm(graph, right)? {
                return Ok(simple_arm(graph, left)? && simple_arm(graph, right)?);
            }
            Ok(flattens(graph, left)? && flattens(graph, right)?)
        }
        _ => Ok(false),
    }
}
fn replace_globals(graph: &mut Graph<'_>, globals: &[String]) -> Result<bool> {
    for id in globals {
        let below = graph.ancestors(id)?.iter().map(|node| node.id().to_owned()).collect::<Names>();
        for node in &mut graph.ir.nodes {
            if below.contains(node.id()) {
                super::dummy::fold_node(&mut node.clone())?;
            }
        }
        let source = below
            .iter()
            .find(|edge| graph.node(edge).is_ok_and(|node| matches!(node, Node::Source { .. })));
        let source = source.context("global aggregate reads no source")?.clone();
        graph.remove(id)?;
        graph.ir.nodes.push(Node::Constant {
            id: id.clone(),
            input: source,
            rows: vec![Row::new()],
        });
    }
    Ok(!globals.is_empty())
}
fn is_set_arm(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    Ok(match graph.node(edge)? {
        Node::Filter { input, .. } => is_set_arm(graph, input)?,
        Node::Rebind { fields, .. } => {
            fields.iter().any(|field| field.output.name.starts_with("@union_"))
        }
        _ => false,
    })
}
/// `is_simple_union_all`: any subquery can be an arm, but a nested non-ALL set operation cannot.
fn simple_arm(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    match graph.node(edge)? {
        Node::Rebind { id, .. } if super::push::coerced_rebind(graph, id)? => Ok(false),
        Node::Rebind { id, input, .. } if !barrier(id) => simple_arm(graph, input),
        Node::Filter { input, .. } | Node::Map { input, .. } => simple_arm(graph, input),
        Node::Union { left, right, .. } => {
            Ok(simple_arm(graph, left)? && simple_arm(graph, right)?)
        }
        Node::SetOperation { .. } => Ok(false),
        Node::SetDistinct { input, .. } => {
            let Node::PartitionBy { input, .. } = graph.node(input)? else { return Ok(true) };
            let Node::Union { left, .. } = graph.node(input)? else { return Ok(true) };
            Ok(!is_set_arm(graph, left)?)
        }
        _ => Ok(true),
    }
}
/// `remove_unused_subquery_outputs` drops a subplan nothing reads, before planning it.
fn remove_marks(graph: &mut Graph<'_>, marks: &[String]) -> Result<bool> {
    for id in marks {
        let Node::Mark { input, .. } = graph.remove(id)? else { anyhow::bail!("missing mark") };
        let Node::Union { left, .. } = graph.node(&input)? else {
            anyhow::bail!("missing mark union")
        };
        let Node::LeftJoinInput { input: below, .. } = graph.node(left)? else {
            anyhow::bail!("missing mark input")
        };
        let below = below.clone();
        graph.redirect(id, &below);
    }
    Ok(!marks.is_empty())
}
/// The markers only held their columns in place through the rewrites.
pub(super) fn remove_sorted(graph: &mut Graph<'_>) -> Result<()> {
    for id in graph.ids(|node| matches!(node, Node::Sorted { .. })) {
        let Node::Sorted { input, .. } = graph.remove(&id)? else {
            anyhow::bail!("missing sort marker")
        };
        graph.redirect(&id, &input);
    }
    Ok(())
}
pub(super) fn computed_origin(graph: &Graph<'_>, marks: bool) -> Names {
    let mut origin = Names::new();
    for node in &graph.ir.nodes {
        match node {
            Node::Map { computed, .. } => {
                origin.extend(computed.iter().map(|value| value.column.name.clone()));
            }
            Node::Mark { spec, .. } if marks && !spec.semijoin => {
                origin.insert(spec.output.name.clone());
            }
            Node::Rebind { fields, .. } => {
                for field in fields {
                    if origin.contains(&field.input.name) {
                        origin.insert(field.output.name.clone());
                    }
                }
            }
            Node::Partition { spec, .. }
            | Node::Statistics { spec, .. }
            | Node::Finalize { spec, .. } => {
                origin.extend(spec.aggregates.iter().map(|value| value.field.clone()));
            }
            _ => {}
        }
    }
    origin
}
fn trim(node: &mut Node, origin: &Names, required: &Names) -> bool {
    match node {
        Node::Map { computed, .. } => {
            let before = computed.len();
            computed.retain(|value| required.contains(&value.column.name));
            computed.len() != before
        }
        Node::Rebind { id, fields, .. } if !barrier(id) => {
            let before = fields.len();
            fields.retain(|field| {
                required.contains(&field.output.name) || !origin.contains(&field.input.name)
            });
            fields.len() != before
        }
        Node::LeftJoin { spec, .. } => {
            let live = |name: &str| required.contains(name) || !origin.contains(name);
            let before = spec.left.len() + spec.right.len();
            spec.left.retain(|column| live(&column.name));
            spec.right.retain(|column| live(&column.name));
            spec.left.len() + spec.right.len() != before
        }
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::Finalize { spec, .. } => trim_aggregates(spec, required),
        _ => false,
    }
}
/// `PostgreSQL` drops unread aggregates; a scope that reads none still keeps its rows and keys.
fn trim_aggregates(spec: &mut Partition, required: &Names) -> bool {
    if spec.aggregates.iter().any(|value| matches!(value.function, Function::CountDistinct)) {
        return false;
    }
    let before = spec.aggregates.clone();
    let spare = spec.aggregates.iter().find(|value| !key_scale(value)).cloned();
    spec.aggregates.retain(|value| required.contains(&value.field) || key_scale(value));
    if spec.aggregates.iter().all(key_scale)
        && let Some(mut spare) = spare
    {
        spare.function = if matches!(spec.mode, Mode::Ranking { .. }) {
            Function::RowNumber
        } else {
            Function::Count
        };
        spare.argument = None;
        spare.expression = None;
        spare.filter = None;
        spare.navigation = None;
        spec.aggregates.insert(0, spare);
    }
    spec.aggregates != before
}
fn reads(node: &Node) -> Result<Names> {
    let mut names = Names::new();
    match node {
        Node::Map { computed, .. } => {
            let mut reads = std::collections::BTreeMap::new();
            for value in computed {
                value.expression.reads(&mut reads);
            }
            names.extend(reads.into_keys());
        }
        Node::Rebind { fields, .. } => {
            names.extend(fields.iter().map(|field| field.input.name.clone()));
        }
        Node::LeftJoin { spec, .. } => {
            let unlisted = Spec { left: Vec::new(), right: Vec::new(), ..spec.clone() };
            names.extend(column_names(&unlisted)?);
        }
        node => names.extend(column_names(node)?),
    }
    Ok(names)
}
fn remove_empty(graph: &mut Graph<'_>) -> Result<bool> {
    let empty = graph.ids(|node| matches!(node, Node::Map { computed, .. } if computed.is_empty()));
    for id in &empty {
        let Node::Map { input, .. } = graph.remove(id)? else { anyhow::bail!("missing plan map") };
        graph.redirect(id, &input);
    }
    Ok(!empty.is_empty())
}
/// A left join is removable when at most one right row can match each left row.
fn remove_joins(graph: &mut Graph<'_>, candidates: &[String]) -> Result<bool> {
    if candidates.is_empty() {
        return Ok(false);
    }
    graph.sort()?;
    let analysis = Analysis::infer(graph.contract, graph.ir)?;
    let mut changed = false;
    for id in candidates {
        let Some(Node::LeftJoin { input, .. }) =
            graph.ir.nodes.iter().find(|node| node.id() == id).cloned()
        else {
            continue;
        };
        let Node::Union { left, right, .. } = graph.node(&input)?.clone() else { continue };
        let Node::LeftJoinInput { input: below, spec, .. } = graph.node(&right)?.clone() else {
            continue;
        };
        let keys = spec.keys.iter().map(|key| key.name.clone()).collect::<Vec<_>>();
        if distinct_for(graph, &analysis, &below, keys)? {
            let Node::LeftJoinInput { input: preserved, .. } = graph.node(&left)?.clone() else {
                continue;
            };
            fold_removed(graph, id, &below)?;
            graph.remove(id)?;
            graph.redirect(id, &preserved);
            changed = true;
        }
    }
    Ok(changed)
}
/// A pulled-up input's quals folded before `remove_useless_joins` ran; a subquery never planned.
fn fold_removed(graph: &Graph<'_>, join: &str, edge: &str) -> Result<()> {
    super::dummy::fold_condition(&mut graph.node(join)?.clone())?;
    let mut at = edge;
    loop {
        match graph.node(at)? {
            Node::Filter { input, .. } => {
                super::dummy::fold_condition(&mut graph.node(at)?.clone())?;
                at = input;
            }
            Node::Map { input, .. } => at = input,
            Node::Rebind { id, input, .. } if !barrier(id) => at = input,
            _ => return Ok(()),
        }
    }
}
/// `join_is_removable`: one base relation, or a subquery distinct on the join's own columns.
fn distinct_for(
    graph: &Graph<'_>,
    analysis: &Analysis,
    edge: &str,
    keys: Vec<String>,
) -> Result<bool> {
    let grouped = |columns: &[crate::compiler::ColumnRef]| {
        columns.iter().all(|column| keys.contains(&column.name))
    };
    Ok(match graph.node(edge)? {
        Node::Source { .. } => analysis.edge(edge)?.proves_key(&keys),
        Node::Filter { input, .. } => distinct_for(graph, analysis, input, keys)?,
        Node::Map { input, computed, .. } => {
            !computed.iter().any(|value| keys.contains(&value.column.name))
                && distinct_for(graph, analysis, input, keys)?
        }
        Node::Rebind { id, input, fields } if !barrier(id) => {
            let below = keys.iter().map(|key| {
                let field = fields.iter().find(|field| &field.output.name == key);
                field.map(|field| field.input.name.clone())
            });
            match below.collect::<Option<Vec<_>>>() {
                Some(below) => distinct_for(graph, analysis, input, below)?,
                None => false,
            }
        }
        Node::Partition { spec, .. }
        | Node::Finalize { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. } => {
            matches!(&spec.mode, Mode::Grouped { keys: groups } if grouped(groups))
        }
        Node::SetDistinct { columns, .. } => grouped(columns),
        _ => false,
    })
}
