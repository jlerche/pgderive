//! Drop unread computed columns and left joins nothing above can observe.
use super::{graph::Graph, join::Join, scope::Scopes};
use crate::compiler::{
    Predicate,
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
        if settled {
            changed |= simplify_map(node);
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
fn unflattened_arms(graph: &Graph<'_>) -> Result<Names> {
    let scopes = Scopes::new(graph, graph.ir.nodes.iter().map(Node::id))?;
    let mut pinned = Names::new();
    for id in graph.ids(|node| matches!(node, Node::Union { .. })) {
        let arms = append_arms(graph, &id)?;
        let mut mixed = false;
        for arm in &arms {
            mixed |= !scopes.at(arm)?.coerced.is_empty();
        }
        for arm in arms {
            if mixed || failing_output(graph, &arm)? {
                pinned.insert(arm);
            }
        }
    }
    Ok(pinned)
}
/// Capture `PostgreSQL`'s flattening decisions before rewrites move filters or remove joins.
pub(super) fn provenance(graph: &Graph<'_>) -> Result<(Names, Names)> {
    let scopes = Scopes::new(graph, graph.ir.nodes.iter().map(Node::id))?;
    let mut single = Names::new();
    let mut flat = Names::new();
    for node in &graph.ir.nodes {
        match node {
            Node::Partition { id, input, .. } | Node::Finalize { id, input, .. }
                if scopes.at(input)?.flat =>
            {
                single.insert(id.clone());
            }
            Node::Union { id, .. } => {
                for arm in append_arms(graph, id)? {
                    if scopes.at(&arm)?.single {
                        flat.insert(arm);
                    }
                }
            }
            _ => {}
        }
    }
    Ok((single, flat))
}
/// Append arms are enumerated on demand; retaining every nested arm set would be quadratic.
fn append_arms(graph: &Graph<'_>, edge: &str) -> Result<Names> {
    match graph.node(edge)? {
        Node::Union { left, right, .. } => {
            let mut arms = append_arms(graph, left)?;
            arms.extend(append_arms(graph, right)?);
            Ok(arms)
        }
        Node::Filter { input, .. } => append_arms(graph, input),
        Node::Rebind { id, fields, .. }
            if fields.iter().any(|field| field.output.name.starts_with("@union_")) =>
        {
            Ok([id.clone()].into())
        }
        _ => Ok(Names::new()),
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
/// A global aggregate none of whose results are read returns its one row without its input.
fn unread_global(node: &Node, required: &Names, single: &Names) -> bool {
    let (Node::Partition { spec, .. } | Node::Finalize { spec, .. }) = node else { return false };
    single.contains(node.id())
        && matches!(&spec.mode, Mode::Grouped { keys } if keys.is_empty())
        && spec.aggregates.iter().all(|value| !required.contains(&value.field))
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
/// `remove_unused_subquery_outputs` drops a subplan nothing reads, before planning it.
fn remove_marks(graph: &mut Graph<'_>, marks: &[String]) -> Result<bool> {
    for id in marks {
        let Some(join) = Join::at(graph, id)? else { anyhow::bail!("missing mark") };
        graph.remove(id)?;
        graph.redirect(id, &join.left.input);
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
            // Borrowed predicate binding needs the side of every read, even when
            // a computed column is unused above the join.
            let predicate = spec.predicate.as_ref().map_or_else(Names::new, Predicate::read_names);
            let live = |name: &str| {
                required.contains(name) || predicate.contains(name) || !origin.contains(name)
            };
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
/// Decide lazy output branches before collecting their live inputs. Failed folding leaves
/// the original expression intact for the authoritative final folding phase.
fn simplify_map(node: &mut Node) -> bool {
    let Node::Map { computed, .. } = node else { return false };
    let mut changed = false;
    for value in computed {
        if let Ok(folded) = super::dummy::fold_computed(value)
            && folded != value.expression
        {
            value.expression = folded;
            changed = true;
        }
    }
    changed
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
        let Some(join) = Join::at(graph, id)? else { continue };
        let keys = join.right.keys.iter().map(|key| key.name.clone()).collect();
        if distinct_for(graph, &analysis, &join.right.input, keys)? {
            fold_removed(graph, id, &join.right.input)?;
            graph.remove(id)?;
            graph.redirect(id, &join.left.input);
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

#[cfg(test)]
mod tests {
    use super::{Names, trim};
    use crate::compiler::{
        ColumnRef, Predicate, expression::Expr, left_join::Spec, relational::Node, typed::Typed,
    };

    #[test]
    fn residual_reads_keep_computed_join_side_metadata() {
        let column =
            ColumnRef { right: false, name: "computed_right".into(), oid: 16, nullable: true };
        let mut join = Node::LeftJoin {
            id: "join".into(),
            input: "tagged".into(),
            spec: Spec {
                left: Vec::new(),
                right: vec![column.clone()],
                full: true,
                predicate: Some(Predicate::new(Expr::Value(Typed::column(column)))),
            },
        };
        let origin = Names::from(["computed_right".into()]);
        assert!(!trim(&mut join, &origin, &Names::new()));
        assert!(matches!(join, Node::LeftJoin {spec,..} if spec.right.len() == 1));
    }
}
