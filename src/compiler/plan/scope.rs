//! Scope properties shared by append pushdown and unused-output planning.
use super::{graph::Graph, push::probe_expression};
use crate::compiler::{
    relational::{Node, barrier},
    setop::Kind,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};

type Names = BTreeSet<String>;
#[derive(Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent SQL scope facts overlap: materialization permits a simple append member while blocking flattening"
)]
pub(super) struct Scope {
    /// One relation with no WHERE or evaluation boundary: safe append-member target folding.
    pub(super) single: bool,
    /// One relation or simple append: an unread global aggregate can skip its input.
    pub(super) flat: bool,
    /// A simple UNION ALL member; a materialized subquery can be one without being `flat`.
    pub(super) simple: bool,
    pub(super) arm: bool,
    /// Common-type alignment casts, keyed by the exposed column names.
    pub(super) coerced: Names,
    /// EXCEPT reachable through set-operation nodes; ordinary filters are boundaries.
    pub(super) except: bool,
}
pub(super) struct Scopes(BTreeMap<String, Scope>);
impl Scopes {
    pub(super) fn new<'a>(
        graph: &Graph<'_>,
        roots: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self> {
        let mut scopes = Self(BTreeMap::new());
        for root in roots {
            scopes.visit(graph, root)?;
        }
        Ok(scopes)
    }
    pub(super) fn at(&self, edge: &str) -> Result<&Scope> {
        self.0.get(edge).context("missing plan scope")
    }
    fn visit(&mut self, graph: &Graph<'_>, edge: &str) -> Result<()> {
        if self.0.contains_key(edge) {
            return Ok(());
        }
        let node = graph.node(edge)?;
        for input in node.inputs() {
            self.visit(graph, input)?;
        }
        let scope = self.classify(graph, node)?;
        self.0.insert(edge.to_owned(), scope);
        Ok(())
    }
    fn classify(&self, graph: &Graph<'_>, node: &Node) -> Result<Scope> {
        let empty = Scope::default();
        let inputs = node.inputs();
        let below = inputs.first().map_or(Ok(&empty), |input| self.at(input))?;
        let mut scope = Scope { simple: true, ..Scope::default() };
        match node {
            Node::Source { .. } | Node::Constant { .. } => {
                scope.single = true;
                scope.flat = true;
            }
            Node::Rebind { id, input, fields } => {
                scope.coerced = fields
                    .iter()
                    .filter(|field| {
                        field.input.name.contains("_cast_")
                            && probe_expression(graph, input, &field.input).casts_type()
                    })
                    .map(|field| field.output.name.clone())
                    .collect();
                scope.arm = fields.iter().any(|field| field.output.name.starts_with("@union_"));
                let coerced = scope.arm && !scope.coerced.is_empty();
                scope.single = !barrier(id) && below.single;
                scope.flat = !barrier(id) && !coerced && below.flat;
                scope.simple = !coerced && (barrier(id) || below.simple);
                scope.except = below.except;
            }
            Node::Map { .. } => {
                scope.single = below.single;
                scope.flat = below.flat;
                scope.simple = below.simple;
                scope.except = below.except;
            }
            Node::Filter { .. } => {
                scope.flat = below.flat;
                scope.simple = below.simple;
                scope.arm = below.arm;
            }
            Node::PartitionBy { .. } => {
                scope.flat = below.flat;
                scope.coerced.clone_from(&below.coerced);
                scope.except = below.except;
            }
            Node::Statistics { .. } | Node::LeftJoinInput { .. } => scope.flat = below.flat,
            Node::Mark { input, spec, .. } if !spec.semijoin => {
                if let Node::Union { left, .. } = graph.node(input)? {
                    scope.flat = self.at(left)?.flat;
                }
            }
            Node::Union { right, .. } => {
                let right = self.at(right)?;
                scope.simple = below.simple && right.simple;
                scope.flat =
                    if below.arm && right.arm { scope.simple } else { below.flat && right.flat };
                scope.except = below.except || right.except;
            }
            Node::SetOperation { spec, .. } => {
                scope.simple = false;
                scope.except = spec.kind == Kind::Except || below.except;
            }
            Node::SetDistinct { input, .. } => {
                scope.simple = self.distinct_arm(graph, input)?;
                scope.except = below.except;
            }
            Node::SetTag { .. } => {
                scope.coerced.clone_from(&below.coerced);
                scope.except = below.except;
            }
            Node::Sorted { .. } => scope.except = below.except,
            _ => {}
        }
        Ok(scope)
    }
    fn distinct_arm(&self, graph: &Graph<'_>, input: &str) -> Result<bool> {
        let Node::PartitionBy { input, .. } = graph.node(input)? else { return Ok(true) };
        let Node::Union { left, .. } = graph.node(input)? else { return Ok(true) };
        Ok(!self.at(left)?.arm)
    }
}
