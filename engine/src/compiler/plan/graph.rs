//! Edge bookkeeping shared by the plan rewrites.
use crate::compiler::{
    Predicate,
    expression::Expr,
    relational::{Node, Relational, field},
};
use crate::source::Contract;
use anyhow::{Context, Result};
use std::collections::BTreeSet;

pub(super) struct Graph<'a> {
    pub(super) ir: &'a mut Relational,
    pub(super) contract: &'a Contract,
    fresh: usize,
    pub(super) derived: BTreeSet<Expr>,
    pub(super) single_globals: BTreeSet<String>,
    pub(super) flat_arms: BTreeSet<String>,
}
impl<'a> Graph<'a> {
    pub(super) const fn new(ir: &'a mut Relational, contract: &'a Contract) -> Self {
        Self {
            ir,
            contract,
            fresh: 0,
            derived: BTreeSet::new(),
            single_globals: BTreeSet::new(),
            flat_arms: BTreeSet::new(),
        }
    }
    pub(super) fn ensure_unique(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for node in &self.ir.nodes {
            anyhow::ensure!(seen.insert(node.id()), "duplicate plan edge {}", node.id());
        }
        Ok(())
    }
    pub(super) fn node(&self, id: &str) -> Result<&Node> {
        self.ir.nodes.iter().find(|node| node.id() == id).context("missing plan edge")
    }
    pub(super) fn node_mut(&mut self, id: &str) -> Result<&mut Node> {
        self.ir.nodes.iter_mut().find(|node| node.id() == id).context("missing plan edge")
    }
    pub(super) fn consumers(&self, edge: &str) -> impl Iterator<Item = &Node> {
        self.ir.nodes.iter().filter(move |node| node.inputs().contains(&edge))
    }
    pub(super) fn only_consumer(&self, edge: &str) -> Option<&Node> {
        let mut consumers = self.consumers(edge);
        let first = consumers.next()?;
        consumers.next().is_none().then_some(first)
    }
    pub(super) fn ids(&self, select: impl Fn(&Node) -> bool) -> Vec<String> {
        self.ir.nodes.iter().filter(|node| select(node)).map(|node| node.id().to_owned()).collect()
    }
    pub(super) fn fresh(&mut self, kind: &str) -> String {
        self.fresh += 1;
        format!("plan_{kind}_{}", self.fresh)
    }
    pub(super) fn remove(&mut self, id: &str) -> Result<Node> {
        let position =
            self.ir.nodes.iter().position(|node| node.id() == id).context("missing plan edge")?;
        Ok(self.ir.nodes.remove(position))
    }
    pub(super) fn redirect(&mut self, from: &str, to: &str) {
        for node in &mut self.ir.nodes {
            node.rewrite(&|id| id.into(), &|edge| {
                if edge == from { to.into() } else { edge.into() }
            });
        }
    }
    /// Filter what `consumer` reads from `edge`, leaving other consumers of `edge` alone.
    pub(super) fn filter_edge(
        &mut self,
        consumer: &str,
        edge: &str,
        values: Vec<Expr>,
    ) -> Result<()> {
        let Some(predicate) = Predicate::from_conjuncts(values) else { return Ok(()) };
        anyhow::ensure!(
            self.node(consumer)?.inputs().contains(&edge),
            "plan filter target no longer reads input"
        );
        let id = self.fresh("filter");
        self.node_mut(consumer)?.rewrite(&|id| id.into(), &|input| {
            if input == edge { id.clone() } else { input.into() }
        });
        self.ir.nodes.push(Node::Filter { id, input: edge.into(), predicate });
        Ok(())
    }
    /// The conjuncts a filter keeps; a filter that keeps none leaves the graph.
    pub(super) fn keep(&mut self, id: &str, values: Vec<Expr>) -> Result<()> {
        if let Some(kept) = Predicate::from_conjuncts(values) {
            let Node::Filter { predicate, .. } = self.node_mut(id)? else {
                anyhow::bail!("missing plan filter")
            };
            *predicate = kept;
            return Ok(());
        }
        let Node::Filter { input, .. } = self.remove(id)? else {
            anyhow::bail!("missing plan filter")
        };
        self.redirect(id, &input);
        Ok(())
    }
    /// Columns an edge exposes; empty where its layout is not known, which blocks a move.
    pub(super) fn columns(&self, edge: &str) -> Result<BTreeSet<String>> {
        Ok(match self.node(edge)? {
            Node::Source { source, .. } => self.source_columns(*source)?,
            Node::Constant { columns, .. } => {
                columns.iter().map(|column| column.reference.name.clone()).collect()
            }
            Node::Filter { input, .. }
            | Node::EquijoinKey { input, .. }
            | Node::CrossKey { input, .. }
            | Node::LeftJoinInput { input, .. }
            | Node::Empty { input, .. }
            | Node::Sorted { input, .. }
            | Node::Existence { input, .. } => self.columns(input)?,
            Node::Union { left, .. } => self.columns(left)?,
            Node::Map { input, computed, .. } => {
                let mut out = self.columns(input)?;
                out.extend(computed.iter().map(|value| value.column.name.clone()));
                out
            }
            Node::Rebind { fields, .. } => {
                fields.iter().map(|field| field.output.name.clone()).collect()
            }
            Node::Join { left, right, .. } => {
                let mut out = self.columns(left)?;
                out.extend(self.columns(right)?);
                out
            }
            Node::LeftJoin { spec, .. } => spec
                .left
                .iter()
                .chain(&spec.right)
                .chain(spec.scalar.iter())
                .map(|value| value.name.clone())
                .collect(),
            Node::Mark { input, spec, .. } => {
                let mut out = self.columns(input)?;
                out.insert(spec.output.name.clone());
                out
            }
            _ => BTreeSet::new(),
        })
    }
    fn source_columns(&self, index: usize) -> Result<BTreeSet<String>> {
        let source = self.ir.sources.get(index).context("missing plan source")?;
        let relation = self
            .contract
            .relations
            .iter()
            .find(|r| r.schema == source.schema && r.table == source.table)
            .context("plan source differs from contract")?;
        Ok(relation.columns.iter().map(|column| field(index, &column.name)).collect())
    }
    /// Keep the nodes the output reaches, inputs before consumers.
    pub(super) fn sort(&mut self) -> Result<()> {
        let output = self.ir.nodes.iter().find(|node| matches!(node, Node::Output { id, .. } if !self.ir.nodes.iter().any(|other| other.inputs().contains(&id.as_str()))));
        let output = output.context("missing plan output")?.id().to_owned();
        let order =
            self.ancestors(&output)?.iter().map(|node| node.id().to_owned()).collect::<Vec<_>>();
        let mut nodes = std::mem::take(&mut self.ir.nodes);
        self.ir.nodes = order
            .iter()
            .map(|id: &String| {
                let position = nodes.iter().position(|node| node.id() == id);
                position.map(|position| nodes.swap_remove(position)).context("missing plan edge")
            })
            .collect::<Result<_>>()?;
        Ok(())
    }
    /// Reachable nodes once each, inputs before consumers, including the edge itself.
    pub(super) fn ancestors(&self, edge: &str) -> Result<Vec<&Node>> {
        let mut order = Vec::new();
        self.visit(edge, &mut BTreeSet::new(), &mut order)?;
        Ok(order)
    }
    pub(super) fn sources(&self, edge: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .ancestors(edge)?
            .iter()
            .filter(|node| matches!(node, Node::Source { .. }))
            .map(|node| node.id().to_owned())
            .collect())
    }
    fn visit<'b>(
        &'b self,
        id: &str,
        seen: &mut BTreeSet<String>,
        order: &mut Vec<&'b Node>,
    ) -> Result<()> {
        if seen.insert(id.into()) {
            let node = self.node(id)?;
            for input in node.inputs() {
                self.visit(input, seen, order)?;
            }
            order.push(node);
        }
        Ok(())
    }
}
