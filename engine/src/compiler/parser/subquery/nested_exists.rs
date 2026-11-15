//! Positive existential conjunction can flatten duplicate-insensitive witness rows.
use super::{ast, correlation, membership, nested_node};
use crate::compiler::parser::{
    Table,
    expressions::{builtin_name, column, names},
    node, optional, table,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use std::collections::BTreeSet;

#[derive(Default)]
struct Witnesses {
    sources: Vec<(Node, Table)>,
    qualifiers: BTreeSet<String>,
    active: Vec<Table>,
    filters: Vec<Node>,
}

pub(super) fn flatten(outer: &pg::SelectStmt, first: &pg::SelectStmt) -> Result<pg::SelectStmt> {
    let mut witnesses = Witnesses::default();
    for source in &outer.from_clause {
        witnesses.push_source(source)?;
    }
    witnesses.select(first)?;
    let (from, filters) = join_witnesses(witnesses.sources, witnesses.filters)?;
    let mut query = first.clone();
    membership::clear_exists_clauses(&mut query)?;
    query.from_clause = vec![from];
    query.where_clause = ast::logical(filters, true);
    Ok(query)
}

impl Witnesses {
    fn select(&mut self, query: &pg::SelectStmt) -> Result<()> {
        ensure!(self.sources.len() < 8, "nested EXISTS source count exceeded");
        ensure!(
            query.limit_offset.is_none()
                && !query.limit_count.as_deref().is_some_and(membership::zero_limit),
            "nested EXISTS requires row-preserving bounds"
        );
        membership::existence_value(query)?;
        let [source] = query.from_clause.as_slice() else {
            anyhow::bail!("nested EXISTS requires one native table per level");
        };
        let relation = self.push_source(source)?;
        self.sources.push((source.clone(), relation));
        if let Some(filter) = query.where_clause.as_deref() {
            self.conjunct(filter, 0)?;
        }
        self.active.pop();
        Ok(())
    }

    fn push_source(&mut self, source: &Node) -> Result<Table> {
        let relation = table(source).context("nested EXISTS requires native table qualifiers")?;
        let qualifier = relation
            .alias
            .as_ref()
            .or_else(|| relation.name.0.last())
            .context("missing nested EXISTS qualifier")?;
        ensure!(
            self.qualifiers.insert(qualifier.clone()),
            "nested EXISTS uses a shadowed table qualifier"
        );
        self.active.push(relation.clone());
        Ok(relation)
    }

    fn conjunct(&mut self, value: &Node, depth: usize) -> Result<()> {
        ensure!(depth < 32, "nested EXISTS predicate depth exceeded");
        match node(value)? {
            NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::AndExpr) => {
                for term in &value.args {
                    self.conjunct(term, depth + 1)?;
                }
            }
            NodeEnum::SubLink(link)
                if link.sub_link_type == i32::from(pg::SubLinkType::ExistsSublink) =>
            {
                ensure!(
                    link.testexpr.is_none() && link.oper_name.is_empty(),
                    "invalid EXISTS sublink"
                );
                let NodeEnum::SelectStmt(query) = node(optional(link.subselect.as_deref())?)?
                else {
                    anyhow::bail!("EXISTS requires SELECT");
                };
                self.select(query)?;
            }
            _ => {
                ensure!(!nested_node(value), "nested EXISTS must be a positive WHERE conjunct");
                qualified(value, &self.active, 0)?;
                self.filters.push(value.clone());
            }
        }
        Ok(())
    }
}

fn qualified(value: &Node, active: &[Table], depth: usize) -> Result<()> {
    ensure!(depth < 32, "nested EXISTS predicate depth exceeded");
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        let name = column(value)?;
        let (_, qualifier) = name.0.split_last().context("missing nested EXISTS column name")?;
        ensure!(
            !qualifier.is_empty() && active.iter().any(|table| table.matches_qualifier(qualifier)),
            "nested EXISTS requires a column qualified in the original scope"
        );
    }
    for child in correlation::children(value) {
        qualified(child, active, depth + 1)?;
    }
    Ok(())
}

fn join_witnesses(sources: Vec<(Node, Table)>, filters: Vec<Node>) -> Result<(Node, Vec<Node>)> {
    let mut conditions = vec![Vec::new(); sources.len()];
    let mut remaining = Vec::new();
    for filter in filters {
        if let Some(index) = joining_equality(&filter, &sources)? {
            conditions[index].push(filter);
        } else {
            remaining.push(filter);
        }
    }
    let mut inputs = sources.into_iter().zip(conditions);
    let ((first, _), _) = inputs.next().context("missing EXISTS source")?;
    let from = inputs.fold(first, |left, ((right, _), conditions)| {
        join(left, right, ast::logical(conditions, true))
    });
    Ok((from, remaining))
}

// Keep witness-to-witness equalities as native join keys. Outer
// references and other predicates retain the ordinary WHERE lowering.
fn joining_equality(value: &Node, sources: &[(Node, Table)]) -> Result<Option<usize>> {
    let NodeEnum::AExpr(value) = node(value)? else { return Ok(None) };
    if value.kind != i32::from(pg::AExprKind::AexprOp)
        || builtin_name(&names(&value.name)?.0) != Some("=")
    {
        return Ok(None);
    }
    let lookup = |node: Option<&Node>| -> Option<usize> {
        let name = column(node?).ok()?;
        let (_, qualifier) = name.0.split_last()?;
        sources.iter().position(|(_, table)| table.matches_qualifier(qualifier))
    };
    Ok(match (lookup(value.lexpr.as_deref()), lookup(value.rexpr.as_deref())) {
        (Some(left), Some(right)) if left != right => Some(left.max(right)),
        _ => None,
    })
}

fn join(left: Node, right: Node, quals: Option<Box<Node>>) -> Node {
    Node {
        node: Some(NodeEnum::JoinExpr(Box::new(pg::JoinExpr {
            jointype: i32::from(pg::JoinType::JoinInner),
            larg: Some(Box::new(left)),
            rarg: Some(Box::new(right)),
            quals,
            ..pg::JoinExpr::default()
        }))),
    }
}
