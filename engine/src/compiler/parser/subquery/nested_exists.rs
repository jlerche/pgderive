//! Positive existential conjunction can flatten duplicate-insensitive witness rows.
use super::{ast, correlation, membership, nested_node};
use crate::compiler::parser::{
    Table,
    expressions::{builtin_name, column, names},
    node, optional, table,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

#[derive(Default)]
struct Witnesses {
    sources: Vec<(Node, Table)>,
    active: Vec<ScopeBinding>,
    filters: Vec<Node>,
}

struct ScopeBinding {
    original: Table,
    alias: Option<String>,
}

pub(super) fn flatten(outer: &pg::SelectStmt, first: &pg::SelectStmt) -> Result<pg::SelectStmt> {
    let mut witnesses = Witnesses::default();
    for source in &outer.from_clause {
        witnesses.active.push(ScopeBinding { original: table(source)?, alias: None });
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
        self.push_witness(source)?;
        if let Some(filter) = query.where_clause.as_deref() {
            self.conjunct(filter, 0)?;
        }
        self.active.pop();
        Ok(())
    }

    fn push_witness(&mut self, source: &Node) -> Result<()> {
        let original = table(source).context("nested EXISTS requires native table qualifiers")?;
        let alias = format!("\0exists_witness_{}", self.sources.len());
        let mut rewritten = source.clone();
        let Some(NodeEnum::RangeVar(table)) = &mut rewritten.node else {
            anyhow::bail!("nested EXISTS requires a native table");
        };
        table.alias = Some(pg::Alias { aliasname: alias.clone(), ..pg::Alias::default() });
        let canonical = Table { name: original.name.clone(), alias: Some(alias.clone()) };
        self.sources.push((rewritten, canonical));
        self.active.push(ScopeBinding { original, alias: Some(alias) });
        Ok(())
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
                let mut rewritten = value.clone();
                rewrite_filter(&mut rewritten, &self.active, 0)?;
                self.filters.push(rewritten);
            }
        }
        Ok(())
    }
}

fn rewrite_filter(value: &mut Node, active: &[ScopeBinding], depth: usize) -> Result<()> {
    ensure!(depth < 32, "nested EXISTS predicate depth exceeded");
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        let name = column(value)?;
        let (field, qualifier) =
            name.0.split_last().context("missing nested EXISTS column name")?;
        let binding = active
            .iter()
            .rev()
            .find(|binding| !qualifier.is_empty() && binding.original.matches_qualifier(qualifier))
            .context("nested EXISTS requires a column qualified in the original scope")?;
        if let Some(alias) = &binding.alias {
            *value = ast::column(alias, field);
        }
        return Ok(());
    }
    let next = |child: &mut Node| rewrite_filter(child, active, depth + 1);
    match &mut value.node {
        Some(NodeEnum::BoolExpr(value)) => value.args.iter_mut().try_for_each(next)?,
        Some(NodeEnum::AExpr(value)) => {
            value
                .lexpr
                .as_deref_mut()
                .into_iter()
                .chain(value.rexpr.as_deref_mut())
                .try_for_each(next)?;
        }
        Some(NodeEnum::NullTest(value)) => {
            value.arg.as_deref_mut().into_iter().try_for_each(next)?;
        }
        Some(NodeEnum::BooleanTest(value)) => {
            value.arg.as_deref_mut().into_iter().try_for_each(next)?;
        }
        Some(NodeEnum::TypeCast(value)) => {
            value.arg.as_deref_mut().into_iter().try_for_each(next)?;
        }
        Some(NodeEnum::CollateClause(value)) => {
            value.arg.as_deref_mut().into_iter().try_for_each(next)?;
        }
        _ => {
            ensure!(correlation::children(value).is_empty(), "unsupported nested EXISTS predicate");
        }
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
