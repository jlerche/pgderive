//! Carry qualified ancestor columns into intermediate witness queries.

use super::{
    ast, nested_node,
    parameter_groups::{ALIAS, Domain},
};
use crate::compiler::parser::{
    Table,
    expressions::{column, names},
    table,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn rewrite(outer: &pg::SelectStmt, query: &pg::SelectStmt) -> Result<pg::SelectStmt> {
    let mut rewritten = query.clone();
    let [outer_source] = outer.from_clause.as_slice() else { return Ok(rewritten) };
    let Ok(outer) = table(outer_source) else { return Ok(rewritten) };
    let [local_source] = rewritten.from_clause.as_slice() else { return Ok(rewritten) };
    let Ok(local) = table(local_source) else { return Ok(rewritten) };
    let mut references =
        References { outer, domain: Domain::default(), used: BTreeMap::new(), needs_domain: false };
    if let Some(filter) = rewritten.where_clause.as_deref_mut()
        && nested_node(filter)
    {
        references.visit(filter, std::slice::from_ref(&local), false, 0)?;
    }
    // A child that only needs its immediate parent can lift using the normal RHS.
    if !references.needs_domain {
        return Ok(query.clone());
    }
    let mut query = rewritten;
    ensure!(
        query.group_clause.is_empty()
            && query.having_clause.is_none()
            && query.op == i32::from(pg::SetOperation::SetopNone),
        "nested correlation parameter domain requires an ordinary SELECT"
    );
    let mut filter = Filter::partition(query.where_clause.take(), &local)?;
    let source = query.from_clause.pop().map(Box::new);
    query.from_clause = vec![Node {
        node: Some(NodeEnum::JoinExpr(Box::new(pg::JoinExpr {
            jointype: i32::from(pg::JoinType::JoinInner),
            larg: source,
            rarg: Some(Box::new(references.domain.relation(outer_source.clone()))),
            quals: ast::logical(filter.keys, true),
            ..pg::JoinExpr::default()
        }))),
    }];
    for (outer, parameter) in references.used.into_values() {
        let parameter_name = column(&parameter)?;
        let kind = if filter.strict.contains(&parameter_name.0) {
            // An ordinary join equality already ruled out NULL for this parameter.
            pg::AExprKind::AexprOp
        } else {
            pg::AExprKind::AexprNotDistinct
        };
        filter.remaining.push(ast::comparison_with_kind(outer, parameter, "=", kind));
    }
    query.where_clause = ast::logical(filter.remaining, true);
    Ok(query)
}

#[derive(Default)]
struct Filter {
    remaining: Vec<Node>,
    keys: Vec<Node>,
    strict: BTreeSet<Vec<String>>,
}

impl Filter {
    // Keep direct cross-input column equalities eligible for ordinary join keys.
    fn partition(input: Option<Box<Node>>, local: &Table) -> Result<Self> {
        let mut filter = Self::default();
        let mut pending = input.into_iter().map(|node| *node).collect::<Vec<_>>();
        while let Some(value) = pending.pop() {
            if let Some(NodeEnum::BoolExpr(logical)) = &value.node
                && logical.boolop == i32::from(pg::BoolExprType::AndExpr)
            {
                pending.extend(logical.args.iter().rev().cloned());
                continue;
            }
            if let Some(NodeEnum::AExpr(expr)) = &value.node
                && let Some(parameter) = connecting_parameter(expr, local)?
            {
                filter.strict.insert(parameter);
                filter.keys.push(value);
            } else {
                filter.remaining.push(value);
            }
        }
        Ok(filter)
    }
}

fn connecting_parameter(value: &pg::AExpr, local: &Table) -> Result<Option<Vec<String>>> {
    if value.kind != i32::from(pg::AExprKind::AexprOp) || names(&value.name)?.0 != ["="] {
        return Ok(None);
    }
    let side = |value: Option<&Node>| -> Option<(bool, Vec<String>)> {
        let name = column(value?).ok()?.0;
        let (_, qualifier) = name.split_last()?;
        if qualifier == [ALIAS] {
            Some((true, name))
        } else if !qualifier.is_empty() && local.matches_qualifier(qualifier) {
            Some((false, name))
        } else {
            None
        }
    };
    Ok(match (side(value.lexpr.as_deref()), side(value.rexpr.as_deref())) {
        (Some((true, name)), Some((false, _))) | (Some((false, _)), Some((true, name))) => {
            Some(name)
        }
        _ => None,
    })
}

struct References {
    outer: Table,
    domain: Domain,
    used: BTreeMap<Vec<String>, (Node, Node)>,
    needs_domain: bool,
}

impl References {
    fn visit(
        &mut self,
        value: &mut Node,
        local: &[Table],
        mixed: bool,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth < 32, "nested correlation expression depth exceeded");
        if matches!(value.node, Some(NodeEnum::ColumnRef(_))) {
            return self.column(value, local, mixed);
        }
        let mixed = mixed
            || matches!(&value.node, Some(NodeEnum::BoolExpr(expression))
            if expression.boolop != i32::from(pg::BoolExprType::AndExpr))
                && nested_node(value);
        let mut visit = |value: &mut Node| self.visit(value, local, mixed, depth + 1);
        match &mut value.node {
            Some(NodeEnum::SubLink(link)) => self.link(link, local, depth + 1),
            Some(NodeEnum::BoolExpr(expression)) => expression.args.iter_mut().try_for_each(visit),
            Some(NodeEnum::AExpr(expression)) => expression
                .lexpr
                .as_deref_mut()
                .into_iter()
                .chain(expression.rexpr.as_deref_mut())
                .try_for_each(visit),
            Some(NodeEnum::NullTest(test)) => {
                test.arg.as_deref_mut().into_iter().try_for_each(visit)
            }
            Some(NodeEnum::BooleanTest(test)) => {
                test.arg.as_deref_mut().into_iter().try_for_each(visit)
            }
            Some(NodeEnum::TypeCast(cast)) => {
                cast.arg.as_deref_mut().into_iter().try_for_each(visit)
            }
            Some(NodeEnum::CollateClause(collation)) => {
                collation.arg.as_deref_mut().into_iter().try_for_each(&mut visit)
            }
            _ => Ok(()),
        }
    }

    fn column(&mut self, value: &mut Node, local: &[Table], mixed: bool) -> Result<()> {
        let name = column(value)?.0;
        let Some((_, qualifier)) = name.split_last() else { return Ok(()) };
        if qualifier.is_empty()
            || !self.outer.matches_qualifier(qualifier)
            || local.iter().any(|relation| relation.matches_qualifier(qualifier))
        {
            return Ok(());
        }
        self.needs_domain |= mixed || local.len() > 1;
        ensure!(
            self.used.len() < 8 || self.used.contains_key(&name),
            "nested correlation parameter count exceeded"
        );
        let (_, parameter) = self.used.entry(name).or_insert_with(|| {
            let original = value.clone();
            let parameter = self.domain.add(original.clone());
            (original, parameter)
        });
        *value = parameter.clone();
        Ok(())
    }

    fn link(&mut self, link: &mut pg::SubLink, local: &[Table], depth: usize) -> Result<()> {
        let Some(NodeEnum::SelectStmt(select)) =
            link.subselect.as_deref_mut().and_then(|value| value.node.as_mut())
        else {
            return Ok(());
        };
        let [source] = select.from_clause.as_slice() else { return Ok(()) };
        let Ok(source) = table(source) else { return Ok(()) };
        let mut local = local.to_vec();
        local.push(source);
        if let Some(filter) = select.where_clause.as_deref_mut() {
            self.visit(filter, &local, false, depth)?;
        }
        Ok(())
    }
}
