//! Lift bounded scalar/IN subqueries into the ordinary relational source tree.
mod ast;
mod correlation;
mod scalar;
mod scalar_input;
use super::{
    Parsed,
    expressions::{between_kind, distinct_kind, names, node, optional},
    native_join::Tree,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
struct Rewrite<'a> {
    select: &'a pg::SelectStmt,
    tree: Option<Tree>,
    links: usize,
    visited: usize,
    changed: bool,
    validation: Vec<Parsed>,
}
pub(super) fn parse(select: &pg::SelectStmt, visible: usize) -> Result<Option<Parsed>> {
    let mut checked = select.clone();
    let mut rewrite = Rewrite {
        select,
        tree: None,
        links: 0,
        visited: 0,
        changed: false,
        validation: Vec::new(),
    };
    for target in &mut checked.target_list {
        let original = target.clone();
        let previous = rewrite.links;
        rewrite.visit(target, false, 0)?;
        if rewrite.links != previous
            && let Some(NodeEnum::ResTarget(target)) = &mut target.node
            && target.name.is_empty()
        {
            target.name = super::label::output(super::target(&original)?)?;
        }
    }
    for condition in [&mut checked.where_clause, &mut checked.having_clause].into_iter().flatten() {
        rewrite.visit(condition, false, 0)?;
    }
    if !rewrite.changed {
        return Ok(None);
    }
    if let (Some(tree), Some(condition)) = (&mut rewrite.tree, checked.where_clause.as_deref()) {
        mark_semijoins(tree, condition);
    }
    let distinct = !checked.distinct_clause.is_empty();
    if distinct {
        ensure!(
            checked.distinct_clause.len() == 1 && checked.distinct_clause[0].node.is_none(),
            "DISTINCT ON unsupported"
        );
    }
    checked.distinct_clause.clear();
    super::validate_query(&checked)?;
    let mut parsed = super::partition::parse(&checked, visible)?;
    if let Some(tree) = rewrite.tree {
        parsed.source = super::partition::Source::Join(tree);
    }
    let parsed = if distinct {
        Parsed::Membership { parsed, distinct: true }
    } else {
        Parsed::Partition(parsed)
    };
    Ok(Some(if rewrite.validation.is_empty() {
        parsed
    } else {
        Parsed::With { bindings: rewrite.validation, inner: Box::new(parsed) }
    }))
}
/// `PostgreSQL` pulls up IN as a semijoin only when it is a top-level WHERE conjunct.
fn mark_semijoins(tree: &mut Tree, condition: &Node) {
    let Tree::Mark { left, alias, semijoin, .. } = tree else { return };
    *semijoin = conjunct(condition, &ast::column(alias, ast::VALUE));
    mark_semijoins(left, condition);
}
fn conjunct(condition: &Node, column: &Node) -> bool {
    match &condition.node {
        Some(NodeEnum::BoolExpr(value)) if value.boolop == i32::from(pg::BoolExprType::AndExpr) => {
            value.args.iter().any(|value| conjunct(value, column))
        }
        Some(NodeEnum::ColumnRef(value)) => {
            matches!(&column.node, Some(NodeEnum::ColumnRef(wanted)) if wanted.fields == value.fields)
        }
        _ => false,
    }
}
impl Rewrite<'_> {
    fn alias(&mut self) -> Result<String> {
        self.links += 1;
        ensure!(self.links <= 8, "subquery lift count exceeded");
        Ok(format!("\0subquery_{}", self.links))
    }
    fn take_tree(&mut self) -> Result<Tree> {
        if let Some(tree) = self.tree.take() {
            return Ok(tree);
        }
        if self.select.from_clause.is_empty() {
            let select = pg::SelectStmt {
                target_list: vec![ast::target(ast::boolean(true), "\0unit")],
                op: i32::from(pg::SetOperation::SetopNone),
                limit_option: i32::from(pg::LimitOption::Default),
                ..pg::SelectStmt::default()
            };
            return Ok(Tree::Derived {
                source: Box::new(super::query(&select)?),
                alias: "\0unit".into(),
            });
        }
        let [from] = self.select.from_clause.as_slice() else {
            anyhow::bail!("subquery outer scope requires one FROM tree");
        };
        super::native_join::tree(from, &mut 0, 0)
    }
    fn visit(&mut self, value: &mut Node, guarded: bool, depth: usize) -> Result<()> {
        self.visited += 1;
        ensure!(depth < 64 && self.visited <= 1024, "subquery expression budget exceeded");
        if let Some(NodeEnum::SubLink(link)) = &value.node {
            if link.sub_link_type == i32::from(pg::SubLinkType::ExistsSublink) {
                return Ok(());
            }
            ensure!(
                !guarded,
                "subqueries in lazy CASE/COALESCE/NULLIF/BETWEEN operands are unsupported; compute them in a derived scope first"
            );
            *value = self.link(link)?;
            self.changed = true;
            return Ok(());
        }
        if let Some(NodeEnum::AExpr(expression)) = &value.node
            && expression.kind == i32::from(pg::AExprKind::AexprIn)
        {
            let NodeEnum::List(list) = node(optional(expression.rexpr.as_deref())?)? else {
                anyhow::bail!("IN requires a scalar list");
            };
            ensure!((1..=64).contains(&list.items.len()), "IN list budget exceeded");
            let name = names(&expression.name)?;
            ensure!(
                matches!(name.0.as_slice(), [name] if matches!(name.as_str(), "="|"<>")),
                "unsupported IN operator"
            );
        }
        self.children(value, guarded, depth)
    }
    fn children(&mut self, value: &mut Node, guarded: bool, depth: usize) -> Result<()> {
        match value.node.as_mut() {
            Some(NodeEnum::ResTarget(value)) => {
                let original = value.val.as_deref().cloned();
                let links = self.links;
                if let Some(value) = value.val.as_deref_mut() {
                    self.visit(value, guarded, depth + 1)?;
                }
                if value.name.is_empty()
                    && let Some(original) = original
                    && self.links != links
                {
                    value.name = super::label::output(&original)?;
                }
            }
            Some(NodeEnum::AExpr(value)) => {
                // Either comparison can disappear before the executor reaches it.
                let guarded = guarded || between_kind(value.kind).is_some();
                let nullif = value.kind == i32::from(pg::AExprKind::AexprNullif);
                let lazy = !nullif
                    && distinct_kind(value.kind).is_none()
                    && (value.lexpr.as_deref().is_some_and(constant_null)
                        || value.rexpr.as_deref().is_some_and(constant_null));
                self.optional(value.lexpr.as_deref_mut(), guarded || lazy, depth)?;
                // A constant NULL left argument can make the right argument unreachable.
                self.optional(value.rexpr.as_deref_mut(), guarded || lazy || nullif, depth)?;
            }
            Some(NodeEnum::List(value)) => self.list(&mut value.items, guarded, depth)?,
            Some(NodeEnum::BoolExpr(value)) => {
                let lazy = value.args.iter().any(|argument| dominating(argument, value.boolop));
                self.list(&mut value.args, guarded || lazy, depth)?;
            }
            Some(NodeEnum::FuncCall(value)) => {
                let name = names(&value.funcname)?;
                let text_join = super::expressions::builtin_name(&name.0)
                    .and_then(crate::compiler::capabilities::text_join)
                    .is_some();
                let lazy = value.agg_filter.is_some()
                    || (!text_join && value.args.iter().any(constant_null));
                self.list(&mut value.args, guarded || lazy, depth)?;
                self.optional(value.agg_filter.as_deref_mut(), guarded, depth)?;
            }
            Some(NodeEnum::MinMaxExpr(value)) => self.list(&mut value.args, guarded, depth)?,
            Some(NodeEnum::CoalesceExpr(value)) => {
                for (index, value) in value.args.iter_mut().enumerate() {
                    self.visit(value, guarded || index > 0, depth + 1)?;
                }
            }
            Some(NodeEnum::TypeCast(value)) => {
                self.optional(value.arg.as_deref_mut(), guarded, depth)?;
            }
            Some(NodeEnum::CollateClause(value)) => {
                self.optional(value.arg.as_deref_mut(), guarded, depth)?;
            }
            Some(NodeEnum::BooleanTest(value)) => {
                self.optional(value.arg.as_deref_mut(), guarded, depth)?;
            }
            Some(NodeEnum::NullTest(value)) => {
                self.optional(value.arg.as_deref_mut(), guarded, depth)?;
            }
            Some(NodeEnum::CaseExpr(value)) => {
                // Folding can discard a simple CASE's test or even its first WHEN.
                let guarded = guarded || value.arg.is_some();
                self.optional(value.arg.as_deref_mut(), guarded, depth)?;
                for (index, arm) in value.args.iter_mut().enumerate() {
                    let Some(NodeEnum::CaseWhen(arm)) = arm.node.as_mut() else {
                        anyhow::bail!("invalid CASE arm");
                    };
                    self.optional(arm.expr.as_deref_mut(), guarded || index > 0, depth)?;
                    self.optional(arm.result.as_deref_mut(), true, depth)?;
                }
                self.optional(value.defresult.as_deref_mut(), true, depth)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn optional(&mut self, value: Option<&mut Node>, guarded: bool, depth: usize) -> Result<()> {
        if let Some(value) = value {
            self.visit(value, guarded, depth + 1)?;
        }
        Ok(())
    }
    fn list(&mut self, values: &mut [Node], guarded: bool, depth: usize) -> Result<()> {
        for value in values {
            self.visit(value, guarded, depth + 1)?;
        }
        Ok(())
    }
    fn link(&mut self, link: &pg::SubLink) -> Result<Node> {
        let NodeEnum::SelectStmt(query) = node(optional(link.subselect.as_deref())?)? else {
            anyhow::bail!("subquery requires SELECT");
        };
        ensure!(
            !nested_query(query),
            "nested scalar/IN subqueries inside a lifted subquery are unsupported"
        );
        ensure!(
            query.op != i32::from(pg::SetOperation::SetopNone)
                || !query.values_lists.is_empty()
                || query.target_list.len() == 1,
            "scalar/IN subquery requires exactly one output"
        );
        match pg::SubLinkType::try_from(link.sub_link_type)? {
            pg::SubLinkType::ExprSublink => {
                ensure!(
                    link.testexpr.is_none() && link.oper_name.is_empty(),
                    "invalid scalar sublink"
                );
                Ok(self.scalar((**query).clone())?.value)
            }
            pg::SubLinkType::AnySublink => {
                ensure!(link.oper_name.is_empty(), "only IN equality ANY subqueries are supported");
                let probe = optional(link.testexpr.as_deref())?.clone();
                if query.group_clause.is_empty() && scalar::global_aggregate(query)? {
                    let scalar = self.scalar((**query).clone())?;
                    return Ok(ast::case(
                        ast::is_null(scalar.present),
                        ast::boolean(false),
                        ast::comparison(probe, scalar.value, "="),
                    ));
                }
                self.membership((**query).clone(), probe)
            }
            _ => anyhow::bail!("unsupported scalar/IN sublink kind"),
        }
    }
    fn membership(&mut self, mut query: pg::SelectStmt, probe: Node) -> Result<Node> {
        let correlation = correlation::extract(&mut query)?;
        let alias = self.alias()?;
        if !correlation.pairs.is_empty() {
            ensure!(
                query.group_clause.is_empty()
                    && query.distinct_clause.is_empty()
                    && query.having_clause.is_none()
                    && query.sort_clause.is_empty()
                    && query.limit_count.is_none()
                    && query.limit_offset.is_none()
                    && query.op == i32::from(pg::SetOperation::SetopNone),
                "correlated IN requires a native projection/filter without grouping/DISTINCT/order/LIMIT"
            );
        }
        let mut conditions = Vec::new();
        for (index, (outer, inner)) in correlation.pairs.into_iter().enumerate() {
            let label = format!("\0key_{index}");
            query.target_list.push(ast::target(inner, &label));
            conditions.push(ast::comparison(outer, ast::column(&alias, &label), "="));
        }
        let left = self.take_tree()?;
        self.tree = Some(Tree::Mark {
            left: Box::new(left),
            right: Box::new(super::query(&query)?),
            probe: Box::new(probe),
            alias: alias.clone(),
            condition: ast::logical(conditions, true),
            semijoin: false,
        });
        Ok(ast::column(&alias, ast::VALUE))
    }
}
fn constant_null(value: &Node) -> bool {
    match &value.node {
        Some(NodeEnum::AConst(value)) => value.isnull,
        Some(NodeEnum::TypeCast(value)) => value.arg.as_deref().is_some_and(constant_null),
        Some(NodeEnum::CollateClause(value)) => value.arg.as_deref().is_some_and(constant_null),
        _ => false,
    }
}

fn dominating(value: &Node, operation: i32) -> bool {
    matches!(&value.node, Some(NodeEnum::AConst(value)) if matches!(&value.val, Some(pg::a_const::Val::Boolval(value)) if (operation == i32::from(pg::BoolExprType::AndExpr) && !value.boolval) || (operation == i32::from(pg::BoolExprType::OrExpr) && value.boolval)))
}

fn nested_query(query: &pg::SelectStmt) -> bool {
    query
        .target_list
        .iter()
        .chain(&query.from_clause)
        .chain(&query.group_clause)
        .chain(&query.sort_clause)
        .chain(
            [&query.where_clause, &query.having_clause, &query.limit_count, &query.limit_offset]
                .into_iter()
                .flatten()
                .map(Box::as_ref),
        )
        .any(nested_node)
        || [&query.larg, &query.rarg].into_iter().flatten().any(|query| nested_query(query))
}
pub(super) fn nested_node(value: &Node) -> bool {
    match &value.node {
        Some(NodeEnum::SubLink(_)) => true,
        Some(NodeEnum::SelectStmt(value)) => nested_query(value),
        Some(NodeEnum::RangeSubselect(value)) => value.subquery.as_deref().is_some_and(nested_node),
        Some(NodeEnum::JoinExpr(value)) => [&value.larg, &value.rarg, &value.quals]
            .into_iter()
            .flatten()
            .any(|value| nested_node(value)),
        _ => correlation::children(value).into_iter().any(nested_node),
    }
}
