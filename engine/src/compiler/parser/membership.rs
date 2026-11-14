//! Narrow native DISTINCT and equality-correlated existence composition.
use super::{
    Parsed,
    native_join::{self, Tree},
    node, optional,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn parse(select: &pg::SelectStmt, visible: usize) -> Result<Option<Parsed>> {
    let distinct = !select.distinct_clause.is_empty();
    if distinct {
        ensure!(
            select.distinct_clause.len() == 1 && select.distinct_clause[0].node.is_none(),
            "DISTINCT ON unsupported"
        );
    }
    let existence = select.where_clause.as_deref().map(exists).transpose()?.flatten();
    if !distinct && existence.is_none() {
        return Ok(None);
    }
    let mut checked = select.clone();
    checked.distinct_clause.clear();
    super::validate_query(&checked)?;
    if existence.is_some() {
        checked.where_clause = None;
    }
    let mut parsed = super::partition::parse(&checked, visible)?;
    if let Some((query, anti)) = existence {
        parsed.source = super::partition::Source::Join(existence_source(select, query, anti)?);
    }
    Ok(Some(Parsed::Membership { parsed, distinct }))
}
fn existence_source(select: &pg::SelectStmt, query: &pg::SelectStmt, anti: bool) -> Result<Tree> {
    existence_value(query)?;
    let [source] = select.from_clause.as_slice() else {
        anyhow::bail!("membership requires one native source")
    };
    let [right] = query.from_clause.as_slice() else {
        anyhow::bail!("EXISTS requires one native source")
    };
    let mut keys = Vec::new();
    native_join::computed_equalities(optional(query.where_clause.as_deref())?, &mut keys, 0)?;
    ensure!((1..=16).contains(&keys.len()), "EXISTS correlation key budget exceeded");
    Ok(Tree::Exists {
        left: Box::new(Tree::Source(super::table(source)?)),
        right: super::table(right)?,
        keys,
        anti,
    })
}

pub(super) fn existence_value(query: &pg::SelectStmt) -> Result<Node> {
    let mut shape = query.clone();
    clear_exists_clauses(&mut shape)?;
    super::validate_query(&shape)?;
    ensure!(
        query.group_clause.is_empty()
            && query.having_clause.is_none()
            && query.window_clause.is_empty(),
        "EXISTS grouping/HAVING/WINDOW unsupported"
    );
    for target in &query.target_list {
        ensure!(
            discardable(
                super::target(target)?,
                query.limit_count.as_deref().is_some_and(zero_limit)
            )?,
            "EXISTS projection requires a supported row-preserving expression"
        );
    }
    Ok(Node {
        node: Some(NodeEnum::AConst(pg::AConst {
            val: Some(pg::a_const::Val::Ival(pg::Integer { ival: 1 })),
            ..pg::AConst::default()
        })),
    })
}

fn discardable(value: &Node, total: bool) -> Result<bool> {
    if total {
        return Ok(matches!(node(value)?, NodeEnum::AConst(_) | NodeEnum::ColumnRef(_)));
    }
    row_preserving(value, 0)
}

// Native prepare validates the original clauses. A literal zero limit
// becomes an empty RHS before the existence mark runs.
pub(super) fn clear_exists_clauses(query: &mut pg::SelectStmt) -> Result<()> {
    let total = query.limit_count.as_deref().is_some_and(zero_limit);
    if let Some(limit) = query.limit_count.as_deref() {
        ensure!(
            query.limit_option == i32::from(pg::LimitOption::Count) && supported_limit(limit),
            "EXISTS requires a nonnegative literal LIMIT or LIMIT ALL"
        );
        query.limit_count = None;
        query.limit_option = i32::from(pg::LimitOption::Default);
    }
    for sort in &query.sort_clause {
        let NodeEnum::SortBy(sort) = node(sort)? else {
            anyhow::bail!("invalid EXISTS ORDER BY");
        };
        ensure!(!total || sort.use_op.is_empty(), "EXISTS requires a default sort operator");
        ensure!(
            discardable(optional(sort.node.as_deref())?, total)?,
            "EXISTS ORDER BY requires a supported row-preserving expression"
        );
    }
    for key in query.distinct_clause.iter().filter(|key| key.node.is_some()) {
        ensure!(discardable(key, total)?, "EXISTS DISTINCT ON requires a row-preserving key");
    }
    query.distinct_clause.clear();
    query.sort_clause.clear();
    Ok(())
}

const fn supported_limit(node: &Node) -> bool {
    matches!(&node.node, Some(NodeEnum::AConst(value))
        if value.isnull || matches!(&value.val, Some(pg::a_const::Val::Ival(n)) if n.ival >= 0))
}

pub(super) const fn zero_limit(node: &Node) -> bool {
    matches!(&node.node, Some(NodeEnum::AConst(value))
        if matches!(&value.val, Some(pg::a_const::Val::Ival(n)) if n.ival == 0))
}

// Dropping a scalar target does not change row existence. Calls are not
// admitted here because a raw FuncCall can name an aggregate or an SRF.
fn row_preserving(value: &Node, depth: usize) -> Result<bool> {
    ensure!(depth < 32, "EXISTS projection depth exceeded");
    match node(value)? {
        NodeEnum::AConst(_) | NodeEnum::ColumnRef(_) => Ok(true),
        NodeEnum::AExpr(value) => {
            let names = super::expressions::names(&value.name)?;
            Ok(value.kind == i32::from(pg::AExprKind::AexprOp)
                && matches!(
                    super::expressions::builtin_name(&names.0),
                    Some("+" | "-" | "*" | "/")
                )
                && value.lexpr.as_deref().is_some_and(integer_literal)
                && value.rexpr.as_deref().is_some_and(integer_literal))
        }
        NodeEnum::TypeCast(value) => row_preserving(optional(value.arg.as_deref())?, depth + 1),
        NodeEnum::CollateClause(value) => {
            row_preserving(optional(value.arg.as_deref())?, depth + 1)
        }
        _ => Ok(false),
    }
}

const fn integer_literal(node: &Node) -> bool {
    matches!(&node.node, Some(NodeEnum::AConst(value))
        if matches!(value.val, Some(pg::a_const::Val::Ival(_))))
}

pub(super) fn uses_keyed_existence(select: &pg::SelectStmt, condition: &Node) -> Result<bool> {
    let [source] = select.from_clause.as_slice() else { return Ok(false) };
    if super::table(source).is_err() {
        return Ok(false);
    }
    let Some((query, _)) = exists(condition)? else { return Ok(false) };
    if query.limit_count.as_deref().is_some_and(zero_limit) {
        return Ok(false);
    }
    super::subquery::correlation::only_cross_scope_equalities(query)
}

fn exists(value: &Node) -> Result<Option<(&pg::SelectStmt, bool)>> {
    let (value, anti) = match node(value)? {
        NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::NotExpr) => {
            let [argument] = value.args.as_slice() else { return Ok(None) };
            (argument, true)
        }
        _ => (value, false),
    };
    let NodeEnum::SubLink(value) = node(value)? else { return Ok(None) };
    if value.sub_link_type != i32::from(pg::SubLinkType::ExistsSublink) {
        return Ok(None);
    }
    ensure!(value.testexpr.is_none() && value.oper_name.is_empty(), "invalid EXISTS sublink");
    let NodeEnum::SelectStmt(query) = node(optional(value.subselect.as_deref())?)? else {
        anyhow::bail!("EXISTS requires SELECT")
    };
    Ok(Some((query, anti)))
}
