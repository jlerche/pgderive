//! `PostgreSQL` correlated left lookup with an explicit deterministic LIMIT 1.
use super::{Columns, Name, Table, node, optional, projection_columns, table};
use crate::compiler::syntax::{self, Expr, Scalar};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Parsed {
    pub left: Table,
    pub right: Table,
    pub alias: String,
    pub inner_columns: Columns,
    pub columns: Columns,
    pub condition: Expr,
    pub order: Vec<(Name, bool, bool)>,
    pub predicate: Option<Expr>,
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    let [from] = select.from_clause.as_slice() else { return Ok(None) };
    let NodeEnum::JoinExpr(join) = node(from)? else { return Ok(None) };
    let NodeEnum::RangeSubselect(subquery) = node(optional(join.rarg.as_deref())?)? else {
        return Ok(None);
    };
    validate_join(join, select, subquery)?;
    let alias = subquery.alias.as_ref().context("lookup subquery requires an alias")?;
    ensure!(alias.colnames.is_empty(), "lookup column alias list unsupported");
    let NodeEnum::SelectStmt(inner) = node(optional(subquery.subquery.as_deref())?)? else {
        anyhow::bail!("lookup requires SELECT");
    };
    let (right, condition, order) = inner_shape(inner)?;
    Ok(Some(Parsed {
        left: table(optional(join.larg.as_deref())?)?,
        right,
        alias: alias.aliasname.clone(),
        inner_columns: projection_columns(&inner.target_list)?,
        columns: projection_columns(&select.target_list)?,
        condition,
        order,
        predicate: syntax::predicate(select.where_clause.as_deref())?,
    }))
}
fn validate_join(
    join: &pg::JoinExpr,
    select: &pg::SelectStmt,
    subquery: &pg::RangeSubselect,
) -> Result<()> {
    ensure!(
        join.jointype == i32::from(pg::JoinType::JoinLeft)
            && !join.is_natural
            && join.using_clause.is_empty()
            && join.join_using_alias.is_none()
            && join.alias.is_none()
            && join.rtindex == 0
            && select.group_clause.is_empty()
            && subquery.lateral,
        "lookup requires LEFT JOIN LATERAL without grouping/join modifiers"
    );
    ensure!(
        matches!(
            syntax::predicate(join.quals.as_deref())?,
            Some(Expr::Value(Scalar::Boolean(true)))
        ),
        "lookup requires ON true"
    );
    Ok(())
}
type Inner = (Table, Expr, Vec<(Name, bool, bool)>);
fn inner_shape(select: &pg::SelectStmt) -> Result<Inner> {
    ensure!(
        matches!(syntax::scalar(optional(select.limit_count.as_deref())?)?, Scalar::Integer(1))
            && select.limit_option == i32::from(pg::LimitOption::Count)
            && select.group_clause.is_empty(),
        "lookup requires LIMIT 1 without WITH TIES/grouping"
    );
    let mut plain = select.clone();
    plain.sort_clause.clear();
    plain.limit_count = None;
    plain.limit_option = i32::from(pg::LimitOption::Default);
    super::validate_query(&plain)?;
    let [source] = select.from_clause.as_slice() else {
        anyhow::bail!("lookup requires one native right source");
    };
    ensure!((1..=16).contains(&select.sort_clause.len()), "lookup requires 1..=16 order columns");
    Ok((
        table(source)?,
        syntax::predicate(select.where_clause.as_deref())?
            .context("lookup requires correlated WHERE")?,
        select.sort_clause.iter().map(super::partition::sort).collect::<Result<_>>()?,
    ))
}
