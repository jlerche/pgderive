//! Bounded constant integer series in a single-source CROSS JOIN.
use super::{Name, Table, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Series {
    pub alias: String,
    pub column: String,
    pub start: i32,
    pub end: i32,
}
pub(super) fn eligible(value: &Node) -> Result<bool> {
    let NodeEnum::JoinExpr(join) = node(value)? else {
        return Ok(false);
    };
    Ok(matches!(node(optional(join.rarg.as_deref())?)?, NodeEnum::RangeFunction(_)))
}
pub(super) fn source(value: &Node) -> Result<(Table, Option<Series>)> {
    if matches!(node(value)?, NodeEnum::RangeVar(_)) {
        return Ok((super::table(value)?, None));
    }
    let NodeEnum::JoinExpr(join) = node(value)? else {
        anyhow::bail!("single source or bounded CROSS JOIN required");
    };
    ensure!(
        join.jointype == i32::from(pg::JoinType::JoinInner)
            && !join.is_natural
            && join.using_clause.is_empty()
            && join.join_using_alias.is_none()
            && join.quals.is_none()
            && join.alias.is_none(),
        "series expansion requires CROSS JOIN"
    );
    let table = super::table(optional(join.larg.as_deref())?)?;
    let NodeEnum::RangeFunction(function) = node(optional(join.rarg.as_deref())?)? else {
        anyhow::bail!("bounded series required");
    };
    Ok((table, Some(series(function)?)))
}
fn series(function: &pg::RangeFunction) -> Result<Series> {
    ensure!(
        !function.lateral
            && !function.ordinality
            && !function.is_rowsfrom
            && function.coldeflist.is_empty(),
        "series modifiers unsupported"
    );
    let [function_node] = function.functions.as_slice() else {
        anyhow::bail!("one series function required");
    };
    let NodeEnum::List(list) = node(function_node)? else {
        anyhow::bail!("invalid series function");
    };
    let [call, definition] = list.items.as_slice() else {
        anyhow::bail!("invalid series function tuple");
    };
    ensure!(definition.node.is_none(), "series column definitions unsupported");
    let NodeEnum::FuncCall(call) = node(call)? else {
        anyhow::bail!("series function required");
    };
    let (start, end) = bounds(call)?;
    let alias = function.alias.as_ref().context("series requires relation and column aliases")?;
    let Name(columns) = super::expressions::names(&alias.colnames)?;
    let [column] = columns.as_slice() else {
        anyhow::bail!("series requires one column alias");
    };
    Ok(Series { alias: alias.aliasname.clone(), column: column.clone(), start, end })
}
fn bounds(call: &pg::FuncCall) -> Result<(i32, i32)> {
    let names = super::expressions::names(&call.funcname)?.0;
    ensure!(
        matches!(names.as_slice(),[name] if name=="generate_series")
            || matches!(names.as_slice(),[schema,name] if schema=="pg_catalog"&&name=="generate_series"),
        "only generate_series expansion supported"
    );
    super::scalar::validate_call(call)?;
    let [start, end] = call.args.as_slice() else {
        anyhow::bail!("series requires two int4 constants");
    };
    let start = integer(start)?;
    let end = integer(end)?;
    ensure!(
        (0..=1023).contains(&start) && (0..=1023).contains(&end),
        "series bounds require integers from 0 through 1023"
    );
    Ok((start, end))
}
fn integer(value: &Node) -> Result<i32> {
    let NodeEnum::AConst(value) = node(value)? else {
        anyhow::bail!("series bound requires int4 constant");
    };
    let Some(pg::a_const::Val::Ival(value)) = &value.val else {
        anyhow::bail!("series bound requires int4 constant");
    };
    Ok(value.ival)
}
