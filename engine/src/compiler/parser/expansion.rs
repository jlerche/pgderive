//! Bounded constant integer series in a single-source CROSS JOIN.
use super::{Table, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
#[derive(Clone)]
pub(in crate::compiler) struct Series {
    pub alias: String,
    pub column: String,
    pub ordinal: Option<String>,
    pub spec: crate::compiler::expansion::Series,
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
        !function.lateral && !function.is_rowsfrom && function.coldeflist.is_empty(),
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
    let spec = bounds(call, function.ordinality)?;
    let (alias, columns) = match &function.alias {
        Some(alias) => (alias.aliasname.clone(), super::expressions::names(&alias.colnames)?.0),
        None => ("generate_series".into(), Vec::new()),
    };
    ensure!(
        columns.len() <= if function.ordinality { 2 } else { 1 },
        "too many series column aliases"
    );
    let mut columns = columns.into_iter();
    let column = columns.next().unwrap_or_else(|| alias.clone());
    let ordinal =
        function.ordinality.then(|| columns.next().unwrap_or_else(|| "ordinality".into()));
    ensure!(ordinal.as_ref() != Some(&column), "duplicate series column alias");
    Ok(Series { alias, column, ordinal, spec })
}

fn bounds(call: &pg::FuncCall, ordinality: bool) -> Result<crate::compiler::expansion::Series> {
    let names = super::expressions::names(&call.funcname)?.0;
    ensure!(
        matches!(names.as_slice(),[name] if name=="generate_series")
            || matches!(names.as_slice(),[schema,name] if schema=="pg_catalog"&&name=="generate_series"),
        "only generate_series expansion supported"
    );
    super::scalar::validate_call(call)?;
    let (start, end, step) = match call.args.as_slice() {
        [start, end] => (integer(start)?, integer(end)?, (Some(1), 0)),
        [start, end, step] => (integer(start)?, integer(end)?, integer(step)?),
        _ => anyhow::bail!("series requires two or three integral constants"),
    };
    crate::compiler::expansion::Series::new(start, end, step, ordinality)
}

fn integer(value: &Node) -> Result<(Option<i64>, u32)> {
    if let NodeEnum::TypeCast(cast) = node(value)? {
        let name = cast.type_name.as_ref().context("missing series argument type")?;
        ensure!(super::expressions::unmodified_type(name), "modified series argument type");
        let names = super::expressions::names(&name.names)?.0;
        let value = literal(optional(cast.arg.as_deref())?)?;
        let oid = match super::expressions::builtin_name(&names) {
            Some("int2") if value.is_none_or(|value| i16::try_from(value).is_ok()) => 21,
            Some("int4") if value.is_none_or(|value| i32::try_from(value).is_ok()) => 23,
            Some("int8") => 20,
            _ => anyhow::bail!("series requires in-range integral constants"),
        };
        return Ok((value, oid));
    }
    let value = literal(value)?;
    Ok((value, value.map_or(0, |value| if i32::try_from(value).is_ok() { 23 } else { 20 })))
}

fn literal(value: &Node) -> Result<Option<i64>> {
    let NodeEnum::AConst(value) = node(value)? else {
        anyhow::bail!("series requires integral constant arguments");
    };
    if value.isnull {
        return Ok(None);
    }
    match &value.val {
        Some(pg::a_const::Val::Ival(value)) => Ok(Some(i64::from(value.ival))),
        Some(pg::a_const::Val::Fval(value)) => {
            Ok(Some(value.fval.parse().context("series requires an integral literal")?))
        }
        _ => anyhow::bail!("series requires integral constant arguments"),
    }
}
