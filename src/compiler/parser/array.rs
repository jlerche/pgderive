//! Closed terminal text splitting with explicit C collation and array index.
use super::{Name, column, expressions::names, node, optional};
use crate::catalog::terminal::OutputMap;
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(super) fn target(value: &Node) -> Result<(Name, OutputMap, String)> {
    let NodeEnum::AIndirection(array) = node(value)? else {
        anyhow::bail!("expected terminal array selection");
    };
    let [index] = array.indirection.as_slice() else {
        anyhow::bail!("terminal selection requires exactly one array index");
    };
    let index = subscript(index)?;
    let NodeEnum::FuncCall(call) = node(optional(array.arg.as_deref())?)? else {
        anyhow::bail!("terminal array selection requires string_to_array");
    };
    let function = names(&call.funcname)?.0;
    ensure!(
        function == ["string_to_array"] || function == ["pg_catalog", "string_to_array"],
        "unsupported terminal array function"
    );
    super::scalar::validate_call(call)?;
    let [input, delimiter] = call.args.as_slice() else {
        anyhow::bail!("string_to_array requires collated native column and delimiter literal");
    };
    let NodeEnum::CollateClause(collation) = node(input)? else {
        anyhow::bail!("terminal splitting requires explicit pg_catalog.\"C\" collation");
    };
    ensure!(
        names(&collation.collname)?.0 == ["pg_catalog", "C"],
        "terminal splitting requires explicit pg_catalog.\"C\" collation"
    );
    let delimiter = super::scalar::string(delimiter)?;
    ensure!(delimiter.len() <= 64, "terminal delimiter exceeds 64 bytes");
    let input = column(optional(collation.arg.as_deref())?)?;
    Ok((input, OutputMap::ArrayElement { delimiter, index }, "string_to_array".into()))
}
fn subscript(value: &Node) -> Result<u16> {
    let NodeEnum::AIndices(index) = node(value)? else {
        anyhow::bail!("expected array index");
    };
    ensure!(!index.is_slice && index.lidx.is_none(), "terminal array slices unsupported");
    let NodeEnum::AConst(value) = node(optional(index.uidx.as_deref())?)? else {
        anyhow::bail!("terminal array index requires positive integer literal");
    };
    ensure!(!value.isnull, "NULL terminal array index unsupported");
    let pg::a_const::Val::Ival(value) = value.val.as_ref().context("missing array index")? else {
        anyhow::bail!("terminal array index requires positive integer literal");
    };
    ensure!((1..=1024).contains(&value.ival), "terminal array index requires 1..=1024");
    u16::try_from(value.ival).map_err(Into::into)
}
