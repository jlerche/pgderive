//! Typed native LAG/LEAD operands and PostgreSQL-compatible result promotion.
use super::{ColumnRef, parser::Name, typed};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub enum Offset {
    Constant(Option<i32>),
    Column(ColumnRef),
}
#[derive(Clone, Serialize)]
pub enum Default {
    Constant(Option<String>),
    Column(ColumnRef),
}
#[derive(Clone, Serialize)]
pub struct Navigation {
    pub offset: Offset,
    pub default: Default,
    pub oid: u32,
}
pub(super) fn bind(
    parsed: &super::parser::navigation::Navigation,
    argument: &ColumnRef,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Navigation> {
    let offset = parsed
        .offset
        .as_deref()
        .map(|value| offset(value, resolve))
        .transpose()?
        .unwrap_or(Offset::Constant(Some(1)));
    let (default, oid) = default(parsed.default.as_deref(), argument.oid, resolve)?;
    Ok(Navigation { offset, default, oid })
}
fn offset(value: &pg_query::Node, resolve: &impl Fn(&Name) -> Result<ColumnRef>) -> Result<Offset> {
    let value = typed::navigation(value, resolve)?;
    if let Some(column) = value.as_column() {
        ensure!(matches!(column.oid, 21 | 23), "LAG/LEAD offset column requires int2/int4");
        return Ok(Offset::Column(column.clone()));
    }
    let integer = value.as_integer_literal();
    ensure!(integer.is_some() || value.oid() == 0, "unsupported LAG/LEAD offset");
    Ok(Offset::Constant(
        integer.map(i32::try_from).transpose().context("LAG/LEAD offset requires int4")?,
    ))
}
fn default(
    value: Option<&pg_query::Node>,
    argument: u32,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<(Default, u32)> {
    let Some(value) = value else {
        return Ok((Default::Constant(None), argument));
    };
    let value = typed::navigation(value, resolve)?;
    let (value, oid) = if let Some(column) = value.as_column() {
        (Default::Column(column.clone()), column.oid)
    } else {
        let (value, oid) = value.navigation_default(argument)?;
        (Default::Constant(value), oid)
    };
    Ok((value, common_type(argument, oid)?))
}
fn common_type(left: u32, right: u32) -> Result<u32> {
    if left == right {
        return Ok(left);
    }
    let integral = |oid| matches!(oid, 20 | 21 | 23 | 1700);
    if integral(left) && integral(right) {
        return Ok(if left == 1700 || right == 1700 {
            1700
        } else if left == 20 || right == 20 {
            20
        } else {
            23
        });
    }
    if matches!(left, 25 | 1043) && matches!(right, 25 | 1043) {
        return Ok(left);
    }
    anyhow::bail!("LAG/LEAD argument/default types need unsupported common-type coercion")
}
impl Offset {
    pub(crate) fn value(&self, row: &crate::transaction::Row) -> Result<Option<i32>> {
        match self {
            Self::Constant(value) => Ok(*value),
            Self::Column(column) => row
                .get(&column.name)
                .context("missing navigation offset")?
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(Into::into),
        }
    }
}
impl Default {
    pub(crate) fn value(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        match self {
            Self::Constant(value) => Ok(value.clone()),
            Self::Column(column) => {
                Ok(row.get(&column.name).context("missing navigation default")?.clone())
            }
        }
    }
}
