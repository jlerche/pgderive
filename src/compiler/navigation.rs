//! Typed native LAG/LEAD operands and PostgreSQL-compatible result promotion.
use super::{ColumnRef, parser::Name, syntax::Scalar};
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
    parsed: super::parser::navigation::Navigation,
    argument: &ColumnRef,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Navigation> {
    let offset = match parsed.offset {
        Scalar::Null => Offset::Constant(None),
        Scalar::Integer(value) => {
            Offset::Constant(Some(i32::try_from(value).context("LAG/LEAD offset requires int4")?))
        }
        Scalar::Column(name) => {
            let column = resolve(&name)?;
            ensure!(matches!(column.oid, 21 | 23), "LAG/LEAD offset column requires int2/int4");
            Offset::Column(column)
        }
        _ => anyhow::bail!("unsupported LAG/LEAD offset"),
    };
    let (default, oid) = default(parsed.default, argument.oid, resolve)?;
    Ok(Navigation { offset, default, oid })
}
fn default(
    value: Scalar,
    argument: u32,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<(Default, u32)> {
    let (value, oid) = match value {
        Scalar::Null => (Default::Constant(None), argument),
        Scalar::Column(name) => {
            let column = resolve(&name)?;
            let oid = column.oid;
            (Default::Column(column), oid)
        }
        Scalar::Integer(value) => (
            Default::Constant(Some(value.to_string())),
            if i32::try_from(value).is_ok() { 23 } else { 20 },
        ),
        Scalar::Boolean(value) => {
            (Default::Constant(Some(if value { "t" } else { "f" }.into())), 16)
        }
        Scalar::String(value) => {
            ensure!(
                matches!(argument, 25 | 1043),
                "string LAG/LEAD defaults require text/varchar; other input casts need typed expression lowering"
            );
            (Default::Constant(Some(value)), argument)
        }
        Scalar::Remainder(..) | Scalar::Difference(..) | Scalar::Interval(..) => {
            anyhow::bail!("unsupported LAG/LEAD default")
        }
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
