//! Lazy searched `CASE`; result promotion happens after branch selection.
use super::{ColumnRef, Name, Typed, Value};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(super) fn bind(
    value: &pg::CaseExpr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{node, optional};
    ensure!(value.arg.is_none() && value.xpr.is_none(), "only searched circuit CASE supported");
    ensure!((1..=32).contains(&value.args.len()), "invalid CASE branch count");
    let mut branches = Vec::new();
    for arm in &value.args {
        let NodeEnum::CaseWhen(arm) = node(arm)? else {
            anyhow::bail!("invalid CASE arm");
        };
        ensure!(arm.xpr.is_none(), "invalid CASE arm metadata");
        let condition = crate::compiler::syntax::predicate(arm.expr.as_deref())?
            .context("missing CASE condition")?;
        let condition = crate::compiler::expression::bind(condition, resolve)?;
        let result = super::bind(optional(arm.result.as_deref())?, resolve, depth + 1)?;
        branches.push((condition, result));
    }
    let otherwise = value
        .defresult
        .as_deref()
        .map(|value| super::bind(value, resolve, depth + 1))
        .transpose()?
        .unwrap_or(Typed { oid: 0, value: Value::Literal(None) });
    let types =
        branches.iter().map(|(_, value)| value.oid).chain([otherwise.oid]).collect::<Vec<_>>();
    ensure!(types.iter().any(|oid| *oid != 0), "circuit CASE needs resolved integral result");
    Ok(Typed {
        oid: super::types::common(&types)?,
        value: Value::Case { branches, otherwise: Box::new(otherwise) },
    })
}
