//! Searched CASE with typed predicates and native integral result arms.
use super::{node, optional};
use crate::compiler::syntax;
use anyhow::{Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
#[derive(Clone)]
pub(in crate::compiler) struct Case {
    pub arms: Vec<(syntax::Expr, syntax::Scalar)>,
    pub otherwise: syntax::Scalar,
}
pub(super) fn parse(value: &pg::CaseExpr) -> Result<Case> {
    ensure!(value.arg.is_none() && value.xpr.is_none(), "only searched CASE supported");
    ensure!((1..=32).contains(&value.args.len()), "CASE requires 1..=32 branches");
    let arms = value
        .args
        .iter()
        .map(|value| {
            let NodeEnum::CaseWhen(arm) = node(value)? else {
                anyhow::bail!("invalid CASE branch");
            };
            ensure!(arm.xpr.is_none(), "invalid CASE branch expression");
            let predicate = syntax::predicate(arm.expr.as_deref())?
                .ok_or_else(|| anyhow::anyhow!("missing CASE condition"))?;
            Ok((predicate, syntax::scalar(optional(arm.result.as_deref())?)?))
        })
        .collect::<Result<_>>()?;
    let otherwise =
        value.defresult.as_deref().map(syntax::scalar).transpose()?.unwrap_or(syntax::Scalar::Null);
    Ok(Case { arms, otherwise })
}
