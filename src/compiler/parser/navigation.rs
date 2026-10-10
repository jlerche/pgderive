//! Native-column LAG/LEAD arguments, offset and current-row fallback.
use super::{Name, column};
use crate::compiler::syntax::{self, Scalar};
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
#[derive(Clone)]
pub(in crate::compiler) struct Navigation {
    pub argument: Name,
    pub offset: Scalar,
    pub default: Scalar,
}
pub(super) fn parse(call: &pg::FuncCall) -> Result<Navigation> {
    ensure!(
        call.over.is_some() && !call.agg_star && call.agg_filter.is_none(),
        "LAG/LEAD require OVER and reject star/FILTER"
    );
    ensure!((1..=3).contains(&call.args.len()), "LAG/LEAD require one to three arguments");
    let argument = column(&call.args[0])?;
    let offset = call.args.get(1).map(syntax::scalar).transpose()?.unwrap_or(Scalar::Integer(1));
    let default = call.args.get(2).map(syntax::scalar).transpose()?.unwrap_or(Scalar::Null);
    ensure!(
        !matches!(offset, Scalar::Remainder(_, _)) && !matches!(default, Scalar::Remainder(_, _)),
        "LAG/LEAD offset/default expressions require native columns or literals"
    );
    Ok(Navigation { argument, offset, default })
}

pub(super) fn window(value: &pg::WindowDef) -> Result<super::partition::Window> {
    let mut window = super::partition::window(value, false)?;
    // Navigation ignores valid frame membership; retain only partition/order.
    window.ranking = true;
    window.peers = None;
    Ok(window)
}
