//! Native-column LAG/LEAD arguments, offset and current-row fallback.
use super::{Name, column};
use crate::compiler::syntax::Expr;
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
#[derive(Clone)]
pub(in crate::compiler) struct Navigation {
    pub argument: Name,
    pub offset: Option<Expr>,
    pub default: Option<Expr>,
}
pub(super) fn parse(call: &pg::FuncCall) -> Result<Navigation> {
    ensure!(
        call.over.is_some() && !call.agg_star && call.agg_filter.is_none(),
        "LAG/LEAD require OVER and reject star/FILTER"
    );
    ensure!((1..=3).contains(&call.args.len()), "LAG/LEAD require one to three arguments");
    let argument = column(&call.args[0])?;
    let offset = call.args.get(1).cloned().map(Box::new);
    let default = call.args.get(2).cloned().map(Box::new);
    Ok(Navigation { argument, offset, default })
}

pub(super) fn window(value: &pg::WindowDef) -> Result<super::partition::Window> {
    let mut window = super::partition::window(value, false)?;
    // Navigation ignores valid frame membership; retain only partition/order.
    window.ranking = true;
    window.peers = None;
    Ok(window)
}
