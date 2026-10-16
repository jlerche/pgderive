//! Evaluate grouped argument expressions only after their aggregate FILTER.
use crate::compiler::{
    ColumnRef, Predicate, expression,
    parser::{Name, partition::Argument},
    scalar::{self, Computed},
    syntax,
    typed::{self, Typed},
};
use anyhow::{Result, ensure};
#[derive(Clone, Copy)]
pub(super) struct Context {
    pub scope: Option<usize>,
    pub count: bool,
}
pub(super) struct Bound {
    pub column: Option<ColumnRef>,
    pub filter: Option<Predicate>,
}
pub(super) fn bind(
    argument: Option<Argument>,
    filter: Option<syntax::Expr>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
    context: Context,
) -> Result<Bound> {
    let filter = filter.map(|value| expression::bind(value, resolve)).transpose()?;
    let Some(Argument::Expression(ast)) = argument else {
        let column = match argument {
            Some(Argument::Column(name)) => Some(resolve(&name)?),
            _ => None,
        };
        return Ok(Bound { column, filter: filter.map(Predicate::new) });
    };
    let mut expression = typed::bind(&ast, resolve, 0)?;
    if expression.oid() == 0 {
        ensure!(context.count, "untyped NULL aggregate input requires an explicit cast");
        expression = Typed::null_text();
    }
    if expression.oid() == 705 {
        expression = expression.unknown_text();
    }
    if let Some(filter) = filter {
        expression = expression.filtered(filter);
    }
    let column =
        scalar::register(expression, computed, &format!("@aggregate_input_{:?}_", context.scope))?;
    Ok(Bound { column: Some(column), filter: None })
}
