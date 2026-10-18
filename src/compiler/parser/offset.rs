//! Timestamp arithmetic restricted to `PostgreSQL`'s interval time component.
use super::{Name, column, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) fn eligible(value: &Node) -> Result<bool> {
    let NodeEnum::AExpr(expression) = node(value)? else { return Ok(false) };
    let Some(right) = expression.rexpr.as_deref() else {
        return Ok(false);
    };
    let NodeEnum::TypeCast(cast) = node(right)? else {
        return Ok(false);
    };
    let kind = cast.type_name.as_ref().context("missing timestamp offset type")?;
    Ok(matches!(node(optional(expression.lexpr.as_deref())?)?, NodeEnum::ColumnRef(_))
        && super::expressions::builtin_name(&super::expressions::names(&kind.names)?.0)
            == Some("interval"))
}
pub(in crate::compiler) fn parse(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<crate::compiler::ColumnRef>,
) -> Result<crate::compiler::scalar::Temporal> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("timestamp offset requires operator");
    };
    ensure!(expression.kind == i32::from(pg::AExprKind::AexprOp), "unsupported timestamp operator");
    let names = super::expressions::names(&expression.name)?.0;
    let [operator] = names.as_slice() else { anyhow::bail!("unsupported timestamp operator") };
    ensure!(matches!(operator.as_str(), "+" | "-"), "timestamp offset requires + or -");
    let interval = optional(expression.rexpr.as_deref())?;
    let duration = super::scalar::interval(interval, false)?;
    let input = resolve(&column(optional(expression.lexpr.as_deref())?)?)?;
    ensure!(matches!(input.oid, 1114 | 1184), "offset requires native timestamp input");
    Ok(crate::compiler::scalar::Temporal {
        input,
        bin: None,
        shift: None,
        subtract: if operator == "+" { -duration } else { duration },
    })
}
