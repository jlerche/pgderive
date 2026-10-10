//! Timestamp arithmetic restricted to `PostgreSQL`'s interval time component.
use super::{Name, column, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
#[derive(Clone)]
pub(in crate::compiler) struct Offset {
    pub input: Name,
    // Signed microseconds to subtract; calendar months/days are not accepted.
    pub subtract: i64,
}
pub(super) fn eligible(value: &Node) -> Result<bool> {
    let NodeEnum::AExpr(expression) = node(value)? else { return Ok(false) };
    Ok(expression
        .lexpr
        .as_deref()
        .is_some_and(|value| matches!(node(value), Ok(NodeEnum::ColumnRef(_))))
        && expression
            .rexpr
            .as_deref()
            .is_some_and(|value| matches!(node(value), Ok(NodeEnum::TypeCast(_)))))
}
pub(super) fn parse(value: &Node) -> Result<Offset> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("timestamp offset requires operator");
    };
    ensure!(expression.kind == i32::from(pg::AExprKind::AexprOp), "unsupported timestamp operator");
    let names = super::expressions::names(&expression.name)?.0;
    let [operator] = names.as_slice() else { anyhow::bail!("unsupported timestamp operator") };
    ensure!(matches!(operator.as_str(), "+" | "-"), "timestamp offset requires + or -");
    let interval = optional(expression.rexpr.as_deref())?;
    validate_time_component(interval)?;
    let duration = super::scalar::interval(interval)?;
    Ok(Offset {
        input: column(optional(expression.lexpr.as_deref())?)?,
        subtract: if operator == "+" { -duration } else { duration },
    })
}

fn validate_time_component(interval: &Node) -> Result<()> {
    let NodeEnum::TypeCast(cast) = node(interval)? else {
        anyhow::bail!("timestamp offset requires explicit interval");
    };
    let NodeEnum::AConst(literal) = node(optional(cast.arg.as_deref())?)? else {
        anyhow::bail!("timestamp offset requires constant interval");
    };
    let Some(pg::a_const::Val::Sval(text)) = &literal.val else {
        anyhow::bail!("timestamp offset requires string interval");
    };
    let unit = text.sval.split_whitespace().last().context("missing interval unit")?;
    ensure!(
        !matches!(unit.to_ascii_lowercase().as_str(), "day" | "days"),
        "timestamp offsets require time durations, not calendar days"
    );
    Ok(())
}
