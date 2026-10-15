//! Exact decimal multiplication deferred only across the final projection boundary.
use super::{Name, column, expressions::names, node, optional};
use crate::catalog::terminal::{OutputMap, validate_factor};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn target(value: &Node) -> Result<(Name, OutputMap, String)> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("expected decimal multiplication");
    };
    ensure!(
        expression.kind == i32::from(pg::AExprKind::AexprOp) && names(&expression.name)?.0 == ["*"],
        "terminal arithmetic supports only decimal literal multiplication"
    );
    let left = optional(expression.lexpr.as_deref())?;
    let right = optional(expression.rexpr.as_deref())?;
    let (input, literal) = if matches!(node(left)?, NodeEnum::ColumnRef(_)) {
        (column(left)?, right)
    } else {
        (column(right)?, left)
    };
    let NodeEnum::AConst(constant) = node(literal)? else {
        anyhow::bail!("terminal multiplication requires decimal literal");
    };
    ensure!(!constant.isnull, "NULL decimal literal unsupported");
    let Some(pg::a_const::Val::Fval(decimal)) = &constant.val else {
        anyhow::bail!("terminal multiplication requires decimal literal");
    };
    // Normalize omitted/extra leading zeroes, but preserve fractional scale.
    let decimal = decimal.fval.trim_start_matches('0');
    let factor = if decimal.starts_with('.') { format!("0{decimal}") } else { decimal.into() };
    validate_factor(&factor)?;
    Ok((input, OutputMap::NumericMultiply { factor }, "?column?".into()))
}
