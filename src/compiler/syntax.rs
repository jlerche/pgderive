use super::parser::{
    Name,
    expressions::{column, names, node, optional},
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
#[derive(Clone)]
pub(super) enum Scalar {
    Column(Name),
    Remainder(Name, i64),
    Difference(Name, Name),
    Interval(i64),
    Integer(i64),
    Boolean(bool),
    String(String),
    Null,
}
pub(super) type Expr = Box<Node>;
pub(super) fn predicate(value: Option<&Node>) -> Result<Option<Expr>> {
    value
        .map(|value| {
            node(value)?;
            Ok(Box::new(value.clone()))
        })
        .transpose()
}
pub(super) fn scalar(value: &Node) -> Result<Scalar> {
    match node(value)? {
        NodeEnum::ColumnRef(_) => Ok(Scalar::Column(column(value)?)),
        NodeEnum::AConst(literal) => constant(literal),
        NodeEnum::AExpr(expression) if names(&expression.name)?.0 == ["-"] => {
            ensure!(
                expression.kind == i32::from(pg::AExprKind::AexprOp),
                "unsupported timestamp difference"
            );
            let left = column(optional(expression.lexpr.as_deref())?);
            let right = column(optional(expression.rexpr.as_deref())?);
            match (left, right) {
                (Ok(left), Ok(right)) => Ok(Scalar::Difference(left, right)),
                _ => anyhow::bail!("not a native column difference"),
            }
        }
        NodeEnum::TypeCast(_) => super::parser::scalar::interval(value, true).map(Scalar::Interval),
        NodeEnum::FuncCall(_) | NodeEnum::AExpr(_) => remainder(value),
        _ => anyhow::bail!("unsupported scalar expression"),
    }
}
fn constant(value: &pg::AConst) -> Result<Scalar> {
    use pg::a_const::Val;
    if value.isnull {
        return Ok(Scalar::Null);
    }
    match value.val.as_ref() {
        Some(Val::Ival(value)) => Ok(Scalar::Integer(i64::from(value.ival))),
        Some(Val::Fval(value)) => {
            ensure!(
                value.fval.bytes().all(|byte| byte.is_ascii_digit() || byte == b'-'),
                "only integral literals supported"
            );
            Ok(Scalar::Integer(value.fval.parse()?))
        }
        Some(Val::Boolval(value)) => Ok(Scalar::Boolean(value.boolval)),
        Some(Val::Sval(value)) => Ok(Scalar::String(value.sval.clone())),
        _ => anyhow::bail!("unsupported literal"),
    }
}

fn remainder(value: &Node) -> Result<Scalar> {
    let (left, right) = match node(value)? {
        NodeEnum::AExpr(expr) => {
            ensure!(
                expr.kind == i32::from(pg::AExprKind::AexprOp) && names(&expr.name)?.0 == ["%"],
                "unsupported scalar operator"
            );
            (optional(expr.lexpr.as_deref())?, optional(expr.rexpr.as_deref())?)
        }
        NodeEnum::FuncCall(call) => {
            let function = names(&call.funcname)?.0;
            ensure!(
                function == ["mod"] || function == ["pg_catalog", "mod"],
                "unsupported scalar function"
            );
            ensure!(
                call.args.len() == 2
                    && call.agg_order.is_empty()
                    && call.agg_filter.is_none()
                    && call.over.is_none()
                    && !call.agg_within_group
                    && !call.agg_star
                    && !call.agg_distinct
                    && !call.func_variadic
                    && call.funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
                "unsupported MOD modifier"
            );
            (&call.args[0], &call.args[1])
        }
        _ => anyhow::bail!("unsupported remainder expression"),
    };
    let NodeEnum::AConst(value) = node(right)? else {
        anyhow::bail!("MOD divisor must be a nonzero integral literal");
    };
    let Scalar::Integer(divisor) = constant(value)? else {
        anyhow::bail!("MOD divisor must be a nonzero integral literal");
    };
    ensure!(divisor != 0, "MOD zero divisor is outside the supported subset");
    Ok(Scalar::Remainder(column(left)?, divisor))
}
