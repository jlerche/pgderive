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
#[derive(Clone)]
pub(super) enum Expr {
    Value(Scalar),
    Null(Scalar, bool),
    Compare(Compare, Scalar, Scalar),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}
pub(super) fn predicate(value: Option<&Node>) -> Result<Option<Expr>> {
    value.map(|value| expression(value, &mut 0, 0)).transpose()
}
fn expression(value: &Node, visited: &mut usize, depth: usize) -> Result<Expr> {
    *visited += 1;
    ensure!(*visited <= 512 && depth <= 64, "WHERE exceeds AST/depth budget");
    match node(value)? {
        NodeEnum::BoolExpr(expr) => boolean(expr, visited, depth),
        NodeEnum::NullTest(test) => {
            ensure!(!test.argisrow && test.xpr.is_none(), "row NULL tests unsupported");
            let not = match pg::NullTestType::try_from(test.nulltesttype)? {
                pg::NullTestType::IsNull => false,
                pg::NullTestType::IsNotNull => true,
                pg::NullTestType::Undefined => anyhow::bail!("invalid NULL test"),
            };
            Ok(Expr::Null(scalar(optional(test.arg.as_deref())?)?, not))
        }
        NodeEnum::AExpr(expr) => comparison(expr),
        _ => Ok(Expr::Value(scalar(value)?)),
    }
}
fn boolean(expr: &pg::BoolExpr, visited: &mut usize, depth: usize) -> Result<Expr> {
    ensure!(expr.xpr.is_none(), "invalid Boolean expression");
    let mut args = expr
        .args
        .iter()
        .map(|value| expression(value, visited, depth + 1))
        .collect::<Result<Vec<_>>>()?;
    match pg::BoolExprType::try_from(expr.boolop)? {
        pg::BoolExprType::AndExpr | pg::BoolExprType::OrExpr => {
            ensure!(args.len() >= 2, "invalid Boolean arity");
            Ok(if expr.boolop == i32::from(pg::BoolExprType::AndExpr) {
                Expr::And(args)
            } else {
                Expr::Or(args)
            })
        }
        pg::BoolExprType::NotExpr => {
            ensure!(args.len() == 1, "invalid NOT arity");
            Ok(Expr::Not(Box::new(args.remove(0))))
        }
        pg::BoolExprType::Undefined => anyhow::bail!("invalid Boolean operator"),
    }
}
fn comparison(expr: &pg::AExpr) -> Result<Expr> {
    ensure!(expr.kind == i32::from(pg::AExprKind::AexprOp), "unsupported comparison kind");
    let name = names(&expr.name)?;
    let [name] = name.0.as_slice() else {
        anyhow::bail!("qualified operators unsupported");
    };
    let op = match name.as_str() {
        "=" => Compare::Eq,
        "<>" => Compare::Ne,
        "<" => Compare::Lt,
        "<=" => Compare::Le,
        ">" => Compare::Gt,
        ">=" => Compare::Ge,
        _ => anyhow::bail!("unsupported comparison operator"),
    };
    Ok(Expr::Compare(
        op,
        scalar(optional(expr.lexpr.as_deref())?)?,
        scalar(optional(expr.rexpr.as_deref())?)?,
    ))
}
pub(super) fn scalar(value: &Node) -> Result<Scalar> {
    match node(value)? {
        NodeEnum::ColumnRef(_) => Ok(Scalar::Column(column(value)?)),
        NodeEnum::AConst(value) => constant(value),
        NodeEnum::AExpr(expression) if names(&expression.name)?.0 == ["-"] => {
            ensure!(
                expression.kind == i32::from(pg::AExprKind::AexprOp),
                "unsupported timestamp difference"
            );
            Ok(Scalar::Difference(
                column(optional(expression.lexpr.as_deref())?)?,
                column(optional(expression.rexpr.as_deref())?)?,
            ))
        }
        NodeEnum::TypeCast(_) => Ok(Scalar::Interval(super::parser::scalar::interval(value)?)),
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
