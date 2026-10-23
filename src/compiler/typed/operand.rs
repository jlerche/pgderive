//! Contextual predicate operands reuse the shared typed scalar representation.
use super::{ColumnRef, Name, Typed, Value};
use crate::compiler::parser::{
    expressions::{column, names, node, optional},
    scalar,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) struct Operand {
    value: Typed,
    contextual: bool,
    null_test: bool,
}
pub(in crate::compiler) fn bind(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Operand> {
    let (value, contextual, null_test) = if let Ok(name) = column(value) {
        (Typed::column(resolve(&name)?), false, true)
    } else if let Ok(literal) = native_literal(value) {
        (literal, true, false)
    } else if matches!(node(value)?, NodeEnum::TypeCast(_))
        && let Ok(interval) = scalar::interval(value, true)
    {
        (
            Typed { oid: 1186, value: Value::Interval(crate::temporal::Gap::Finite(interval)) },
            true,
            false,
        )
    } else if let Ok((left, right)) = difference(value) {
        (Typed::difference(resolve(&left)?, resolve(&right)?)?, false, false)
    } else if let Ok((name, divisor)) = remainder(value) {
        (Typed::remainder(resolve(&name)?, divisor)?, false, false)
    } else {
        (super::bind(value, resolve, 0)?, false, true)
    };
    Ok(Operand { value, contextual, null_test })
}
impl Operand {
    pub(in crate::compiler) const fn oid(&self) -> Option<u32> {
        if self.contextual { None } else { Some(self.value.oid) }
    }
    pub(in crate::compiler) fn coerce(self, oid: u32) -> Result<Typed> {
        if self.contextual {
            return self.value.predicate_literal(oid);
        }
        ensure!(
            self.value.oid == oid
                || (matches!(oid, 20 | 21 | 23 | 1700)
                    && matches!(self.value.oid, 20 | 21 | 23 | 1700)),
            "SQL type: incompatible comparison operands"
        );
        Ok(self.value)
    }
    pub(in crate::compiler) fn null_test(self) -> Result<Typed> {
        ensure!(self.null_test, "SQL type: NULL tests require a column or typed expression");
        Ok(self.value)
    }
}
pub(in crate::compiler) fn native_literal(value: &Node) -> Result<Typed> {
    let NodeEnum::AConst(value) = node(value)? else { anyhow::bail!("native literal required") };
    if !value.isnull
        && let Some(pg::a_const::Val::Fval(value)) = &value.val
    {
        ensure!(
            value.fval.bytes().all(|byte| byte.is_ascii_digit() || byte == b'-'),
            "only integral literals supported"
        );
        value.fval.parse::<i64>()?;
    }
    super::literal(value)
}
pub(in crate::compiler) fn navigation(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Typed> {
    if let Ok(name) = column(value) {
        return Ok(Typed::column(resolve(&name)?));
    }
    native_literal(value)
}
fn difference(value: &Node) -> Result<(Name, Name)> {
    let NodeEnum::AExpr(value) = node(value)? else { anyhow::bail!("native difference required") };
    ensure!(
        value.kind == i32::from(pg::AExprKind::AexprOp) && names(&value.name)?.0 == ["-"],
        "native difference required"
    );
    Ok((column(optional(value.lexpr.as_deref())?)?, column(optional(value.rexpr.as_deref())?)?))
}
fn remainder(value: &Node) -> Result<(Name, i64)> {
    let (left, right) = match node(value)? {
        NodeEnum::AExpr(value) => {
            ensure!(
                value.kind == i32::from(pg::AExprKind::AexprOp) && names(&value.name)?.0 == ["%"],
                "native MOD required"
            );
            (optional(value.lexpr.as_deref())?, optional(value.rexpr.as_deref())?)
        }
        NodeEnum::FuncCall(call) => {
            ensure!(
                matches!(
                    super::super::parser::expressions::builtin_name(&names(&call.funcname)?.0),
                    Some("mod")
                ),
                "native MOD required"
            );
            scalar::validate_call(call)?;
            let [left, right] = call.args.as_slice() else {
                anyhow::bail!("MOD requires two arguments")
            };
            (left, right)
        }
        _ => anyhow::bail!("native MOD required"),
    };
    let divisor =
        native_literal(right)?.as_integer_literal().context("integral MOD divisor required")?;
    ensure!(divisor != 0, "nonzero MOD divisor required");
    Ok((column(left)?, divisor))
}
