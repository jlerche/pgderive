//! Contextual predicate operands reuse the shared typed scalar representation.
use super::{ColumnRef, Name, Typed};
use crate::compiler::parser::expressions::{column, node};
use anyhow::{Result, ensure};
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
    bind_at(value, resolve, 0)
}
pub(super) fn bind_at(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Operand> {
    let (value, contextual, null_test) = if let Ok(name) = column(value) {
        (Typed::column(resolve(&name)?), false, true)
    } else if let Ok(literal) = native_literal(value) {
        (literal, true, false)
    } else {
        (super::bind(value, resolve, depth)?, false, true)
    };
    Ok(Operand { value, contextual, null_test })
}
impl Operand {
    pub(in crate::compiler) fn duplicate_for_between(&self) -> Result<Self> {
        ensure!(
            crate::compiler::plan::bounded(&self.value),
            "BETWEEN probe exceeds expression expansion budget"
        );
        Ok(Self {
            value: self.value.clone(),
            contextual: self.contextual,
            null_test: self.null_test,
        })
    }
    pub(in crate::compiler) fn into_value(self) -> Typed {
        self.value
    }
    pub(in crate::compiler) const fn literal_oid(&self) -> u32 {
        if self.value.oid == 705 { 0 } else { self.value.oid }
    }
    pub(in crate::compiler) const fn oid(&self) -> Option<u32> {
        if self.contextual { None } else { Some(self.value.oid) }
    }
    pub(in crate::compiler) fn coerce(self, oid: u32) -> Result<Typed> {
        if self.contextual {
            return self.value.predicate_literal(oid);
        }
        ensure!(
            self.value.oid == oid
                || (matches!(self.value.oid, 25 | 1043) && matches!(oid, 25 | 1043))
                || (matches!(oid, 1082 | 1114 | 1184)
                    && matches!(self.value.oid, 1082 | 1114 | 1184))
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
