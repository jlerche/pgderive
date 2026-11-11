//! `PostgreSQL` USING values: cast inputs once, then choose a Var or coalesce.
use super::{ColumnRef, Typed, Value};
use crate::compiler::left_join::Kind;
use anyhow::Result;
impl Typed {
    pub(in crate::compiler) fn using_value(
        columns: (ColumnRef, ColumnRef),
        kind: Kind,
        oid: u32,
        modifiers: (i32, i32),
    ) -> Result<Self> {
        let common = if columns.0.oid == columns.1.oid && modifiers.0 == modifiers.1 {
            modifiers.0
        } else {
            -1
        };
        let left_var = columns.0.oid == oid && modifiers.0 == common;
        let right_var = columns.1.oid == oid && modifiers.1 == common;
        let convert = |column: ColumnRef, modifier| -> Result<Self> {
            if column.oid == oid && modifier == common {
                return Ok(Self::column(column));
            }
            let input = Self::set_cast(column, oid, Some(modifier))?;
            Ok(if modifier == common {
                input
            } else {
                Self { oid, value: Value::Cast { input: Box::new(input), typmod: None } }
            })
        };
        let left = convert(columns.0, modifiers.0)?;
        let right = convert(columns.1, modifiers.1)?;
        Ok(match kind {
            Kind::Full => Self { oid, value: Value::Coalesce(vec![left, right]) },
            Kind::Right => right,
            Kind::Inner if !left_var && right_var => right,
            Kind::Inner | Kind::Left => left,
        })
    }
}
