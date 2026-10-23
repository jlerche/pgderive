//! Contextual literal binding and native values for the shared scalar IR.
use super::{ColumnRef, Typed, Value};
use crate::{compiler::value, transaction::Row};
use anyhow::{Context, Result, ensure};
impl Typed {
    pub(in crate::compiler) const fn column(column: ColumnRef) -> Self {
        Self { oid: column.oid, value: Value::Column(column) }
    }
    pub(super) fn predicate_literal(self, oid: u32) -> Result<Self> {
        let text = match self.value {
            Value::Literal(None) => None,
            Value::Literal(Some(value)) if matches!(oid, 20 | 21 | 23) => {
                return Ok(Self { oid: self.oid, value: Value::Literal(Some(value)) });
            }
            Value::Literal(Some(value)) if oid == 1700 => Some(value.to_string()),
            Value::Encoded(value) if self.oid == 16 && oid == 16 => value,
            Value::Text(value) if matches!(oid, 25 | 1043 | 1700 | 2950 | 1114 | 1184) => {
                Some(value)
            }
            Value::Interval(value) if oid == 1186 => {
                return Ok(Self { oid, value: Value::Interval(value) });
            }
            _ => anyhow::bail!("SQL type: literal does not match native column"),
        };
        let text = text.map(|text| value::literal_text(oid, &text)).transpose()?;
        Ok(Self { oid, value: Value::Encoded(text) })
    }
    pub(in crate::compiler) fn navigation_default(
        self,
        argument: u32,
    ) -> Result<(Option<String>, u32)> {
        Ok(match self.value {
            Value::Literal(None) => (None, argument),
            Value::Literal(Some(value)) => (Some(value.to_string()), self.oid),
            Value::Encoded(value) if self.oid == 16 => (value, 16),
            Value::Text(value) => {
                ensure!(
                    matches!(argument, 25 | 1043),
                    "string LAG/LEAD defaults require text/varchar; other input casts need typed expression lowering"
                );
                (Some(value), argument)
            }
            _ => anyhow::bail!("unsupported LAG/LEAD default"),
        })
    }
    pub(in crate::compiler) fn difference(left: ColumnRef, right: ColumnRef) -> Result<Self> {
        if matches!(left.oid, 20 | 21 | 23 | 1700) && matches!(right.oid, 20 | 21 | 23 | 1700) {
            return Ok(super::difference(left, right));
        }
        ensure!(
            matches!(left.oid, 1114 | 1184) && left.oid == right.oid,
            "timestamp subtraction requires same-type native timestamp operands"
        );
        Ok(Self { oid: 1186, value: Value::Difference(left, right) })
    }
    pub(in crate::compiler) fn remainder(input: ColumnRef, divisor: i64) -> Result<Self> {
        ensure!(
            matches!(input.oid, 20 | 21 | 23) && divisor != 0,
            "SQL type: MOD requires integral input and nonzero literal divisor"
        );
        let oid = if input.oid == 20 || i32::try_from(divisor).is_err() { 20 } else { 23 };
        Ok(Self {
            oid,
            value: Value::Remainder(super::super::remainder::Remainder { input, divisor }),
        })
    }
    pub(in crate::compiler) fn is_null(&self, rows: (&Row, &Row)) -> Result<bool> {
        if let Value::Column(column) = &self.value {
            return Ok((if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("compiled predicate column absent")?
                .is_none());
        }
        Ok(self.native_pair(rows)?.is_none())
    }
    pub(in crate::compiler) const fn as_remainder(
        &self,
    ) -> Option<&crate::compiler::remainder::Remainder> {
        if let Value::Remainder(value) = &self.value { Some(value) } else { None }
    }
    pub(in crate::compiler) const fn as_column(&self) -> Option<&ColumnRef> {
        if let Value::Column(column) = &self.value { Some(column) } else { None }
    }
    pub(in crate::compiler) const fn as_integer_literal(&self) -> Option<i64> {
        if let Value::Literal(value) = &self.value { *value } else { None }
    }
}
impl Typed {
    pub(in crate::compiler) const fn temporal(value: super::super::scalar::Temporal) -> Self {
        Self { oid: value.input.oid, value: Value::Temporal(value) }
    }
    pub(in crate::compiler) const fn shift(&self) -> Option<&(ColumnRef, i64)> {
        if let Value::Temporal(value) = &self.value { value.shift.as_ref() } else { None }
    }
}
impl Typed {
    pub(in crate::compiler) fn filtered(
        self,
        condition: crate::compiler::expression::Expr,
    ) -> Self {
        let otherwise = Self { oid: self.oid, value: Value::Literal(None) };
        Self {
            oid: self.oid,
            value: Value::Case {
                branches: vec![(condition, self)],
                otherwise: Box::new(otherwise),
            },
        }
    }
    pub(in crate::compiler) const fn null_text() -> Self {
        Self { oid: 25, value: Value::Literal(None) }
    }
    pub(in crate::compiler) const fn unknown_text(mut self) -> Self {
        if self.oid == 705 {
            self.oid = 25;
        }
        self
    }
}
