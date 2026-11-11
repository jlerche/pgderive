//! Column reads, substitution and strictness over bound scalars.
use super::{ColumnRef, Typed, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Replacement for a column read; `None` keeps the column.
pub(in crate::compiler) type Replace<'a> = dyn Fn(&ColumnRef) -> Option<Typed> + 'a;

impl Typed {
    pub(in crate::compiler) fn reads(&self, out: &mut BTreeMap<String, ColumnRef>) {
        let mut add = |column: &ColumnRef| {
            out.entry(column.name.clone()).or_insert_with(|| column.clone());
        };
        match &self.value {
            Value::Column(column) | Value::Average(column) => add(column),
            Value::Temporal(value) => {
                add(&value.input);
                if let Some((column, _)) = &value.shift {
                    add(column);
                }
            }
            Value::Boolean(value) => value.reads(out),
            Value::SimpleCase(case) => {
                case.test.reads(out);
                case.otherwise.reads(out);
                for (candidate, result) in &case.branches {
                    candidate.reads(out);
                    result.reads(out);
                }
            }
            Value::Call(call) => call.arguments.iter().for_each(|value| value.reads(out)),
            Value::Cast { input, .. } => input.reads(out),
            Value::Binary { left, right, .. }
            | Value::NullIf { left, right }
            | Value::Concat { left, right } => {
                left.reads(out);
                right.reads(out);
            }
            Value::Coalesce(values) | Value::MinMax { arguments: values, .. } => {
                for value in values {
                    value.reads(out);
                }
            }
            Value::Case { branches, otherwise } => {
                for (condition, value) in branches {
                    condition.reads(out);
                    value.reads(out);
                }
                otherwise.reads(out);
            }
            Value::Literal(_)
            | Value::Numeric(_)
            | Value::Text(_)
            | Value::Encoded(_)
            | Value::Interval(_) => {}
        }
    }
    /// A cast a set operation added to an arm's column, not a literal it typed.
    /// The value reaches its type through a cast from another type.
    pub(in crate::compiler) fn casts_type(&self) -> bool {
        matches!(&self.value, Value::Cast { input, .. } if input.oid != self.oid)
    }
    /// Rewrite column reads; `None` when a kernel needs a column and receives a computation.
    pub(in crate::compiler) fn replaced(&self, replace: &Replace<'_>) -> Option<Self> {
        let value = match &self.value {
            Value::Column(column) => match replace(column) {
                Some(value) => return Some(value),
                None => Value::Column(column.clone()),
            },
            Value::Average(column) => Value::Average(renamed(column, replace)?),
            Value::Temporal(value) => {
                let mut value = value.clone();
                value.input = renamed(&value.input, replace)?;
                if let Some((column, _)) = &mut value.shift {
                    *column = renamed(column, replace)?;
                }
                Value::Temporal(value)
            }
            Value::Boolean(value) => Value::Boolean(Box::new(value.replaced(replace)?)),
            Value::SimpleCase(case) => Value::SimpleCase(case.replaced(replace)?),
            Value::Call(call) => {
                let mut call = call.clone();
                call.arguments = all(&call.arguments, replace)?;
                Value::Call(call)
            }
            Value::Cast { input, typmod } => {
                Value::Cast { input: Box::new(input.replaced(replace)?), typmod: *typmod }
            }
            Value::Binary { operation, left, right } => Value::Binary {
                operation: *operation,
                left: Box::new(left.replaced(replace)?),
                right: Box::new(right.replaced(replace)?),
            },
            Value::Concat { left, right } => Value::Concat {
                left: Box::new(left.replaced(replace)?),
                right: Box::new(right.replaced(replace)?),
            },
            Value::NullIf { left, right } => Value::NullIf {
                left: Box::new(left.replaced(replace)?),
                right: Box::new(right.replaced(replace)?),
            },
            Value::Coalesce(values) => Value::Coalesce(all(values, replace)?),
            Value::MinMax { operation, arguments } => {
                Value::MinMax { operation: *operation, arguments: all(arguments, replace)? }
            }
            Value::Case { branches, otherwise } => Value::Case {
                branches: branches
                    .iter()
                    .map(|(condition, value)| {
                        Some((condition.replaced(replace)?, value.replaced(replace)?))
                    })
                    .collect::<Option<_>>()?,
                otherwise: Box::new(otherwise.replaced(replace)?),
            },
            value => value.clone(),
        };
        Some(Self { oid: self.oid, value })
    }
    /// Columns whose NULL forces this scalar to NULL, as `find_nonnullable_rels` proves it.
    pub(in crate::compiler) fn strict_columns(&self, out: &mut BTreeSet<String>) {
        match &self.value {
            Value::Column(column) => {
                out.insert(column.name.clone());
            }
            Value::Temporal(value) => {
                out.insert(value.input.name.clone());
            }
            Value::Boolean(value) => out.extend(value.strict_columns(false)),
            Value::Call(call) if call.operation.is_strict() => {
                for value in &call.arguments {
                    value.strict_columns(out);
                }
            }
            Value::Cast { input, .. } => input.strict_columns(out),
            Value::Binary { left, right, .. } | Value::Concat { left, right } => {
                left.strict_columns(out);
                right.strict_columns(out);
            }
            _ => {}
        }
    }
    /// No lazy construct, as `contain_nonstrict_functions` decides for pulled-up outputs.
    pub(in crate::compiler) fn is_strict(&self) -> bool {
        match &self.value {
            Value::Boolean(value) => value.is_strict(),
            Value::Call(call) => {
                call.operation.is_strict() && call.arguments.iter().all(Self::is_strict)
            }
            Value::Cast { input, .. } => input.is_strict(),
            Value::Binary { left, right, .. } | Value::Concat { left, right } => {
                left.is_strict() && right.is_strict()
            }
            Value::Coalesce(_)
            | Value::Case { .. }
            | Value::SimpleCase(_)
            | Value::NullIf { .. }
            | Value::MinMax { .. } => false,
            _ => true,
        }
    }
}
fn renamed(column: &ColumnRef, replace: &Replace<'_>) -> Option<ColumnRef> {
    replace(column).map_or_else(|| Some(column.clone()), |value| value.as_column().cloned())
}
fn all(values: &[Typed], replace: &Replace<'_>) -> Option<Vec<Typed>> {
    values.iter().map(|value| value.replaced(replace)).collect()
}
