//! Constant folding, run once rewrites settle: `PostgreSQL` folds while planning what it keeps.
use super::{Typed, Value};
use crate::transaction::Row;
use anyhow::{Context, Result};

impl Typed {
    pub(in crate::compiler) fn fold(&self) -> Result<Self> {
        let value = match &self.value {
            Value::Boolean(value) => Value::Boolean(Box::new(value.fold()?)),
            Value::Call(call) => {
                let mut call = call.clone();
                call.arguments = call.arguments.iter().map(Self::fold).collect::<Result<_>>()?;
                if call.operation.is_strict() && call.arguments.iter().any(Self::constant_null) {
                    Value::Literal(None)
                } else {
                    Value::Call(call)
                }
            }
            Value::Cast { input, typmod } => {
                let input = input.fold()?;
                if typmod.is_none() && input.constant_null() {
                    Value::Literal(None)
                } else {
                    Value::Cast { input: Box::new(input), typmod: *typmod }
                }
            }
            Value::Binary { operation, left, right } => {
                strict_binary(left, right, |left, right| Value::Binary {
                    operation: *operation,
                    left,
                    right,
                })?
            }
            Value::Concat { left, right } => {
                strict_binary(left, right, |left, right| Value::Concat { left, right })?
            }
            Value::NullIf { left, right } => {
                let (left, right) = (left.fold()?, right.fold()?);
                if left.constant_null() || right.constant_null() {
                    left.value
                } else {
                    Value::NullIf { left: Box::new(left), right: Box::new(right) }
                }
            }
            Value::Coalesce(values) => Value::Coalesce(coalesced(values)?),
            Value::MinMax { operation, arguments } => Value::MinMax {
                operation: *operation,
                arguments: arguments.iter().map(Self::fold).collect::<Result<_>>()?,
            },
            Value::Case { branches, otherwise } => cased(branches, otherwise)?,
            Value::SimpleCase(case) => Value::SimpleCase(case.fold()?),
            value => value.clone(),
        };
        let folded = Self { oid: self.oid, value };
        if folded.is_constant() {
            folded.evaluate(&Row::new()).context("constant expression failed in-engine")?;
        }
        Ok(folded)
    }
    /// An aggregate FILTER is written apart from its argument, so its constant false does not
    /// spare the argument the way a CASE condition spares its branch.
    pub(in crate::compiler) fn fold_filtered(&self) -> Result<()> {
        if let Value::Case { branches, .. } = &self.value {
            for (_, result) in branches {
                result.fold()?;
            }
        }
        Ok(())
    }
    /// A constant that evaluates to NULL; one that fails is left for `fold` to report.
    pub(in crate::compiler) fn constant_null(&self) -> bool {
        self.is_constant() && matches!(self.evaluate(&Row::new()), Ok(None))
    }
    pub(in crate::compiler) const fn null_boolean() -> Self {
        Self { oid: 16, value: Value::Literal(None) }
    }
}
fn strict_binary(
    left: &Typed,
    right: &Typed,
    build: impl FnOnce(Box<Typed>, Box<Typed>) -> Value,
) -> Result<Value> {
    let (left, right) = (left.fold()?, right.fold()?);
    Ok(if left.constant_null() || right.constant_null() {
        Value::Literal(None)
    } else {
        build(Box::new(left), Box::new(right))
    })
}

/// Folding stops at the first constant non-NULL argument and drops constant NULLs.
fn coalesced(values: &[Typed]) -> Result<Vec<Typed>> {
    let mut out = Vec::new();
    for value in values {
        let value = value.fold()?;
        if value.constant_null() {
            continue;
        }
        let decided = value.is_constant();
        out.push(value);
        if decided {
            break;
        }
    }
    Ok(out)
}
/// Constant conditions decide their branch before folding reaches its result.
fn cased(
    branches: &[(crate::compiler::expression::Expr, Typed)],
    otherwise: &Typed,
) -> Result<Value> {
    let mut kept = Vec::new();
    for (condition, result) in branches {
        let condition = condition.fold()?;
        match condition.constant_truth()? {
            Some(true) => {
                return Ok(Value::Case { branches: kept, otherwise: Box::new(result.fold()?) });
            }
            None if !condition.is_constant() => kept.push((condition, result.fold()?)),
            _ => {}
        }
    }
    Ok(Value::Case { branches: kept, otherwise: Box::new(otherwise.fold()?) })
}
