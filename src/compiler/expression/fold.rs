//! Constant folding over predicates, in the order `eval_const_expressions` reaches operands.
use super::{Expr, Predicate, Scalar};
use anyhow::{Context, Result};

impl Expr {
    pub(in crate::compiler) fn fold(&self) -> Result<Self> {
        let folded = match self {
            Self::Value(value) => Self::Value(value.fold()?),
            Self::Null(value, not) => Self::Null(value.fold()?, *not),
            Self::Compare(op, left, right) => {
                let (left, right) = (left.fold()?, right.fold()?);
                if left.constant_null() || right.constant_null() {
                    Self::Value(Scalar::null_boolean())
                } else {
                    Self::Compare(op.clone(), left, right)
                }
            }
            Self::Distinct { left, right, negated } => {
                Self::Distinct { left: left.fold()?, right: right.fold()?, negated: *negated }
            }
            Self::And(values) => connective(values, false, Self::And)?,
            Self::Or(values) => connective(values, true, Self::Or)?,
            Self::Quals(values) => quals(values)?,
            Self::Array { and, values } => Self::Array {
                and: *and,
                values: values.iter().map(Self::fold).collect::<Result<_>>()?,
            },
            Self::Not(value) => Self::Not(Box::new(value.fold()?)),
            Self::BooleanTest { input, expected, negated } => Self::BooleanTest {
                input: Box::new(input.fold()?),
                expected: *expected,
                negated: *negated,
            },
        };
        if folded.is_constant() {
            folded.constant_truth().context("constant expression failed in-engine")?;
        }
        Ok(folded)
    }
}
fn connective(values: &[Expr], deciding: bool, build: fn(Vec<Expr>) -> Expr) -> Result<Expr> {
    let mut kept = Vec::new();
    for value in values {
        let value = value.fold()?;
        match value.constant_truth()? {
            Some(truth) if truth == deciding => return Ok(value),
            Some(_) => {}
            None => kept.push(value),
        }
    }
    Ok(match kept.len() {
        0 => build(Vec::new()),
        1 => kept.remove(0),
        _ => build(kept),
    })
}
/// Merged clauses were folded separately, so a failure in one survives a false in another.
fn quals(values: &[Expr]) -> Result<Expr> {
    let folded = values.iter().map(Expr::fold).collect::<Result<Vec<_>>>()?;
    for value in &folded {
        if value.is_constant() && value.constant_truth()? != Some(true) {
            return Ok(value.clone());
        }
    }
    let mut kept = folded.into_iter().filter(|value| !value.is_constant()).collect::<Vec<_>>();
    Ok(if kept.len() == 1 { kept.remove(0) } else { Expr::Quals(kept) })
}
impl Predicate {
    pub(in crate::compiler) fn fold(&self) -> Result<Self> {
        Ok(Self(self.0.fold()?))
    }
}
