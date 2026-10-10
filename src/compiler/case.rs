//! Lazy integral CASE evaluation at a relational scalar-map boundary.
use super::{ColumnRef, expression, parser::Name, syntax};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Case {
    arms: Vec<(expression::Expr, Arm)>,
    otherwise: Arm,
}
#[derive(Clone, Serialize)]
pub enum Arm {
    Column(ColumnRef),
    Literal(Option<i64>),
}
impl Case {
    pub(super) fn has_gap(&self) -> bool {
        self.arms.iter().any(|(condition, _)| condition.has_gap())
    }
    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        for (condition, value) in &self.arms {
            if condition.evaluate((row, row))? == Some(true) {
                return value.evaluate(row);
            }
        }
        self.otherwise.evaluate(row)
    }
}
impl Arm {
    fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        match self {
            Self::Literal(value) => Ok(value.map(|value| value.to_string())),
            Self::Column(column) => row.get(&column.name).context("missing CASE value").cloned(),
        }
    }
}
pub(super) fn bind(
    parsed: &super::parser::case::Case,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<(Case, u32, bool)> {
    let mut oid = None;
    let mut nullable = false;
    let otherwise = arm(&parsed.otherwise, resolve, &mut oid, &mut nullable)?;
    let arms = parsed
        .arms
        .iter()
        .map(|(condition, value)| {
            Ok((
                expression::bind(condition.clone(), resolve)?,
                arm(value, resolve, &mut oid, &mut nullable)?,
            ))
        })
        .collect::<Result<_>>()?;
    Ok((Case { arms, otherwise }, oid.context("CASE requires an integral result arm")?, nullable))
}
fn arm(
    value: &syntax::Scalar,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    oid: &mut Option<u32>,
    nullable: &mut bool,
) -> Result<Arm> {
    let (arm, candidate, null) = match value {
        syntax::Scalar::Column(name) => {
            let column = resolve(name)?;
            ensure!(
                matches!(column.oid, 20 | 21 | 23),
                "CASE requires native integral result arms"
            );
            let (oid, nullable) = (column.oid, column.nullable);
            (Arm::Column(column), Some(oid), nullable)
        }
        syntax::Scalar::Integer(value) => (
            Arm::Literal(Some(*value)),
            Some(if i32::try_from(*value).is_ok() { 23 } else { 20 }),
            false,
        ),
        syntax::Scalar::Null => (Arm::Literal(None), None, true),
        _ => anyhow::bail!("CASE requires integral column/literal or NULL arms"),
    };
    if let Some(candidate) = candidate {
        *oid = Some(match (*oid, candidate) {
            (Some(20), _) | (_, 20) => 20,
            (Some(23), _) | (_, 23) => 23,
            _ => 21,
        });
    }
    *nullable |= null;
    Ok(arm)
}
