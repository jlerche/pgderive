//! Closed typed tuple-local programs; SQL text is generated only after validation.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

mod context;
mod functions;
mod normalize;
mod outputs;
pub use outputs::OutputMap;
mod render;
pub use functions::{Calendar, Function, Operation, implementation_types};
use functions::{Signature, call_type};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Literal {
    Text(String),
    Integer(i32),
    Numeric(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expression {
    Input(usize),
    Average(usize),
    Null(u32),
    Literal(Literal),
    Call { function: Function, arguments: Vec<usize> },
    Coalesce(Vec<usize>),
    And(Vec<usize>),
    Or(Vec<usize>),
    Not(usize),
    CollateC(usize),
    ArrayElement { input: usize, index: u16 },
    IsNull { input: usize, negated: bool },
    Case { branches: Vec<(usize, usize)>, otherwise: usize },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub(crate) label: String,
    pub(crate) expression: usize,
}
/// Nodes reference only preceding nodes. Inputs and visible outputs are distinct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub(crate) expressions: Vec<Expression>,
    pub(crate) outputs: Vec<Output>,
    pub(crate) predicate: Option<usize>,
}
impl Program {
    pub(super) fn validate(&self, inputs: &[u32]) -> Result<Vec<u32>> {
        ensure!((1..=512).contains(&self.expressions.len()), "terminal expression budget exceeded");
        ensure!((1..=64).contains(&self.outputs.len()), "invalid terminal output layout");
        let mut types = Vec::new();
        for (index, expression) in self.expressions.iter().enumerate() {
            types.push(expression_type(expression, &types, inputs)?);
            context::validate(expression, &self.expressions[..index])?;
        }
        for output in &self.outputs {
            ensure!(
                !output.label.is_empty() && !output.label.contains('\0'),
                "invalid terminal output label"
            );
            types.get(output.expression).context("terminal output node absent")?;
        }
        if let Some(predicate) = self.predicate {
            ensure!(types.get(predicate) == Some(&16), "terminal predicate requires boolean");
        }
        Ok(types)
    }
    pub(super) fn signatures(&self, inputs: &[u32]) -> Result<Vec<Signature>> {
        let types = self.validate(inputs)?;
        self.render(inputs)?;
        let mut signatures = Vec::new();
        for expression in &self.expressions {
            if let Expression::Call { function, arguments } = expression {
                let arguments = argument_types(arguments, &types)?;
                signatures.push(function.signature(&arguments)?);
            } else if matches!(expression, Expression::Average(_)) {
                signatures.extend([
                    ("pg_catalog.numeric_in(cstring,oid,integer)", "numeric_in"),
                    ("pg_catalog.numeric_div(numeric,numeric)", "numeric_div"),
                    ("pg_catalog.split_part(text,text,integer)", "split_part"),
                ]);
            } else if matches!(expression, Expression::Literal(Literal::Numeric(_)))
                || matches!(expression, Expression::Input(index) if inputs.get(*index) == Some(&1700))
            {
                signatures.push(("pg_catalog.numeric_in(cstring,oid,integer)", "numeric_in"));
            }
        }
        signatures.sort_unstable();
        signatures.dedup();
        Ok(signatures)
    }
    pub fn has_collation(&self) -> bool {
        self.expressions.iter().any(|expression| matches!(expression, Expression::CollateC(_)))
    }
    pub(super) fn has_calendar(&self) -> bool {
        self.expressions.iter().any(|expression| {
            matches!(
                expression,
                Expression::Call {
                    function: Function::Utc | Function::FormatDate | Function::FormatMinute,
                    ..
                }
            )
        })
    }
}
fn argument_types(arguments: &[usize], types: &[u32]) -> Result<Vec<u32>> {
    arguments
        .iter()
        .map(|index| {
            types.get(*index).copied().context("terminal node must reference a prior node")
        })
        .collect()
}
pub fn expression_type(expression: &Expression, types: &[u32], inputs: &[u32]) -> Result<u32> {
    match expression {
        Expression::Input(index) => {
            let oid = *inputs.get(*index).context("terminal input absent")?;
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 2950 | 1114 | 1184 | 1700),
                "unsupported terminal input codec"
            );
            Ok(oid)
        }
        Expression::Average(index) => {
            ensure!(inputs.get(*index) == Some(&1700), "average finalization requires ratio input");
            Ok(1700)
        }
        Expression::Null(oid) => {
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 25 | 2950 | 1114 | 1184 | 1700),
                "unsupported typed NULL"
            );
            Ok(*oid)
        }
        Expression::Literal(literal) => literal_type(literal),
        Expression::Call { function, arguments } => {
            call_type(*function, &argument_types(arguments, types)?)
        }
        Expression::And(arguments) | Expression::Or(arguments) => {
            ensure!((2..=64).contains(&arguments.len()), "invalid terminal boolean arity");
            ensure!(
                argument_types(arguments, types)?.iter().all(|oid| *oid == 16),
                "terminal boolean operands required"
            );
            Ok(16)
        }
        Expression::Not(index) => {
            ensure!(types.get(*index) == Some(&16), "terminal NOT requires boolean prior node");
            Ok(16)
        }
        Expression::Case { branches, otherwise } => case_type(branches, *otherwise, types),
        Expression::Coalesce(arguments) => {
            ensure!((1..=64).contains(&arguments.len()), "invalid COALESCE arity");
            let arguments = argument_types(arguments, types)?;
            let result = *arguments.first().context("missing COALESCE operand")?;
            ensure!(
                arguments.iter().all(|oid| *oid == result),
                "COALESCE requires matching resolved types"
            );
            ensure!(result != 1009, "COALESCE requires scalar operands");
            Ok(result)
        }
        Expression::CollateC(index) => {
            ensure!(
                matches!(types.get(*index), Some(25 | 1043)),
                "C collation requires text prior node"
            );
            Ok(25)
        }
        Expression::ArrayElement { input, index } => {
            ensure!(
                types.get(*input) == Some(&1009) && (1..=1024).contains(index),
                "terminal capture requires text array and index 1 or 2"
            );
            Ok(25)
        }
        Expression::IsNull { input, .. } => {
            ensure!(
                types.get(*input).is_some_and(|oid| *oid != 1009),
                "terminal NULL test requires scalar prior node"
            );
            Ok(16)
        }
    }
}
fn literal_type(literal: &Literal) -> Result<u32> {
    match literal {
        Literal::Text(value) => {
            ensure!(
                value.len() <= 1024 && !value.contains('\0'),
                "terminal text literal exceeds budget"
            );
            Ok(25)
        }
        Literal::Integer(_) => Ok(23),
        Literal::Numeric(value) => {
            ensure!(
                value.len() <= 65 && !value.is_empty(),
                "terminal numeric literal exceeds budget"
            );
            let mut points = 0;
            let mut digits = 0;
            for byte in value.bytes() {
                if byte == b'.' {
                    points += 1;
                } else {
                    ensure!(byte.is_ascii_digit(), "invalid terminal numeric literal");
                    digits += 1;
                }
            }
            ensure!(points <= 1 && digits > 0, "invalid terminal numeric literal");
            Ok(1700)
        }
    }
}
fn case_type(branches: &[(usize, usize)], otherwise: usize, types: &[u32]) -> Result<u32> {
    ensure!((1..=32).contains(&branches.len()), "terminal CASE branch budget exceeded");
    let result = *types.get(otherwise).context("terminal CASE default node absent")?;
    for (condition, value) in branches {
        ensure!(types.get(*condition) == Some(&16), "terminal CASE requires boolean condition");
        ensure!(types.get(*value) == Some(&result), "terminal CASE requires matching result types");
    }
    Ok(result)
}

#[cfg(test)]
#[path = "program/tests.rs"]
mod tests;

impl Expression {
    pub(crate) fn dependencies(&self) -> Vec<usize> {
        match self {
            Self::Input(_) | Self::Average(_) | Self::Null(_) | Self::Literal(_) => Vec::new(),
            Self::Call { arguments, .. }
            | Self::Coalesce(arguments)
            | Self::And(arguments)
            | Self::Or(arguments) => arguments.clone(),
            Self::Not(input)
            | Self::CollateC(input)
            | Self::ArrayElement { input, .. }
            | Self::IsNull { input, .. } => vec![*input],
            Self::Case { branches, otherwise } => branches
                .iter()
                .flat_map(|(condition, value)| [*condition, *value])
                .chain([*otherwise])
                .collect(),
        }
    }
}
