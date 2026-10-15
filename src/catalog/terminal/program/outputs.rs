//! Lower simple output maps into the same typed expression program.
use super::{Expression, Function, Literal, Output, Program};
use anyhow::{Result, ensure};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputMap {
    Identity,
    Abs,
    Length,
    Numeric,
    Average,
    NumericMultiply { factor: String },
    ArrayElement { delimiter: String, index: u16 },
    TimestampFormat { pattern: String, utc: bool },
}
impl Program {
    pub(in crate::catalog::terminal) fn outputs(
        maps: Vec<OutputMap>,
        types: &[u32],
    ) -> Result<Self> {
        ensure!(maps.len() == types.len() && !maps.is_empty(), "invalid terminal layout");
        ensure!(
            maps.iter().any(|map| *map != OutputMap::Identity),
            "terminal map has no functions"
        );
        let mut program = Self { expressions: Vec::new(), outputs: Vec::new(), predicate: None };
        for (index, (map, oid)) in maps.into_iter().zip(types).enumerate() {
            let expression = if map == OutputMap::Average {
                program.append(Expression::Average(index))
            } else {
                let input = program.append(Expression::Input(index));
                program.map(input, *oid, map)?
            };
            program.outputs.push(Output { label: index.to_string(), expression });
        }
        Ok(program)
    }
    fn append(&mut self, expression: Expression) -> usize {
        let index = self.expressions.len();
        self.expressions.push(expression);
        index
    }
    fn map(&mut self, input: usize, oid: u32, map: OutputMap) -> Result<usize> {
        let (function, arguments) = match map {
            OutputMap::Identity => return Ok(input),
            OutputMap::Numeric => {
                ensure!(oid == 1700, "numeric transport requires numeric input");
                return Ok(input);
            }
            OutputMap::Abs => (Function::Abs { oid }, vec![input]),
            OutputMap::Length => (Function::Length, vec![input]),
            OutputMap::Average => anyhow::bail!("average requires ratio transport"),
            OutputMap::NumericMultiply { factor } => {
                super::super::validate_factor(&factor)?;
                let input = self.append(Expression::Call {
                    function: Function::Numeric,
                    arguments: vec![input],
                });
                let factor = self.append(Expression::Literal(Literal::Numeric(factor)));
                (Function::Multiply, vec![input, factor])
            }
            OutputMap::ArrayElement { delimiter, index } => {
                let input = self.append(Expression::CollateC(input));
                let delimiter = self.append(Expression::Literal(Literal::Text(delimiter)));
                let input = self.append(Expression::Call {
                    function: Function::Split,
                    arguments: vec![input, delimiter],
                });
                return Ok(self.append(Expression::ArrayElement { input, index }));
            }
            OutputMap::TimestampFormat { pattern, utc } => {
                let input = if utc {
                    self.append(Expression::Call {
                        function: Function::Utc,
                        arguments: vec![input],
                    })
                } else {
                    input
                };
                let function = match pattern.as_str() {
                    "YYYY-MM-DD" => Function::FormatDate,
                    "HH24:MI" => Function::FormatMinute,
                    _ => anyhow::bail!("unsupported calendar format"),
                };
                (function, vec![input])
            }
        };
        Ok(self.append(Expression::Call { function, arguments }))
    }
}
