use super::{Builder, Expression, Function, names, node, optional};
use anyhow::{Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
impl Builder<'_> {
    pub(super) fn operator(&mut self, value: &pg::AExpr, depth: usize) -> Result<usize> {
        ensure!(
            value.kind == i32::from(pg::AExprKind::AexprOp),
            "unsupported terminal operator kind"
        );
        let names = names(&value.name)?.0;
        let [operator] = names.as_slice() else {
            anyhow::bail!("qualified terminal operators unsupported");
        };
        let function = match operator.as_str() {
            "*" => Function::Multiply,
            ">" => Function::Greater,
            "<" => Function::Less,
            ">=" => Function::GreaterEqual,
            "<=" => Function::LessEqual,
            "-" => Function::Subtract,
            _ => anyhow::bail!("unsupported terminal operator"),
        };
        let left = self.expression(optional(value.lexpr.as_deref())?, depth)?;
        let right = self.expression(optional(value.rexpr.as_deref())?, depth)?;
        if function == Function::Multiply {
            ensure!(
                self.types.get(left) == Some(&1700) || self.types.get(right) == Some(&1700),
                "integral multiplication requires native overflow semantics and is unsupported"
            );
        }
        let arguments = if function == Function::Subtract {
            vec![left, right]
        } else {
            vec![self.numeric(left)?, self.numeric(right)?]
        };
        self.push(Expression::Call { function, arguments })
    }
    pub(super) fn call(&mut self, value: &pg::FuncCall, depth: usize) -> Result<usize> {
        let name = names(&value.funcname)?.0;
        let function = match name.as_slice() {
            [name] => name.as_str(),
            [schema, name] if schema == "pg_catalog" => name.as_str(),
            _ => anyhow::bail!("unsupported terminal function namespace"),
        };
        let mut plain = value.clone();
        if matches!(function, "extract" | "timezone") {
            ensure!(
                matches!(plain.funcformat, value if value == i32::from(pg::CoercionForm::CoerceSqlSyntax) || value == i32::from(pg::CoercionForm::CoerceExplicitCall)),
                "invalid terminal function syntax"
            );
            plain.funcformat = i32::from(pg::CoercionForm::CoerceExplicitCall);
        }
        crate::compiler::parser::scalar::validate_call(&plain)?;
        match function {
            "length" | "replace" => {
                let function =
                    if function == "length" { Function::Length } else { Function::Replace };
                let arguments = value
                    .args
                    .iter()
                    .map(|value| self.expression(value, depth))
                    .collect::<Result<Vec<_>>>()?;
                self.push(Expression::Call { function, arguments })
            }
            "extract" | "timezone" => {
                let [literal, input] = value.args.as_slice() else {
                    anyhow::bail!("calendar function requires two arguments");
                };
                let text = crate::compiler::parser::scalar::string(literal)?;
                ensure!(
                    text == if function == "extract" { "hour" } else { "UTC" },
                    "only hour extraction and explicit UTC supported"
                );
                let input = self.expression(input, depth)?;
                let function = if function == "extract" { Function::Hour } else { Function::Utc };
                self.push(Expression::Call { function, arguments: vec![input] })
            }
            _ => anyhow::bail!("unsupported terminal built-in"),
        }
    }
    pub(super) fn boolean(&mut self, value: &pg::BoolExpr, depth: usize) -> Result<usize> {
        let arguments = value
            .args
            .iter()
            .map(|value| self.expression(value, depth))
            .collect::<Result<Vec<_>>>()?;
        let expression = match pg::BoolExprType::try_from(value.boolop)? {
            pg::BoolExprType::AndExpr => Expression::And(arguments),
            pg::BoolExprType::OrExpr => Expression::Or(arguments),
            pg::BoolExprType::NotExpr => {
                let [input] = arguments.as_slice() else {
                    anyhow::bail!("NOT requires one operand");
                };
                Expression::Not(*input)
            }
            pg::BoolExprType::Undefined => anyhow::bail!("invalid terminal boolean operator"),
        };
        self.push(expression)
    }
    pub(super) fn case(&mut self, value: &pg::CaseExpr, depth: usize) -> Result<usize> {
        ensure!(
            value.arg.is_none() && value.xpr.is_none(),
            "only searched terminal CASE supported"
        );
        let mut branches = Vec::new();
        for value in &value.args {
            let NodeEnum::CaseWhen(value) = node(value)? else {
                anyhow::bail!("invalid terminal CASE arm");
            };
            ensure!(value.xpr.is_none(), "invalid terminal CASE expression");
            branches.push((
                self.expression(optional(value.expr.as_deref())?, depth)?,
                self.expression(optional(value.result.as_deref())?, depth)?,
            ));
        }
        let otherwise = self.expression(optional(value.defresult.as_deref())?, depth)?;
        self.push(Expression::Case { branches, otherwise })
    }
}
