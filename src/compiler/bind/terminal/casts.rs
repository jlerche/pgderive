//! Closed native integral casts; `PostgreSQL` evaluates narrowing overflow at publication.
use super::{Builder, Expression, Function, names, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
impl Builder<'_> {
    pub(super) fn cast(&mut self, value: &pg::TypeCast, depth: usize) -> Result<usize> {
        let kind = value.type_name.as_ref().ok_or_else(|| anyhow::anyhow!("missing cast type"))?;
        ensure!(
            crate::compiler::parser::expressions::unmodified_type(kind),
            "unsupported cast modifier"
        );
        let name = names(&kind.names)?.0;
        let name = crate::compiler::parser::expressions::builtin_name(&name)
            .context("unsupported cast namespace")?;
        let (function, oid) = match name {
            "int2" => (Function::Int2, 21),
            "int4" => (Function::Int4, 23),
            "int8" => (Function::Int8, 20),
            "numeric" => (Function::Numeric, 1700),
            _ => anyhow::bail!("unsupported terminal cast type"),
        };
        let argument = optional(value.arg.as_deref())?;
        if matches!(super::node(argument)?, NodeEnum::AConst(value) if value.isnull) {
            return self.push(Expression::Null(oid));
        }
        let input = self.expression(argument, depth)?;
        if self.types.get(input) == Some(&oid) {
            return Ok(input);
        }
        self.push(Expression::Call { function, arguments: vec![input] })
    }
}

impl Builder<'_> {
    pub(super) fn coalesce(&mut self, value: &pg::CoalesceExpr, depth: usize) -> Result<usize> {
        ensure!(value.xpr.is_none(), "invalid COALESCE AST");
        ensure!((1..=64).contains(&value.args.len()), "invalid COALESCE arity");
        let mut nullable = Vec::new();
        for argument in &value.args {
            nullable.push(
                if matches!(super::node(argument)?, NodeEnum::AConst(value) if value.isnull) {
                    None
                } else {
                    Some(self.expression(argument, depth)?)
                },
            );
        }
        let types = nullable.iter().flatten().map(|index| self.types[*index]).collect::<Vec<_>>();
        let integral = !types.is_empty() && types.iter().all(|oid| matches!(oid, 20 | 21 | 23));
        let target = if integral {
            crate::compiler::scalar::common(&types)
        } else {
            types.first().copied().unwrap_or(25)
        };
        let mut arguments = Vec::new();
        for index in nullable {
            let index = self.coalesce_operand(index, target, integral)?;
            arguments.push(index);
        }
        self.push(Expression::Coalesce(arguments))
    }
}

impl Builder<'_> {
    pub(super) fn integral_operator(
        &mut self,
        operator: &str,
        left: usize,
        right: usize,
    ) -> Result<usize> {
        use crate::catalog::terminal::program::Operation;
        let oid = crate::compiler::scalar::common(&[self.types[left], self.types[right]]);
        let mut arguments = vec![left, right];
        for index in &mut arguments {
            if self.types[*index] != oid {
                let function = if oid == 20 { Function::Int8 } else { Function::Int4 };
                *index = self.push(Expression::Call { function, arguments: vec![*index] })?;
            }
        }
        let operation = match operator {
            "+" => Operation::Add,
            "-" => Operation::Subtract,
            "*" => Operation::Multiply,
            _ => anyhow::bail!("invalid integral operator"),
        };
        // Preserve the existing int4 subtraction identity.
        let function = if operation == Operation::Subtract && oid == 23 {
            Function::Subtract
        } else {
            Function::Integral { operation, oid }
        };
        self.push(Expression::Call { function, arguments })
    }
}

impl Builder<'_> {
    fn coalesce_operand(
        &mut self,
        index: Option<usize>,
        target: u32,
        integral: bool,
    ) -> Result<usize> {
        let Some(index) = index else {
            return self.push(Expression::Null(target));
        };
        if !integral || self.types[index] == target {
            return Ok(index);
        }
        let function = if target == 20 { Function::Int8 } else { Function::Int4 };
        self.push(Expression::Call { function, arguments: vec![index] })
    }
}
