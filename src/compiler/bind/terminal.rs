//! Terminal-only `PostgreSQL` expressions over explicit native input operands.
use super::{Scope, resolve, scope};
use crate::{
    catalog::{
        Terminal,
        terminal::program::{Expression, Function, Literal, Output, Program, expression_type},
    },
    compiler::{
        ColumnRef, Compiled, Projected,
        parser::{
            Table,
            expressions::{column, names, node, optional, target},
        },
        projection::OutputColumn,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
mod casts;
mod expressions;
mod text;
struct Builder<'a> {
    scope: Scope<'a>,
    columns: Vec<OutputColumn>,
    program: Program,
    types: Vec<u32>,
}
pub(super) fn bind(
    source: Table,
    targets: &[Node],
    predicate: Option<&Node>,
    contract: &Contract,
) -> Result<Compiled> {
    let mut builder = Builder {
        scope: scope(source, contract)?,
        columns: Vec::new(),
        types: Vec::new(),
        program: Program { expressions: Vec::new(), outputs: Vec::new(), predicate: None },
    };
    for value in targets {
        let expression = builder.expression(target(value)?, 0)?;
        let NodeEnum::ResTarget(output) = node(value)? else {
            anyhow::bail!("invalid output target");
        };
        let label = if output.name.is_empty() {
            column(target(value)?)?
                .0
                .last()
                .context("computed terminal outputs require explicit aliases")?
                .clone()
        } else {
            output.name.clone()
        };
        builder.program.outputs.push(Output { label, expression });
    }
    builder.program.predicate = predicate.map(|value| builder.expression(value, 0)).transpose()?;
    let revision = if builder.program.has_collation() {
        builder.program.prune()?;
        "sql-terminal-program-v1:pg-query-6.2.1:pg-17.7:typed-native-dag-v1:terminal-3vl-v1:raw-bag-v1:json-v2:pg-terminal-c-text-regex-v1"
    } else {
        "sql-terminal-program-v1:pg-query-6.2.1:pg-17.7:typed-native-dag-v1:terminal-3vl-v1:raw-bag-v1:json-v2"
    };
    if builder.columns.is_empty() {
        let column = builder
            .scope
            .relation
            .columns
            .first()
            .context("constant projection requires source column")?;
        builder.columns.push(OutputColumn {
            label: column.name.clone(),
            column: ColumnRef {
                name: column.name.clone(),
                right: false,
                oid: column.oid,
                nullable: column.nullable,
            },
        });
    }
    let input_types = builder.columns.iter().map(|output| output.column.oid).collect();
    let extended = builder.program.expressions.iter().any(|value| {
        matches!(
            value,
            Expression::Null(_)
                | Expression::Coalesce(_)
                | Expression::Call {
                    function: Function::Int2
                        | Function::Int4
                        | Function::Int8
                        | Function::Integral { .. }
                        | Function::Calendar(_),
                    ..
                }
        )
    });
    let revision = if extended {
        format!("{revision}:pg-typed-terminal-integral-calendar-v2")
    } else {
        revision.into()
    };
    let terminal = Terminal::from_program(builder.program, input_types)?;
    Ok(Compiled {
        program: crate::compiler::Program::Projection {
            projection: Projected {
                schema: builder.scope.relation.schema.clone(),
                table: builder.scope.relation.table.clone(),
                columns: builder.columns,
                terminal: Some(terminal),
            },
        },
        revision: Some(revision),
        predicates: None,
    })
}
impl Builder<'_> {
    fn push(&mut self, expression: Expression) -> Result<usize> {
        let expression =
            if self.program.has_collation() { self.flatten(expression) } else { expression };
        ensure!(self.types.len() < 512, "terminal expression budget exceeded");
        let inputs = self.columns.iter().map(|output| output.column.oid).collect::<Vec<_>>();
        let oid = expression_type(&expression, &self.types, &inputs)?;
        if let Some(index) = self.program.expressions.iter().position(|prior| prior == &expression)
        {
            return Ok(index);
        }
        let index = self.types.len();
        self.types.push(oid);
        self.program.expressions.push(expression);
        Ok(index)
    }
    fn flatten(&self, expression: Expression) -> Expression {
        let (arguments, conjunction) = match expression {
            Expression::And(arguments) => (arguments, true),
            Expression::Or(arguments) => (arguments, false),
            other => return other,
        };
        let arguments = arguments
            .into_iter()
            .flat_map(|index| match self.program.expressions.get(index) {
                Some(Expression::And(children)) if conjunction => children.clone(),
                Some(Expression::Or(children)) if !conjunction => children.clone(),
                _ => vec![index],
            })
            .collect();
        if conjunction { Expression::And(arguments) } else { Expression::Or(arguments) }
    }
    fn input(&mut self, value: &Node) -> Result<usize> {
        let column: ColumnRef = resolve(&column(value)?, std::slice::from_ref(&self.scope))?;
        let index =
            if let Some(index) = self.columns.iter().position(|output| output.column == column) {
                index
            } else {
                ensure!(self.columns.len() < 64, "terminal input budget exceeded");
                let index = self.columns.len();
                self.columns.push(OutputColumn { label: column.name.clone(), column });
                index
            };
        self.push(Expression::Input(index))
    }
    fn numeric(&mut self, index: usize) -> Result<usize> {
        if self.types.get(index) == Some(&1700) {
            return Ok(index);
        }
        self.push(Expression::Call { function: Function::Numeric, arguments: vec![index] })
    }
    fn expression(&mut self, value: &Node, depth: usize) -> Result<usize> {
        ensure!(depth < 64, "terminal expression depth exceeded");
        match node(value)? {
            NodeEnum::ColumnRef(_) => self.input(value),
            NodeEnum::TypeCast(value) => self.cast(value, depth + 1),
            NodeEnum::AConst(value) => self.constant(value),
            NodeEnum::AExpr(value) => self.operator(value, depth + 1),
            NodeEnum::FuncCall(value) => self.call(value, depth + 1),
            NodeEnum::BoolExpr(value) => self.boolean(value, depth + 1),
            NodeEnum::CaseExpr(value) => self.case(value, depth + 1),
            NodeEnum::CoalesceExpr(value) => self.coalesce(value, depth + 1),
            NodeEnum::CollateClause(value) => self.collate(value, depth + 1),
            NodeEnum::AIndirection(value) => self.array(value, depth + 1),
            NodeEnum::NullTest(value) => self.null_test(value, depth + 1),
            _ => anyhow::bail!("SQL lower: unsupported terminal expression"),
        }
    }
    fn constant(&mut self, value: &pg::AConst) -> Result<usize> {
        ensure!(!value.isnull, "untyped terminal NULL literal unsupported");
        let literal = match &value.val {
            Some(pg::a_const::Val::Ival(value)) => Literal::Integer(value.ival),
            Some(pg::a_const::Val::Fval(value)) => {
                ensure!(
                    value.fval.contains('.'),
                    "terminal numeric literal requires explicit decimal syntax"
                );
                Literal::Numeric(value.fval.clone())
            }
            Some(pg::a_const::Val::Sval(value)) => Literal::Text(value.sval.clone()),
            _ => anyhow::bail!("unsupported terminal literal"),
        };
        self.push(Expression::Literal(literal))
    }
}
