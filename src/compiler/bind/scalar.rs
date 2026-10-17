//! Bind native scalar expressions to engine Map/Filter operators.
use super::{resolve, scope};
use crate::compiler::{
    ColumnRef, Compiled, Predicate, Projected,
    parser::{
        Table,
        expressions::{column, node, target},
    },
    projection::OutputColumn,
    relational::{Node, Relational, Source},
    scalar::Computed,
};
use crate::source::Contract;
use anyhow::{Context, Result};
use pg_query::{Node as Ast, NodeEnum};

pub(super) fn bind(
    source: Table,
    targets: &[Ast],
    predicate: Option<&Ast>,
    contract: &Contract,
) -> Result<Compiled> {
    let scope = scope(source, contract)?;
    let native = |name: &crate::compiler::parser::Name| -> Result<ColumnRef> {
        let mut column = resolve(name, std::slice::from_ref(&scope))?;
        column.name = crate::compiler::relational::field(0, &column.name);
        Ok(column)
    };
    let mut computed = Vec::new();
    let columns = targets
        .iter()
        .map(|value| bind_target(value, &native, &mut computed))
        .collect::<Result<Vec<_>>>()?;
    let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
    let mut input = "source";
    if let Some(predicate) = crate::compiler::syntax::predicate(predicate)? {
        let predicate = crate::compiler::expression::bind(&predicate, &native)?;
        nodes.push(Node::Filter {
            id: "filter".into(),
            input: input.into(),
            predicate: Predicate::new(predicate),
        });
        input = "filter";
    }
    if !computed.is_empty() {
        nodes.push(Node::Map { id: "mapped".into(), input: input.into(), computed });
        input = "mapped";
    }
    nodes.push(Node::Output { id: "project".into(), input: input.into() });
    let output = Projected {
        schema: scope.relation.schema.clone(),
        table: scope.relation.table.clone(),
        columns,
    };
    Ok(Compiled {
        result_columns: Vec::new(),
        relational: Relational {
            sources: vec![Source { schema: output.schema.clone(), table: output.table.clone() }],
            nodes,
            output,
        },
    })
}
fn bind_target(
    value: &Ast,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
) -> Result<OutputColumn> {
    let NodeEnum::ResTarget(output) = node(value)? else {
        anyhow::bail!("invalid scalar output");
    };
    let expression = target(value)?;
    let column = if let Ok(name) = column(expression) {
        resolve(&name)?
    } else {
        let description = if let NodeEnum::FuncCall(function) = node(expression)? {
            format!(
                "function {}",
                crate::compiler::parser::expressions::names(&function.funcname)?.0.join(".")
            )
        } else {
            "expression".into()
        };
        crate::compiler::typed::computed(expression, resolve, computed, None)
            .with_context(|| format!("{description} is not available in-engine"))?
    };
    let label = if output.name.is_empty() {
        column.name.split(':').next_back().context("scalar output label absent")?.into()
    } else {
        output.name.clone()
    };
    Ok(OutputColumn { column, label })
}
