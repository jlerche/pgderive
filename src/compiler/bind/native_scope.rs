//! Lift native projections into typed scopes without executing a deferred sink map.
use super::{Compiled, scope};
use crate::{
    compiler::{
        Predicate,
        parser::{Columns, Table},
        relational::{Node, Relational, Source},
    },
    source::Contract,
};
use anyhow::Result;
pub(super) fn bind(
    source: Table,
    columns: Columns,
    predicate: Option<crate::compiler::syntax::Expr>,
    contract: &Contract,
) -> Result<Compiled> {
    let scopes = [scope(source, contract)?];
    let resolve = |name: &crate::compiler::parser::Name| {
        Ok(super::relational::qualify(super::resolve(name, &scopes)?))
    };
    let (columns, terminal) = super::relational::output(columns, &resolve)?;
    let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
    let input = if let Some(predicate) = predicate {
        nodes.push(Node::Filter {
            id: "source_filter".into(),
            input: "source".into(),
            predicate: Predicate::new(crate::compiler::expression::bind(predicate, &resolve)?),
        });
        "source_filter"
    } else {
        "source"
    };
    nodes.push(Node::Output { id: "project".into(), input: input.into() });
    let relation = scopes[0].relation;
    let output = crate::compiler::Projected {
        schema: relation.schema.clone(),
        table: relation.table.clone(),
        columns,
        terminal,
    };
    Ok(Compiled {
        relational: Relational {
            sources: vec![Source {
                schema: relation.schema.clone(),
                table: relation.table.clone(),
            }],
            nodes,
            output,
        },
    })
}
