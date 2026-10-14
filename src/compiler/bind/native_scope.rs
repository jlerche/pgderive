//! Lift native projections into typed scopes without executing a deferred sink map.
use super::{Compiled, scope};
use crate::{
    compiler::{
        Predicate, Program,
        parser::{Columns, Table},
        relational::{Node, Relational, Source},
    },
    source::Contract,
};
use anyhow::{Result, ensure};
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
    ensure!(
        terminal.as_ref().is_none_or(crate::catalog::Terminal::native_rendering),
        "native scope cannot consume sink-deferred expressions"
    );
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
        terminal: None,
    };
    Ok(Compiled {
        program: Program::Relational { relational: Relational { sources: vec![Source { schema: relation.schema.clone(), table: relation.table.clone() }], nodes, output } },
        predicates: None,
        revision: Some("sql-native-scope-v1:pg-query-6.2.1:pg-17.7:native-field-map-v1:where-3vl-v1:raw-record-v1:json-v2".into()),
    })
}
