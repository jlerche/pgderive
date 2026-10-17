mod boundary;
mod derived;
mod lookup;
mod membership;
mod native_join;
mod partition;
mod relational;
mod scoped_join;
mod topk;
use super::{
    ColumnRef, Compiled,
    parser::{Name, Parsed, Table},
};
use crate::{
    source::{Contract, Relation},
    worker::Query,
};
use anyhow::{Context, Result, ensure};
struct Scope<'a> {
    table: Table,
    relation: &'a Relation,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    match parsed {
        Parsed::Membership { parsed, distinct } => membership::bind(parsed, distinct, contract),
        Parsed::ScopedJoin(parsed) => scoped_join::bind(parsed, contract),
        Parsed::Lookup(parsed) => lookup::bind(parsed, contract),
        Parsed::Partition(parsed) => partition::bind(parsed, contract),
    }
}
pub(super) fn legacy(selectors: Query, contract: &Contract) -> Result<Compiled> {
    use super::parser::{
        native_join::Tree,
        partition::{self as syntax, Aggregate, Target, Value},
        scalar::Key,
    };
    use super::partition::Function;
    let group = Name(vec!["l".into(), selectors.group.clone()]);
    let left = Table {
        name: Name(vec![selectors.left_schema, selectors.left_table]),
        alias: Some("l".into()),
    };
    let right = Table {
        name: Name(vec![selectors.right_schema, selectors.right_table]),
        alias: Some("r".into()),
    };
    let mut targets = vec![Target { value: Value::Column(group.clone()), label: selectors.group }];
    for (function, argument, label) in [
        (Function::Count, None, "count"),
        (
            Function::Sum,
            Some(syntax::Argument::Column(Name(vec!["r".into(), selectors.sum]))),
            "sum",
        ),
    ] {
        targets.push(Target {
            value: Value::Aggregate(Box::new(Aggregate {
                function,
                argument,
                filter: None,
                window: None,
                navigation: None,
            })),
            label: label.into(),
        });
    }
    partition::bind(
        syntax::Parsed {
            source: syntax::Source::Join {
                tree: Tree::Join {
                    left: Box::new(Tree::Source(left)),
                    right: Box::new(Tree::Source(right)),
                    keys: vec![(
                        Key::Column(Name(vec!["l".into(), selectors.left_key])),
                        Key::Column(Name(vec!["r".into(), selectors.right_key])),
                    )],
                    outer: false,
                },
                validated: true,
            },
            targets,
            groups: vec![Key::Column(group)],
            predicate: None,
            expansion: None,
        },
        contract,
    )
}

fn scope(table: Table, contract: &Contract) -> Result<Scope<'_>> {
    let relation = contract
        .relations
        .iter()
        .find(|relation| table.name.0 == [relation.schema.clone(), relation.table.clone()])
        .context("SQL resolve: relation absent from native publication")?;
    Ok(Scope { table, relation })
}
fn qualifier(scope: &Scope<'_>) -> String {
    scope.table.alias.clone().unwrap_or_else(|| scope.relation.table.clone())
}
fn resolve(name: &Name, scopes: &[Scope<'_>]) -> Result<ColumnRef> {
    let mut matches = Vec::new();
    for (index, scope) in scopes.iter().enumerate() {
        let qualified = match name.0.as_slice() {
            [_] => true,
            [table, _] => table == &qualifier(scope),
            [schema, table, _] => {
                scope.table.alias.is_none()
                    && schema == &scope.relation.schema
                    && table == &scope.relation.table
            }
            _ => false,
        };
        if qualified
            && let Some(column) =
                scope.relation.columns.iter().find(|column| Some(&column.name) == name.0.last())
        {
            matches.push(ColumnRef {
                right: index == 1,
                name: column.name.clone(),
                oid: column.oid,
                nullable: column.nullable,
            });
        }
    }
    ensure!(matches.len() <= 1, "SQL resolve: ambiguous column {}", name.0.join("."));
    matches.pop().with_context(|| format!("SQL resolve: unknown column {}", name.0.join(".")))
}
