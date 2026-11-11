mod boundary;
mod derived;
mod lookup;
mod membership;
mod native_join;
mod ordering;
mod partition;
mod relational;
mod setop;
mod topk;
mod values;
use super::{
    ColumnRef, Compiled,
    parser::{Name, Parsed, Table},
};
use crate::source::{Contract, Relation};
use anyhow::{Context, Result, ensure};
struct Scope<'a> {
    table: Table,
    relation: &'a Relation,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    match parsed {
        Parsed::With { bindings, inner } => {
            for binding in bindings {
                let planned = planned_binding(&binding);
                let mut compiled = bind(binding, contract)?;
                if planned {
                    super::plan::rewrite(&mut compiled.relational, contract)?;
                }
            }
            bind(*inner, contract)
        }
        Parsed::Renamed { inner, labels } => {
            let mut compiled = bind(*inner, contract)?;
            ensure!(
                labels.len() <= compiled.relational.output.columns.len(),
                "too many derived/CTE column aliases"
            );
            for (column, label) in compiled.relational.output.columns.iter_mut().zip(labels) {
                column.label = label;
            }
            Ok(compiled)
        }
        Parsed::Materialized(inner) => {
            let mut compiled = bind(*inner, contract)?;
            boundary::materialize(&mut compiled.relational)?;
            Ok(compiled)
        }
        Parsed::SetOperation(parsed) => setop::bind(parsed, contract),
        Parsed::Ordered(parsed) => ordering::bind(parsed, contract),
        Parsed::Membership { parsed, distinct } => membership::bind(parsed, distinct, contract),
        Parsed::Lookup(parsed) => lookup::bind(parsed, contract),
        Parsed::Partition(parsed) => partition::bind(parsed, contract),
        Parsed::Values(parsed) => values::bind(parsed, contract),
    }
}

const fn planned_binding(binding: &Parsed) -> bool {
    match binding {
        Parsed::Renamed { inner, .. } => planned_binding(inner),
        Parsed::Materialized(_) => true,
        _ => false,
    }
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
                collation: column.collation,
                modifier: Some(column.modifier).filter(|modifier| *modifier >= 0),
            });
        }
    }
    ensure!(matches.len() <= 1, "SQL resolve: ambiguous column {}", name.0.join("."));
    matches.pop().with_context(|| format!("SQL resolve: unknown column {}", name.0.join(".")))
}
