mod derived;
mod partition;
mod relational;
use super::{
    ColumnRef, Compiled, REVISION,
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
        Parsed::Derived(parsed) => derived::bind_derived(parsed, contract),
        Parsed::Grouped(parsed) => grouped(parsed, contract),
        Parsed::Partition(parsed) => partition::bind(parsed, contract),
        Parsed::JoinProjection { left, right, keys, columns, predicate } => {
            relational::bind((left, right), &keys, columns, predicate, contract)
        }
        Parsed::Projection { source, columns, predicate } => {
            let scopes = [scope(source, contract)?];
            let transforms =
                columns.iter().map(|(_, _, transform)| transform.clone()).collect::<Vec<_>>();
            let columns = columns
                .into_iter()
                .map(|(name, label, _)| {
                    Ok(super::projection::OutputColumn { column: resolve(&name, &scopes)?, label })
                })
                .collect::<Result<Vec<_>>>()?;
            let terminal = terminal(transforms, &columns)?;
            let revision = if terminal.is_some() {
                "sql-projection-v2:pg-query-6.2.1:pg-17.7:where-3vl-v1:row-text-v1:json-v2:native-bag-v1:terminal-builtins-v1"
            } else {
                "sql-projection-v1:pg-query-6.2.1:pg-17.7:where-3vl-v1:row-text-v1:json-v2:native-bag-v1"
            };
            let predicates = predicate
                .map(|expr| super::expression::bind(expr, &|name| resolve(name, &scopes)))
                .transpose()?;
            Ok(Compiled {
                program: super::Program::Projection {
                    projection: super::Projected {
                        schema: scopes[0].relation.schema.clone(),
                        table: scopes[0].relation.table.clone(),
                        columns,
                        terminal,
                    },
                },
                revision: Some(predicate_revision(revision, predicates.as_ref())),
                predicates,
            })
        }
    }
}
fn grouped(parsed: super::parser::Grouped, contract: &Contract) -> Result<Compiled> {
    let left = scope(parsed.left, contract)?;
    let right = scope(parsed.right, contract)?;
    ensure!(left.relation.oid != right.relation.oid, "SQL resolve: self joins are unsupported");
    ensure!(
        qualifier(&left) != qualifier(&right),
        "SQL resolve: duplicate table qualifier; use aliases"
    );
    let scopes = [left, right];
    let group = resolve(&parsed.group, &scopes)?;
    ensure!(
        group == resolve(&parsed.grouping, &scopes)?,
        "SQL type: SELECT group must equal GROUP BY column"
    );
    ensure!(!group.right, "SQL lower: grouping column must come from left input");
    // Text equality/grouping depends on PostgreSQL collation, which the string
    // arrangement cannot execute. Restrict SQL equality/grouping to native scalars.
    ensure!(
        matches!(group.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
        "SQL type: text/varchar grouping requires a collation runtime"
    );
    let sum = resolve(&parsed.sum, &scopes)?;
    ensure!(
        sum.right && matches!(sum.oid, 20 | 21 | 23),
        "SQL type: SUM requires a right integral column"
    );
    let lhs = resolve(&parsed.keys.0, &scopes)?;
    let rhs = resolve(&parsed.keys.1, &scopes)?;
    ensure!(lhs.right != rhs.right, "SQL type: join equality must connect both inputs");
    ensure!(
        matches!(lhs.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
        "SQL type: text/varchar join requires a collation runtime"
    );
    let (lhs, rhs) = if lhs.right { (rhs, lhs) } else { (lhs, rhs) };
    let selectors = Query {
        left_schema: scopes[0].relation.schema.clone(),
        left_table: scopes[0].relation.table.clone(),
        left_key: lhs.name,
        group: group.name,
        right_schema: scopes[1].relation.schema.clone(),
        right_table: scopes[1].relation.table.clone(),
        right_key: rhs.name,
        sum: sum.name,
    };
    selectors.validate(contract).context("SQL type checking")?;
    let predicates = parsed
        .predicates
        .map(|expr| super::expression::bind(expr, &|name| resolve(name, &scopes)))
        .transpose()?;
    Ok(Compiled {
        program: super::Program::Grouped { selectors },
        revision: Some(predicate_revision(REVISION, predicates.as_ref())),
        predicates,
    })
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

fn predicate_revision(base: &str, predicate: Option<&super::expression::Expr>) -> String {
    if predicate.is_some_and(super::expression::Expr::has_remainder) {
        format!("{base}:integral-remainder-v1")
    } else {
        base.into()
    }
}

fn terminal(
    mut transforms: Vec<crate::catalog::terminal::Transform>,
    columns: &[crate::compiler::projection::OutputColumn],
) -> Result<Option<crate::catalog::Terminal>> {
    use crate::catalog::terminal::Transform;
    for (transform, column) in transforms.iter_mut().zip(columns) {
        if *transform == Transform::Identity && column.column.oid == 1700 {
            *transform = Transform::Numeric;
        }
    }
    if transforms.iter().all(|transform| *transform == Transform::Identity) {
        return Ok(None);
    }
    Ok(Some(crate::catalog::Terminal::new(
        transforms,
        columns.iter().map(|output| output.column.oid).collect(),
    )?))
}
