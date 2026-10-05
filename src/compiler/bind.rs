use super::{
    ColumnRef, Compiled, NullTest, REVISION,
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
        matches!(group.oid, 16 | 20 | 21 | 23 | 2950),
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
        matches!(lhs.oid, 16 | 20 | 21 | 23 | 2950),
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
    let mut predicates = parsed
        .predicates
        .into_iter()
        .map(|(name, not)| Ok(NullTest { column: resolve(&name, &scopes)?, not }))
        .collect::<Result<Vec<_>>>()?;
    predicates.sort();
    predicates.dedup();
    Ok(Compiled { selectors, predicates, revision: Some(REVISION.into()) })
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
fn resolve(name: &Name, scopes: &[Scope<'_>; 2]) -> Result<ColumnRef> {
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
