mod boundary;
mod derived;
mod lookup;
mod membership;
mod native_join;
mod partition;
mod relational;
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
        Parsed::Lookup(parsed) => lookup::bind(parsed, contract),
        Parsed::Partition(parsed) => partition::bind(parsed, contract),
    }
}
pub(super) fn legacy(selectors: Query, contract: &Contract) -> Result<Compiled> {
    use super::parser::{native_join::Tree, partition::Value, scalar::Key};
    // Parse fixed placeholders, then inject names without SQL identifier truncation.
    let Parsed::Partition(mut parsed) = super::parser::parse(
        "SELECT l.g AS g,COUNT(*) AS count,SUM(r.v) AS sum \
         FROM ls.lt l JOIN rs.rt r ON l.k=r.k GROUP BY l.g",
    )?
    else {
        anyhow::bail!("legacy template requires a partition");
    };
    let super::parser::partition::Source::Join { tree, validated } = &mut parsed.source else {
        anyhow::bail!("legacy template requires a join");
    };
    let Tree::Join { left, right, keys, .. } = tree else {
        anyhow::bail!("legacy template requires a binary join");
    };
    let (Tree::Source(left), Tree::Source(right)) = (left.as_mut(), right.as_mut()) else {
        anyhow::bail!("legacy template requires native sources");
    };
    left.name = Name(vec![selectors.left_schema, selectors.left_table]);
    right.name = Name(vec![selectors.right_schema, selectors.right_table]);
    *keys = vec![(
        Key::Column(Name(vec!["l".into(), selectors.left_key])),
        Key::Column(Name(vec!["r".into(), selectors.right_key])),
    )];
    *validated = true;
    let [group, _, sum] = parsed.targets.as_mut_slice() else {
        anyhow::bail!("legacy template requires group, count and sum outputs");
    };
    let group_name = Name(vec!["l".into(), selectors.group.clone()]);
    group.value = Value::Column(group_name.clone());
    group.label = selectors.group;
    parsed.groups = vec![Key::Column(group_name)];
    let Value::Aggregate(aggregate) = &mut sum.value else {
        anyhow::bail!("legacy template requires SUM");
    };
    ensure!(
        matches!(aggregate.function, super::partition::Function::Sum),
        "legacy template requires SUM"
    );
    aggregate.argument =
        Some(super::parser::partition::Argument::Column(Name(vec!["r".into(), selectors.sum])));
    partition::bind(parsed, contract)
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
