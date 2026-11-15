//! Expand SQL grouping syntax into a bounded bag of sets of expression indexes.
use super::{
    node,
    partition::{Target, group_key},
    scalar::Key,
};
use anyhow::{Result, ensure};
use pg_query::{
    Node, NodeEnum,
    protobuf::{GroupingSetKind, SelectStmt},
};

const MAX_SETS: usize = 64;

#[derive(Clone)]
pub(in crate::compiler) struct Sets {
    pub groups: Vec<Vec<usize>>,
    pub distinct: bool,
    pub correlation_keys: usize,
}

pub(super) struct Grouping {
    pub keys: Vec<Key>,
    pub sets: Option<Sets>,
}

pub(super) fn parse(select: &SelectStmt, targets: &[Target]) -> Result<Grouping> {
    if !select
        .group_clause
        .iter()
        .any(|n| matches!(&n.node, Some(NodeEnum::GroupingSet(_))) || row_group(n))
    {
        let keys =
            select.group_clause.iter().map(|n| group_key(n, targets)).collect::<Result<_>>()?;
        return Ok(Grouping { keys, sets: None });
    }
    let mut keys = Vec::new();
    let mut groups = vec![Vec::new()];
    for value in &select.group_clause {
        groups = product(&groups, &expand(value, targets, &mut keys, 0)?)?;
    }
    Ok(Grouping {
        keys,
        sets: Some(Sets { groups, distinct: select.group_distinct, correlation_keys: 0 }),
    })
}

fn product(left: &[Vec<usize>], right: &[Vec<usize>]) -> Result<Vec<Vec<usize>>> {
    ensure!(left.len().saturating_mul(right.len()) <= MAX_SETS, "grouping set count exceeds 64");
    Ok(left
        .iter()
        .flat_map(|a| right.iter().map(move |b| a.iter().chain(b).copied().collect()))
        .collect())
}

fn expand(
    value: &Node,
    targets: &[Target],
    keys: &mut Vec<Key>,
    depth: usize,
) -> Result<Vec<Vec<usize>>> {
    ensure!(depth < 16, "grouping set nesting exceeds 16");
    match node(value)? {
        NodeEnum::GroupingSet(set) => {
            use GroupingSetKind::{
                GroupingSetCube, GroupingSetEmpty, GroupingSetRollup, GroupingSetSets,
                GroupingSetSimple, Undefined,
            };
            let children = || set.content.iter();
            match GroupingSetKind::try_from(set.kind)? {
                GroupingSetEmpty => Ok(vec![Vec::new()]),
                GroupingSetSets => {
                    let mut result = Vec::new();
                    for child in children() {
                        result.extend(expand(child, targets, keys, depth + 1)?);
                        ensure!(result.len() <= MAX_SETS, "grouping set count exceeds 64");
                    }
                    Ok(result)
                }
                GroupingSetRollup | GroupingSetCube | GroupingSetSimple => {
                    let mut result = vec![Vec::new()];
                    let mut prefix = vec![Vec::new()];
                    for child in children() {
                        let child = expand(child, targets, keys, depth + 1)?;
                        match GroupingSetKind::try_from(set.kind)? {
                            GroupingSetRollup => {
                                prefix = product(&prefix, &child)?;
                                result.extend(prefix.iter().cloned());
                            }
                            GroupingSetCube => {
                                let included = product(&result, &child)?;
                                result.extend(included);
                            }
                            _ => result = product(&result, &child)?,
                        }
                        ensure!(result.len() <= MAX_SETS, "grouping set count exceeds 64");
                    }
                    Ok(result)
                }
                Undefined => anyhow::bail!("invalid grouping set kind"),
            }
        }
        NodeEnum::RowExpr(row) if row_group(value) => {
            let mut result = vec![Vec::new()];
            for arg in &row.args {
                result = product(&result, &expand(arg, targets, keys, depth + 1)?)?;
            }
            Ok(result)
        }
        _ => {
            ensure!(keys.len() < 64, "grouping expression count exceeds 64");
            let index = keys.len();
            keys.push(group_key(value, targets)?);
            Ok(vec![vec![index]])
        }
    }
}

fn row_group(value: &Node) -> bool {
    matches!(&value.node, Some(NodeEnum::RowExpr(row))
        if row.row_format == i32::from(pg_query::protobuf::CoercionForm::CoerceImplicitCast))
}
