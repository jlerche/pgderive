use super::{Analysis, Column, Identity, Properties};
use crate::compiler::relational::{Node, Relational};
use anyhow::{Result, ensure};
use std::collections::BTreeSet;
fn merge(left: &Properties, right: &Properties) -> Result<Properties> {
    let mut out = left.clone();
    for (name, column) in &right.columns {
        ensure!(
            out.columns.insert(name.clone(), column.clone()).is_none(),
            "property join field collision"
        );
    }
    out.keys.clear();
    out.navigation.clear();
    Ok(out)
}
fn overlap(left: &Properties, right: &Properties) -> bool {
    let identities = left.columns.values().map(|c| &c.identity).collect::<BTreeSet<_>>();
    right.columns.values().any(|c| identities.contains(&c.identity))
}
fn comparable(left: &Properties, right: &Properties) -> bool {
    left.navigation.len() == right.navigation.len()
        && left.navigation.iter().zip(&right.navigation).all(|(a, b)| {
            left.columns.get(a).zip(right.columns.get(b)).is_some_and(|(a, b)| a.compatible(b))
        })
}
pub(super) fn inner(left: &Properties, right: &Properties, id: &str) -> Result<Properties> {
    let mut out = merge(left, right)?;
    out.maximum = product(left.maximum_rows(), right.maximum_rows());
    if overlap(left, right) {
        // Forked row occurrences are independent variables even when they have
        // the same original attribute lineage. Do not collapse their pair key.
        for (name, c) in &mut out.columns {
            c.identity = Identity::Derived(id.into(), name.clone());
        }
        return Ok(out);
    }
    for a in &left.keys {
        for b in &right.keys {
            out.add_key(a.union(b).cloned().collect());
        }
    }
    if comparable(left, right) {
        if right.per_navigation().is_some_and(|n| n <= 1) {
            for key in &left.keys {
                out.add_key(key.clone());
            }
        }
        if left.per_navigation().is_some_and(|n| n <= 1) {
            for key in &right.keys {
                out.add_key(key.clone());
            }
        }
    }
    Ok(out)
}
fn product(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    if left == Some(0) || right == Some(0) {
        Some(0)
    } else {
        left.zip(right).and_then(|(a, b)| a.checked_mul(b))
    }
}
pub(super) fn union(left: &Properties, right: &Properties, id: &str) -> Properties {
    let mut out = Properties::default();
    for (name, a) in &left.columns {
        if let Some(b) = right.columns.get(name)
            && a.reference.oid == b.reference.oid
        {
            let mut column = Column::derived(id, a.reference.clone());
            column.reference.nullable |= b.reference.nullable;
            if a.modifier == b.modifier && a.collation == b.collation {
                column.modifier = a.modifier;
                column.collation = a.collation;
            }
            out.columns.insert(name.clone(), column);
        }
    }
    out.maximum = left.maximum.zip(right.maximum).and_then(|(a, b)| a.checked_add(b));
    out
}
fn node<'a>(ir: &'a Relational, id: &str) -> Option<&'a Node> {
    ir.nodes.iter().find(|n| n.id() == id)
}
impl Analysis {
    pub(super) fn left_join(&self, ir: &Relational, id: &str, input: &str) -> Result<Properties> {
        let Some(Node::Union { left, right, .. }) = node(ir, input) else {
            return Ok(Properties::default());
        };
        let Some(Node::LeftJoinInput { input: a, spec: lk, .. }) = node(ir, left) else {
            return Ok(Properties::default());
        };
        let Some(Node::LeftJoinInput { input: b, spec: rk, .. }) = node(ir, right) else {
            return Ok(Properties::default());
        };
        if !lk.left || rk.left {
            return Ok(Properties::default());
        }
        let mut left = self.edge(a)?.clone();
        left.navigation = lk.keys.iter().map(|k| k.name.clone()).collect();
        let mut right = self.edge(b)?.clone();
        right.navigation = rk.keys.iter().map(|k| k.name.clone()).collect();
        let mut out = merge(&left, &right)?;
        if overlap(&left, &right) {
            for (name, c) in &mut out.columns {
                c.identity = Identity::Derived(id.into(), name.clone());
            }
        } else {
            if comparable(&left, &right) && right.per_navigation().is_some_and(|n| n <= 1) {
                for k in &left.keys {
                    out.add_key(k.clone());
                }
            }
            // Nullable right-hand values are new variables after null extension.
            // A pair key remains unique under NULLS-NOT-DISTINCT row identity.
            let mut remap = std::collections::BTreeMap::new();
            for (name, column) in &right.columns {
                remap
                    .entry(column.identity.clone())
                    .or_insert_with(|| Identity::Derived(id.into(), name.clone()));
            }
            let mapped = right
                .keys
                .iter()
                .filter_map(|key| {
                    key.iter().map(|i| remap.get(i).cloned()).collect::<Option<BTreeSet<_>>>()
                })
                .collect::<Vec<_>>();
            for a in &left.keys {
                for b in &mapped {
                    out.add_key(a.union(b).cloned().collect());
                }
            }
            for (name, c) in &mut out.columns {
                if let Some(old) = right.columns.get(name) {
                    c.identity = remap[&old.identity].clone();
                }
            }
        }
        for name in right.columns.keys() {
            if let Some(c) = out.columns.get_mut(name) {
                c.reference.nullable = true;
            }
        }
        out.prune();
        out.maximum = left.maximum.zip(right.maximum).and_then(|(a, b)| a.checked_mul(b.max(1)));
        Ok(out)
    }
    pub(super) fn semi_join(&self, ir: &Relational, input: &str) -> Result<Properties> {
        let Some(Node::Union { left, .. }) = node(ir, input) else {
            return Ok(Properties::default());
        };
        let Some(Node::LeftJoinInput { input, spec, .. }) = node(ir, left) else {
            return Ok(Properties::default());
        };
        if !spec.left {
            return Ok(Properties::default());
        }
        let mut facts = self.edge(input)?.clone();
        // Tagged existence inputs rekey rows; the pre-tag navigation is stale.
        facts.navigation.clear();
        Ok(facts)
    }
}
