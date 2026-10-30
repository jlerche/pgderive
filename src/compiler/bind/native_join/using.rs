//! USING keeps qualified originals and exposes one merged unqualified key.
use super::{Builder, Column, Edge, keys, resolve};
use crate::compiler::{left_join::Kind, parser::Name, scalar, typed};
use anyhow::{Result, ensure};
use pg_query::Node as Ast;
use std::collections::BTreeSet;
pub(super) struct Pair {
    pub(super) left: Column,
    pub(super) right: Column,
    pub(super) oid: u32,
}
impl Builder<'_> {
    pub(super) fn native_join(
        &mut self,
        left: Edge,
        right: Edge,
        condition: Option<&Ast>,
        clause: (Kind, Vec<String>),
    ) -> Result<Edge> {
        let (kind, names) = clause;
        let id = format!("tree_join_{}", self.joins);
        if names.is_empty() {
            let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
            ensure!(
                kind != Kind::Full
                    || !bound.keys.is_empty()
                    || bound.predicate.as_ref().is_none_or(crate::compiler::Predicate::is_constant),
                "FULL JOIN requires a supported cross-input equality key or a fully constant ON condition"
            );
            return self.join(left, right, bound, kind);
        }
        ensure!(condition.is_none() && names.len() <= 16, "USING key count/clause unsupported");
        let mut seen = BTreeSet::new();
        let pairs = names
            .iter()
            .map(|name| {
                ensure!(seen.insert(name), "duplicate USING column");
                let name = Name(vec![name.clone()]);
                let left = resolve(&name, &left.columns)?.clone();
                let right = resolve(&name, &right.columns)?.clone();
                let oid = common(left.value.oid, right.value.oid)?;
                Ok(Pair { left, right, oid })
            })
            .collect::<Result<Vec<_>>>()?;
        let bound = keys::using(&pairs, &id)?;
        let mut edge = self.join(left, right, bound, kind)?;
        let mut computed = Vec::new();
        let mut merged = Vec::new();
        for (name, pair) in names.into_iter().zip(pairs) {
            let refs = [&pair.left.value, &pair.right.value].map(|original| {
                edge.columns
                    .iter()
                    .find(|column| column.value.name == original.name)
                    .map(|column| column.value.clone())
                    .ok_or_else(|| anyhow::anyhow!("missing USING input after null extension"))
            });
            let [left, right] = refs;
            let expression = typed::Typed::using_value(
                (left?, right?),
                kind,
                pair.oid,
                (pair.left.modifier, pair.right.modifier),
            )?;
            let modifier = if pair.left.value.oid == pair.right.value.oid
                && pair.left.modifier == pair.right.modifier
            {
                pair.left.modifier
            } else {
                -1
            };
            let collation = expression.collation(&|value| {
                Ok(if value.name == pair.left.value.name {
                    pair.left.collation
                } else {
                    pair.right.collation
                })
            })?;
            let value = scalar::register(expression, &mut computed, &format!("@{id}_using_"))?;
            for column in &mut edge.columns {
                if column.label == name {
                    column.unqualified = false;
                }
            }
            merged.push(Column {
                label: name,
                qualifiers: Vec::new(),
                value,
                modifier,
                collation,
                unqualified: true,
            });
        }
        self.map(&mut edge, computed, &format!("{id}_using"));
        merged.extend(edge.columns);
        edge.columns = merged;
        Ok(edge)
    }
}
fn common(left: u32, right: u32) -> Result<u32> {
    let oid = match (left, right) {
        (a, b) if a == b => a,
        (20 | 21 | 23 | 1700, 20 | 21 | 23 | 1700) => [1700, 20, 23, 21]
            .into_iter()
            .find(|oid| *oid == left || *oid == right)
            .ok_or_else(|| anyhow::anyhow!("missing USING numeric type"))?,
        (25 | 1043, 25 | 1043) => left,
        (1114 | 1184, 1114 | 1184) => 1184,
        _ => anyhow::bail!("USING columns have incompatible native types"),
    };
    ensure!(
        matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1700 | 2950 | 1114 | 1184),
        "unsupported USING native type"
    );
    Ok(oid)
}
