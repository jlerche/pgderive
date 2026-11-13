//! NATURAL and USING keep qualified originals and expose one merged key per common name.
use super::{Builder, Column, Edge, keys, resolve};
use crate::compiler::{
    left_join::Kind,
    parser::{Name, native_join::JoinClause},
    typed,
};
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
        kind: Kind,
        clause: JoinClause,
    ) -> Result<Edge> {
        let names = match clause {
            JoinClause::Condition(condition) => {
                return self.join_on(left, right, kind, condition.as_deref());
            }
            JoinClause::Using(names) => names,
            JoinClause::Natural => natural_names(&left.columns, &right.columns),
        };
        if names.is_empty() {
            return self.join_on(left, right, kind, None);
        }
        self.join_using(left, right, kind, names)
    }

    fn join_on(
        &mut self,
        left: Edge,
        right: Edge,
        kind: Kind,
        condition: Option<&Ast>,
    ) -> Result<Edge> {
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
        ensure!(
            kind != Kind::Full
                || !bound.keys.is_empty()
                || bound.predicate.as_ref().is_none_or(crate::compiler::Predicate::is_constant),
            "FULL JOIN requires a supported cross-input equality key or a fully constant ON condition"
        );
        self.join(left, right, bound, kind)
    }

    fn join_using(
        &mut self,
        left: Edge,
        right: Edge,
        kind: Kind,
        names: Vec<String>,
    ) -> Result<Edge> {
        let id = format!("tree_join_{}", self.joins);
        ensure!(names.len() <= 16, "USING key count unsupported");
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
            let value = expression.projected_column(&mut computed, &format!("@{id}_using_"))?;
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
fn natural_names(left: &[Column], right: &[Column]) -> Vec<String> {
    let right = right
        .iter()
        .filter(|column| column.unqualified)
        .map(|column| column.label.as_str())
        .collect::<BTreeSet<_>>();
    // Preserve the exposed left-column order. USING checks that every name
    // resolves to exactly one column on each side.
    left.iter()
        .filter(|column| column.unqualified && right.contains(column.label.as_str()))
        .map(|column| column.label.clone())
        .collect()
}

fn common(left: u32, right: u32) -> Result<u32> {
    ensure!(
        !matches!((left, right), (1082, 1114 | 1184) | (1114 | 1184, 1082)),
        "mixed DATE/timestamp USING requires explicit casts or an ON predicate"
    );
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
        matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184),
        "unsupported USING native type"
    );
    Ok(oid)
}
