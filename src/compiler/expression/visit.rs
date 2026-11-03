//! Column reads, substitution and strictness over bound predicates.
use super::{Expr, Predicate, typed::Replace};
use crate::compiler::{ColumnRef, syntax::Compare};
use std::collections::{BTreeMap, BTreeSet};

impl Expr {
    pub(in crate::compiler) fn reads(&self, out: &mut BTreeMap<String, ColumnRef>) {
        match self {
            Self::Value(value) | Self::Null(value, _) => value.reads(out),
            Self::Compare(_, left, right) => {
                left.reads(out);
                right.reads(out);
            }
            Self::And(values)
            | Self::Or(values)
            | Self::Quals(values)
            | Self::Array { values, .. } => values.iter().for_each(|value| value.reads(out)),
            Self::Not(value) => value.reads(out),
        }
    }
    pub(in crate::compiler) fn read_names(&self) -> BTreeSet<String> {
        let mut out = BTreeMap::new();
        self.reads(&mut out);
        out.into_keys().collect()
    }
    pub(in crate::compiler) fn replaced(&self, replace: &Replace<'_>) -> Option<Self> {
        let all = |values: &[Self]| -> Option<Vec<Self>> {
            values.iter().map(|value| value.replaced(replace)).collect()
        };
        Some(match self {
            Self::Value(value) => Self::Value(value.replaced(replace)?),
            Self::Null(value, not) => Self::Null(value.replaced(replace)?, *not),
            Self::Compare(op, left, right) => {
                Self::Compare(op.clone(), left.replaced(replace)?, right.replaced(replace)?)
            }
            Self::And(values) => Self::And(all(values)?),
            Self::Or(values) => Self::Or(all(values)?),
            Self::Quals(values) => Self::Quals(all(values)?),
            Self::Array { and, values } => Self::Array { and: *and, values: all(values)? },
            Self::Not(value) => Self::Not(Box::new(value.replaced(replace)?)),
        })
    }
    /// Columns whose NULL keeps this predicate from being true, as `find_nonnullable_rels` proves.
    pub(in crate::compiler) fn strict_columns(&self, top: bool) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        match self {
            Self::Value(value) => value.strict_columns(&mut out),
            Self::Null(value, true) if top => value.strict_columns(&mut out),
            Self::Null(..) => {}
            Self::Compare(_, left, right) => {
                left.strict_columns(&mut out);
                right.strict_columns(&mut out);
            }
            Self::And(values) | Self::Quals(values) if top => {
                out.extend(values.iter().flat_map(|value| value.strict_columns(true)));
            }
            Self::And(values)
            | Self::Quals(values)
            | Self::Or(values)
            | Self::Array { values, .. } => return common(values, top),
            Self::Not(value) => return value.strict_columns(false),
        }
        out
    }
    /// The strict columns of each arm of an OR.
    pub(in crate::compiler) fn strict_arms(&self) -> Option<Vec<BTreeSet<String>>> {
        let Self::Or(arms) = self else { return None };
        Some(arms.iter().map(|arm| arm.strict_columns(true)).collect())
    }
    pub(in crate::compiler) fn is_strict(&self) -> bool {
        match self {
            Self::Value(value) => value.is_strict(),
            Self::Compare(_, left, right) => left.is_strict() && right.is_strict(),
            _ => false,
        }
    }
    /// `extract_restriction_or_clauses`: the part of an OR that reads only `columns`.
    pub(in crate::compiler) fn or_restriction(&self, columns: &BTreeSet<String>) -> Option<Self> {
        let Self::Or(arms) = self else { return None };
        let restricted = arms.iter().map(|arm| {
            let clauses = match arm {
                Self::And(values) => values.as_slice(),
                value => std::slice::from_ref(value),
            };
            let local = clauses.iter().filter(|value| {
                let reads = value.read_names();
                !reads.is_empty() && reads.is_subset(columns)
            });
            let mut local = local.cloned().collect::<Vec<_>>();
            match local.len() {
                0 => None,
                1 => local.pop(),
                _ => Some(Self::And(local)),
            }
        });
        Some(Self::Or(restricted.collect::<Option<_>>()?))
    }
    /// The column and constant of a `column = constant` conjunct.
    pub(in crate::compiler) fn constant_equality(&self) -> Option<(&ColumnRef, &super::Scalar)> {
        let Self::Compare(Compare::Eq, left, right) = self else { return None };
        match (left.as_column(), right.as_column()) {
            (Some(column), None) if right.is_constant() => Some((column, right)),
            (None, Some(column)) if left.is_constant() => Some((column, left)),
            _ => None,
        }
    }
    pub(in crate::compiler) const fn equals(column: ColumnRef, constant: super::Scalar) -> Self {
        Self::Compare(Compare::Eq, super::Scalar::column(column), constant)
    }
}
fn common(values: &[Expr], top: bool) -> BTreeSet<String> {
    let mut sets = values.iter().map(|value| value.strict_columns(top));
    let first = sets.next().unwrap_or_default();
    sets.fold(first, |left, right| left.intersection(&right).cloned().collect())
}
impl Predicate {
    pub(in crate::compiler) fn conjuncts(self) -> Vec<Expr> {
        match self.0 {
            Expr::And(values) | Expr::Quals(values) => values,
            value => vec![value],
        }
    }
    pub(in crate::compiler) fn from_conjuncts(values: Vec<Expr>) -> Option<Self> {
        (!values.is_empty()).then(|| Self(Expr::And(values).ordered_quals()))
    }
    pub(in crate::compiler) fn strict_columns(&self) -> BTreeSet<String> {
        self.0.strict_columns(true)
    }
}
