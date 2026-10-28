//! Value identities are scoped to row occurrences, not physical table names.
use super::super::ColumnRef;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Identity {
    Source(usize, i16),
    Derived(String, String),
}
#[derive(Clone, Debug)]
pub(super) struct Column {
    pub reference: ColumnRef,
    pub modifier: i32,
    pub collation: u32,
    pub identity: Identity,
}
impl Column {
    pub fn derived(id: &str, reference: ColumnRef) -> Self {
        Self {
            identity: Identity::Derived(id.into(), reference.name.clone()),
            reference,
            modifier: -1,
            collation: 0,
        }
    }
    pub const fn compatible(&self, other: &Self) -> bool {
        self.reference.oid == other.reference.oid
            && self.modifier == other.modifier
            && self.collation == other.collation
            && matches!(self.reference.oid, 16 | 20 | 21 | 23 | 1700 | 2950 | 1114 | 1184)
    }
    pub const fn sink_compatible(&self) -> bool {
        self.collation == 0 && matches!(self.reference.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184)
    }
}
pub(super) type Key = BTreeSet<Identity>;
/// Conservative properties of complete committed SQL rows on one resolved edge.
/// Keys use NULLS-NOT-DISTINCT row identity; sink PKs additionally require non-NULL.
#[derive(Clone, Debug, Default)]
pub struct Properties {
    pub(super) columns: BTreeMap<String, Column>,
    pub(super) keys: BTreeSet<Key>,
    pub(super) navigation: Vec<String>,
    pub(super) maximum: Option<u64>,
}
impl Properties {
    pub(super) fn prune(&mut self) {
        let present = self.columns.values().map(|c| c.identity.clone()).collect::<Key>();
        self.keys.retain(|key| key.is_subset(&present));
        if self.navigation.iter().any(|name| !self.columns.contains_key(name)) {
            self.navigation.clear();
        }
    }
    pub(super) fn add_key(&mut self, key: Key) {
        if self.keys.iter().any(|prior| prior.is_subset(&key)) {
            return;
        }
        self.keys.retain(|prior| !key.is_subset(prior));
        // Bounded antichain. Dropping additional proofs only makes inference conservative.
        if self.keys.len() < 64 {
            self.keys.insert(key);
        }
    }
    pub(crate) fn unique_rows(&self) -> bool {
        !self.keys.is_empty()
    }
    pub(crate) const fn maximum_rows(&self) -> Option<u64> {
        self.maximum
    }
    pub(super) fn covers(&self, names: &[String]) -> bool {
        let identities = names
            .iter()
            .filter_map(|n| self.columns.get(n))
            .map(|c| c.identity.clone())
            .collect::<Key>();
        self.keys.iter().any(|key| key.is_subset(&identities))
    }
    pub(super) fn per_navigation(&self) -> Option<u64> {
        if self.covers(&self.navigation) { Some(1) } else { self.maximum }
    }
    pub(in crate::compiler) fn proves_key(&self, names: &[String]) -> bool {
        self.covers(names)
    }
    pub(super) fn non_null(&mut self, names: impl IntoIterator<Item = String>) {
        for name in names {
            if let Some(c) = self.columns.get_mut(&name) {
                c.reference.nullable = false;
            }
        }
    }
}
