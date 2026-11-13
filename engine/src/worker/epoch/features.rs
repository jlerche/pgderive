//! Bounded cost-only key-image accounting; never a relational uniqueness proof.
use crate::{
    engine::execution::Limits,
    source::Contract,
    transaction::{Change, Transaction},
};
use anyhow::Result;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(in crate::worker) struct Input {
    pub rows: u64,
    pub bytes: u64,
    pub keys: u64,
}
impl Input {
    pub(super) fn values(self) -> [f64; 3] {
        [self.rows, self.bytes, self.keys].map(|n| f64::from(u32::try_from(n).unwrap_or(u32::MAX)))
    }
    pub(super) const fn plus(self, other: Self) -> Self {
        Self {
            rows: self.rows.saturating_add(other.rows),
            bytes: self.bytes.saturating_add(other.bytes),
            keys: self.keys.saturating_add(other.keys),
        }
    }
    pub(super) fn average(self, n: u64) -> Self {
        Self {
            rows: self.rows.div_ceil(n.max(1)),
            bytes: self.bytes.div_ceil(n.max(1)),
            keys: self.keys.div_ceil(n.max(1)),
        }
    }
}
pub(in crate::worker) struct Keys {
    relations: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    limit: usize,
}
impl Keys {
    pub(in crate::worker) fn new(contract: &Contract, limits: Limits) -> Result<Arc<Self>> {
        contract.validate()?;
        let mut relations = BTreeMap::<String, BTreeMap<String, Vec<String>>>::new();
        for relation in &contract.relations {
            relations.entry(relation.schema.clone()).or_default().insert(
                relation.table.clone(),
                relation.columns.iter().filter(|c| c.primary).map(|c| c.name.clone()).collect(),
            );
        }
        Ok(Arc::new(Self {
            relations,
            limit: limits.resident_entries.min(limits.resident_bytes / 128),
        }))
    }
    fn image(&self, change: &Change, row: &crate::transaction::Row) -> Option<[u8; 32]> {
        let fields = self.relations.get(&change.schema)?.get(&change.table)?;
        let mut digest = Sha256::new();
        for value in [change.schema.as_str(), change.table.as_str()] {
            digest.update(value.len().to_le_bytes());
            digest.update(value.as_bytes());
        }
        for field in fields {
            let value = row.get(field)?.as_deref()?;
            digest.update(value.len().to_le_bytes());
            digest.update(value.as_bytes());
        }
        Some(digest.finalize().into())
    }
}
pub(super) struct Count {
    plan: Arc<Keys>,
    distinct: BTreeSet<[u8; 32]>,
    images: u64,
    saturated: bool,
}
impl Count {
    pub(super) const fn new(plan: Arc<Keys>) -> Self {
        Self { plan, distinct: BTreeSet::new(), images: 0, saturated: false }
    }
    pub(super) fn append(&mut self, transaction: &Transaction) {
        for change in &transaction.changes {
            for row in change.old.iter().chain(&change.new) {
                self.images = self.images.saturating_add(1);
                if self.saturated {
                    continue;
                }
                let image = self.plan.image(change, row);
                if image.is_none()
                    || (self.distinct.len() >= self.plan.limit
                        && image.is_some_and(|key| !self.distinct.contains(&key)))
                {
                    self.saturated = true;
                    self.distinct.clear();
                } else if let Some(key) = image {
                    self.distinct.insert(key);
                }
            }
        }
    }
    pub(super) fn keys(&self) -> u64 {
        if self.saturated {
            self.images
        } else {
            u64::try_from(self.distinct.len()).unwrap_or(u64::MAX)
        }
    }
}
