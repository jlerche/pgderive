//! One set of resident exact identities with two ordered join insertion streams.
use super::{OrderedTerm, Resident};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;

pub(super) struct Entries<T> {
    tree: BTreeMap<T, Resident>,
    sorted: [Vec<(T, Resident)>; 2],
}

impl<T> Default for Entries<T> {
    fn default() -> Self {
        Self { tree: BTreeMap::new(), sorted: [Vec::new(), Vec::new()] }
    }
}

impl<T: Ord> Entries<T> {
    pub(super) fn get_mut(&mut self, key: &T, term: Option<OrderedTerm>) -> Option<&mut Resident> {
        if let Some(value) = self.tree.get_mut(key) {
            return Some(value);
        }
        // Each tagged stream emits strictly increasing full identities.
        // Earlier streams can still contain this identity.
        for (index, rows) in self.sorted.iter_mut().enumerate() {
            if term.is_some_and(|term| term.index() == index)
                || rows.last().is_none_or(|(last, _)| last < key)
                || rows.first().is_none_or(|(first, _)| first > key)
            {
                continue;
            }
            if let Ok(found) = rows.binary_search_by(|(other, _)| other.cmp(key)) {
                return Some(&mut rows[found].1);
            }
        }
        None
    }

    pub(super) fn insert(
        &mut self,
        key: T,
        record: Resident,
        term: Option<OrderedTerm>,
    ) -> Result<()> {
        if let Some(term) = term {
            let rows = &mut self.sorted[term.index()];
            ensure!(rows.last().is_none_or(|(last, _)| last < &key), "noncanonical join term");
            rows.push((key, record));
        } else {
            self.tree.insert(key, record);
        }
        Ok(())
    }

    pub(super) fn len(&self) -> usize {
        self.tree.len() + self.sorted[0].len() + self.sorted[1].len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn ordered(self) -> impl Iterator<Item = (T, Resident)> {
        let [prior, cross] = self.sorted;
        let sorted = !prior.is_empty() || !cross.is_empty();
        let mut tree = self.tree.into_iter().peekable();
        let mut prior = prior.into_iter().peekable();
        let mut cross = cross.into_iter().peekable();
        std::iter::from_fn(move || {
            if !sorted {
                return tree.next();
            }
            let tree_head = tree.peek().map(|(key, _)| key);
            let prior_head = prior.peek().map(|(key, _)| key);
            let cross_head = cross.peek().map(|(key, _)| key);
            match [(tree_head, 0), (prior_head, 1), (cross_head, 2)]
                .into_iter()
                .filter_map(|(key, source)| key.map(|key| (key, source)))
                .min()?
                .1
            {
                0 => tree.next(),
                1 => prior.next(),
                _ => cross.next(),
            }
        })
    }
}
