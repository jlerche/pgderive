//! A frame can omit one physical occurrence without copying or renumbering its rows.
use super::Ordered;

#[derive(Clone, Copy)]
pub(super) struct Selection<'a> {
    before: &'a [Ordered],
    after: &'a [Ordered],
}

impl<'a> Selection<'a> {
    pub(super) fn new(rows: &'a [Ordered], exclude: Option<usize>) -> Self {
        let Some(index) = exclude else {
            return Self { before: rows, after: &[] };
        };
        let (before, excluded_and_after) = rows.split_at(index);
        Self { before, after: &excluded_and_after[1..] }
    }

    pub(super) const fn len(self) -> usize {
        self.before.len() + self.after.len()
    }

    pub(super) fn iter(self) -> impl Iterator<Item = &'a Ordered> {
        self.before.iter().chain(self.after)
    }

    pub(super) fn get(self, index: usize) -> Option<&'a Ordered> {
        if index < self.before.len() {
            self.before.get(index)
        } else {
            self.after.get(index - self.before.len())
        }
    }
}
