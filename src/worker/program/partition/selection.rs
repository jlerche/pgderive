//! An excluded frame is a sequence of borrowed slices in window order.
use super::Ordered;
use crate::compiler::partition::Exclusion;
use std::ops::Range;

#[derive(Clone, Copy)]
pub(super) struct Selection<'a> {
    parts: [&'a [Ordered]; 3],
}

impl<'a> Selection<'a> {
    pub(super) const fn all(rows: &'a [Ordered]) -> Self {
        Self { parts: [rows, &[], &[]] }
    }

    pub(super) fn new(
        rows: &'a [Ordered],
        frame: &Range<usize>,
        peers: &Range<usize>,
        current: usize,
        exclusion: Exclusion,
    ) -> Self {
        let excluded = match exclusion {
            Exclusion::None => return Self::all(&rows[frame.clone()]),
            Exclusion::Current => current..current + 1,
            Exclusion::Group | Exclusion::Ties => peers.clone(),
        };
        let head = excluded.start.clamp(frame.start, frame.end);
        let tail = excluded.end.clamp(frame.start, frame.end);
        let kept = if exclusion == Exclusion::Ties && frame.contains(&current) {
            &rows[current..=current]
        } else {
            &[]
        };
        Self { parts: [&rows[frame.start..head], kept, &rows[tail..frame.end]] }
    }

    pub(super) fn len(self) -> usize {
        self.parts.iter().map(|part| part.len()).sum()
    }

    pub(super) fn iter(self) -> impl Iterator<Item = &'a Ordered> {
        self.parts.into_iter().flatten()
    }

    pub(super) fn get(self, mut index: usize) -> Option<&'a Ordered> {
        for part in self.parts {
            if let Some(row) = part.get(index) {
                return Some(row);
            }
            index -= part.len();
        }
        None
    }
}
