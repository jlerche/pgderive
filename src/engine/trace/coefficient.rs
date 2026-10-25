//! Lazy full-tuple validation jobs across every immutable reader occurrence.
use super::Run;
use crate::engine::reader::{BatchData, ObjectBatch};
use anyhow::Result;
use std::ops::Range;

pub(super) struct Page<K: BatchData, V: BatchData> {
    pub(super) batch: ObjectBatch<K, V>,
    pub(super) ordinal: usize,
    pub(super) range: Range<usize>,
}
pub(super) struct Pages<'a, K: BatchData, V: BatchData> {
    runs: &'a [Run<K, V>],
    tuples: &'a [&'a (K, V)],
    run: usize,
    position: usize,
}
impl<'a, K: BatchData, V: BatchData> Pages<'a, K, V> {
    pub(super) const fn new(runs: &'a [Run<K, V>], tuples: &'a [&'a (K, V)]) -> Self {
        Self { runs, tuples, run: 0, position: 0 }
    }
}
impl<K: BatchData, V: BatchData> Iterator for Pages<'_, K, V> {
    type Item = Result<Page<K, V>>;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(run) = self.runs.get(self.run) {
            if let Run::Object(batch) = run {
                match batch.next_weight_page(self.tuples, &mut self.position) {
                    Ok(Some(page)) => {
                        return Some(Ok(Page {
                            batch: batch.clone(),
                            ordinal: page.ordinal,
                            range: page.range,
                        }));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        self.run = self.runs.len();
                        return Some(Err(error));
                    }
                }
            }
            self.run += 1;
            self.position = 0;
        }
        None
    }
}
