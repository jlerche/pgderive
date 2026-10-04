use super::record;
use crate::engine::reader::BatchData;
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::{cmp::Reverse, collections::BinaryHeap, fs::File, io::BufReader, path::PathBuf};

struct Run<T> {
    reader: BufReader<File>,
    weight: BigInt,
    previous: Option<T>,
}
pub(super) struct Merge<T: BatchData> {
    runs: Vec<Run<T>>,
    heap: BinaryHeap<Reverse<(T, usize)>>,
    limit: usize,
}
impl<T: BatchData> Merge<T> {
    pub(super) fn open(paths: &[PathBuf], limit: usize) -> Result<Self> {
        let mut merge = Self { runs: Vec::new(), heap: BinaryHeap::new(), limit };
        for path in paths {
            merge.runs.push(Run {
                reader: BufReader::new(File::open(path)?),
                weight: BigInt::default(),
                previous: None,
            });
            merge.advance(merge.runs.len() - 1)?;
        }
        Ok(merge)
    }
    fn advance(&mut self, ordinal: usize) -> Result<()> {
        let run = &mut self.runs[ordinal];
        if let Some((tuple, weight)) = record::read::<T>(&mut run.reader, self.limit)? {
            ensure!(
                run.previous.as_ref().is_none_or(|previous| previous < &tuple),
                "noncanonical scratch run"
            );
            run.previous = Some(tuple.clone());
            run.weight = weight;
            self.heap.push(Reverse((tuple, ordinal)));
        }
        Ok(())
    }
    pub(super) fn next(&mut self) -> Result<Option<(T, BigInt)>> {
        while let Some(Reverse((tuple, ordinal))) = self.heap.pop() {
            let mut weight = self.runs[ordinal].weight.clone();
            self.advance(ordinal)?;
            while self.heap.peek().is_some_and(|Reverse((other, _))| other == &tuple) {
                if let Some(Reverse((_, ordinal))) = self.heap.pop() {
                    weight += &self.runs[ordinal].weight;
                    self.advance(ordinal)?;
                }
            }
            if weight != BigInt::default() {
                return Ok(Some((tuple, weight)));
            }
        }
        Ok(None)
    }
}
