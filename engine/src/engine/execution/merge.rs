use super::record;
use crate::engine::reader::BatchData;
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::{cmp::Reverse, collections::BinaryHeap, fs::File, io::BufReader};

struct Run {
    reader: BufReader<File>,
    weight: BigInt,
    bytes: u64,
}
pub(super) struct Merge<T: BatchData> {
    runs: Vec<Run>,
    heap: BinaryHeap<Reverse<(T, usize)>>,
    limit: usize,
    record_buffer: Vec<u8>,
}
impl<T: BatchData> Merge<T> {
    pub(super) fn open(paths: &[record::Run], limit: usize) -> Result<Self> {
        let mut merge =
            Self { runs: Vec::new(), heap: BinaryHeap::new(), limit, record_buffer: Vec::new() };
        for path in paths {
            merge.runs.push(Run {
                reader: BufReader::new(File::open(&path.path)?),
                weight: BigInt::default(),
                bytes: path.bytes,
            });
            merge.advance(merge.runs.len() - 1, None)?;
        }
        Ok(merge)
    }
    fn advance(&mut self, ordinal: usize, previous: Option<&T>) -> Result<()> {
        let run = &mut self.runs[ordinal];
        if let Some((tuple, weight)) = record::read_complete::<T>(
            &mut run.reader,
            self.limit,
            run.bytes,
            &mut self.record_buffer,
        )? {
            ensure!(previous.is_none_or(|previous| previous < &tuple), "noncanonical scratch run");
            run.weight = weight;
            self.heap.push(Reverse((tuple, ordinal)));
        }
        Ok(())
    }
    pub(super) fn next(&mut self) -> Result<Option<(T, BigInt)>> {
        while let Some(Reverse((tuple, ordinal))) = self.heap.pop() {
            let mut weight = std::mem::take(&mut self.runs[ordinal].weight);
            self.advance(ordinal, Some(&tuple))?;
            while self.heap.peek().is_some_and(|Reverse((other, _))| other == &tuple) {
                if let Some(Reverse((_, ordinal))) = self.heap.pop() {
                    weight += &self.runs[ordinal].weight;
                    self.advance(ordinal, Some(&tuple))?;
                }
            }
            if weight != BigInt::default() {
                return Ok(Some((tuple, weight)));
            }
        }
        Ok(None)
    }
}
