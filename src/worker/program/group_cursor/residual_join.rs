//! Differentiate residual and FULL joins from the delta: changed pairs plus match transitions.
use super::{Rows, is_left, split, unmatched};
use crate::{
    compiler::left_join::Spec,
    engine::{
        dataflow::{Group, GroupCursor, GroupInput, PartitionWork},
        execution::{CapacityExceeded, Consolidator},
    },
    transaction::Row,
    worker::program::{
        left_join::{matching, null_extend},
        relational::Key,
    },
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::collections::BTreeMap;

const OLD: usize = 0;
const NEW: usize = 1;
const ROW_OVERHEAD: usize = 128;

#[derive(Clone, Copy)]
struct Change {
    weight: [i64; 2],
    matched: [bool; 2],
}
impl Change {
    const fn pending(self, state: usize) -> bool {
        self.weight[state] > 0 && !self.matched[state]
    }
}
#[derive(Default)]
struct Changes {
    rows: BTreeMap<Row, Change>,
    bytes: u64,
}
impl Changes {
    fn seeded(rows: &Rows<'_>, limit: u64) -> Result<Self> {
        let mut changes = Self::default();
        for (row, delta) in rows {
            changes.insert(row, Change { weight: [0, *delta], matched: [false, false] }, limit)?;
        }
        Ok(changes)
    }
    fn insert(&mut self, row: &Row, change: Change, limit: u64) -> Result<()> {
        let size = ROW_OVERHEAD
            + row
                .iter()
                .map(|(name, value)| name.len() + value.as_ref().map_or(0, String::len))
                .sum::<usize>();
        self.bytes = self.bytes.saturating_add(u64::try_from(size)?);
        ensure!(self.bytes <= limit, CapacityExceeded("residual join affected rows exceed limit"));
        self.rows.insert(row.clone(), change);
        Ok(())
    }
    fn retained(&mut self, row: &Row, weight: i64) -> Result<()> {
        if let Some(change) = self.rows.get_mut(row) {
            change.weight[OLD] = weight;
            change.weight[NEW] =
                change.weight[NEW].checked_add(weight).context("join row weight")?;
        }
        Ok(())
    }
    fn flag(&mut self, row: &Row, weight: i64, state: usize, limit: u64) -> Result<()> {
        if let Some(change) = self.rows.get_mut(row) {
            change.matched[state] = true;
            return Ok(());
        }
        let mut change = Change { weight: [weight, weight], matched: [false, false] };
        change.matched[state] = true;
        self.insert(row, change, limit)
    }
}

struct Scan<'a> {
    spec: &'a Spec,
    left: Rows<'a>,
    right: Rows<'a>,
    lefts: Changes,
    rights: Changes,
    limit: u64,
    output: Consolidator<((), Row)>,
}

pub(super) fn operator(spec: &Spec) -> Group<Key, Row> {
    let spec = spec.clone();
    Group::new(move |input, work| {
        let spec = spec.clone();
        Box::pin(async move {
            let (left, right) = split(input.delta)?;
            let limit = input.limits.output_bytes;
            let lefts = Changes::seeded(&left, limit)?;
            let rights = Changes::seeded(&right, limit)?;
            let output = Consolidator::new(input.limits)?;
            let mut scan = Scan { spec: &spec, left, right, lefts, rights, limit, output };
            scan.before(input.before().await?, work).await?;
            scan.after(input.after().await?, work).await?;
            scan.changed_pairs(work)?;
            scan.settle(&input, work).await?;
            scan.finish()
        })
    })
}

impl Scan<'_> {
    async fn before(
        &mut self,
        mut cursor: GroupCursor<Key, Row>,
        work: &mut PartitionWork,
    ) -> Result<()> {
        while let Some((row, weight)) = cursor.next(work).await? {
            if !is_left(&row)? {
                self.rights.retained(&row, weight)?;
            } else if self.lefts.rows.contains_key(&row) {
                self.lefts.retained(&row, weight)?;
            } else {
                self.unchanged_left(&row, weight, work)?;
            }
        }
        Ok(())
    }
    fn unchanged_left(&mut self, left: &Row, weight: i64, work: &mut PartitionWork) -> Result<()> {
        let changes = std::mem::take(&mut self.right);
        for (right, delta) in &changes {
            work.charge(1)?;
            let Some(result) = matching(self.spec, left, right)? else { continue };
            self.output.add(((), result), BigInt::from(weight) * *delta)?;
            let state = if *delta > 0 { NEW } else { OLD };
            self.lefts.flag(left, weight, state, self.limit)?;
            self.rights.flag(right, 0, state, self.limit)?;
        }
        self.right = changes;
        Ok(())
    }
    async fn after(
        &mut self,
        mut cursor: GroupCursor<Key, Row>,
        work: &mut PartitionWork,
    ) -> Result<()> {
        while let Some((row, weight)) = cursor.next(work).await? {
            if !is_left(&row)? && !self.rights.rows.contains_key(&row) {
                self.unchanged_right(&row, weight, work)?;
            }
        }
        Ok(())
    }
    fn unchanged_right(
        &mut self,
        right: &Row,
        weight: i64,
        work: &mut PartitionWork,
    ) -> Result<()> {
        let changes = std::mem::take(&mut self.left);
        for (left, delta) in &changes {
            work.charge(1)?;
            let Some(result) = matching(self.spec, left, right)? else { continue };
            self.output.add(((), result), BigInt::from(weight) * *delta)?;
            let state = if *delta > 0 { NEW } else { OLD };
            self.lefts.flag(left, 0, state, self.limit)?;
            if self.spec.full {
                self.rights.flag(right, weight, state, self.limit)?;
            }
        }
        self.left = changes;
        Ok(())
    }
    fn changed_pairs(&mut self, work: &mut PartitionWork) -> Result<()> {
        let (lefts, rights) = (std::mem::take(&mut self.left), std::mem::take(&mut self.right));
        for (left, _) in &lefts {
            for (right, _) in &rights {
                work.charge(1)?;
                self.changed_pair(left, right)?;
            }
        }
        Ok(())
    }
    fn changed_pair(&mut self, left: &Row, right: &Row) -> Result<()> {
        let weight = |changes: &Changes, row: &Row| -> Result<[i64; 2]> {
            Ok(changes.rows.get(row).context("missing changed join row")?.weight)
        };
        let (a, b) = (weight(&self.lefts, left)?, weight(&self.rights, right)?);
        let exists = [a[OLD] > 0 && b[OLD] > 0, a[NEW] > 0 && b[NEW] > 0];
        if exists == [false, false] {
            return Ok(());
        }
        let Some(result) = matching(self.spec, left, right)? else { return Ok(()) };
        let new = BigInt::from(a[NEW]) * b[NEW];
        self.output.add(((), result), new - BigInt::from(a[OLD]) * b[OLD])?;
        for state in [OLD, NEW].into_iter().filter(|state| exists[*state]) {
            self.lefts.flag(left, 0, state, self.limit)?;
            self.rights.flag(right, 0, state, self.limit)?;
        }
        Ok(())
    }
    async fn settle(
        &mut self,
        input: &GroupInput<'_, Key, Row>,
        work: &mut PartitionWork,
    ) -> Result<()> {
        self.resolve(input.before().await?, OLD, work).await?;
        self.resolve(input.after().await?, NEW, work).await
    }
    async fn resolve(
        &mut self,
        mut cursor: GroupCursor<Key, Row>,
        state: usize,
        work: &mut PartitionWork,
    ) -> Result<()> {
        let mut lefts = pending(&self.lefts, state);
        let mut rights = if self.spec.full { pending(&self.rights, state) } else { Vec::new() };
        while !(lefts.is_empty() && rights.is_empty()) {
            let Some((row, _)) = cursor.next(work).await? else { break };
            let left = is_left(&row)?;
            let candidates = if left { &mut rights } else { &mut lefts };
            let changes = if left { &mut self.rights } else { &mut self.lefts };
            for candidate in std::mem::take(candidates) {
                work.charge(1)?;
                let (a, b) = if left { (&row, &candidate) } else { (&candidate, &row) };
                if matching(self.spec, a, b)?.is_some() {
                    changes.flag(&candidate, 0, state, self.limit)?;
                } else {
                    candidates.push(candidate);
                }
            }
        }
        Ok(())
    }
    fn finish(mut self) -> Result<crate::engine::Batch<(), Row>> {
        for (sign, state) in [(-1, OLD), (1, NEW)] {
            for (row, change) in &self.lefts.rows {
                if change.pending(state) {
                    let result = unmatched(self.spec, row)?;
                    self.output.add(((), result), BigInt::from(change.weight[state]) * sign)?;
                }
            }
            if !self.spec.full {
                continue;
            }
            for (row, change) in &self.rights.rows {
                if change.pending(state) {
                    let result = null_extend(&self.spec.left, row)?;
                    self.output.add(((), result), BigInt::from(change.weight[state]) * sign)?;
                }
            }
        }
        self.output.finish_batch()
    }
}

fn pending(changes: &Changes, state: usize) -> Vec<Row> {
    changes
        .rows
        .iter()
        .filter(|(_, change)| change.pending(state))
        .map(|(row, _)| row.clone())
        .collect()
}
