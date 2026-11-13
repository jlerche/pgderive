use super::Model;
use crate::weighted::{Batch, Tuple};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;

// Compare the emitted batch with endpoint SQL snapshots, independently of CDC
// operations and the batch normalizer. Primary keys are fixture-only navigation.
pub(super) fn verify(batch: &Batch, schema: &str, before: &Model, after: &Model) -> Result<()> {
    let mut expected = BTreeMap::new();
    for (state, sign) in [(before, -1_i64), (after, 1_i64)] {
        for ((table, _), row) in state {
            let tuple = Tuple { schema: schema.to_owned(), table: table.clone(), row: row.clone() };
            *expected.entry(tuple).or_insert(0_i64) += sign;
        }
    }
    expected.retain(|_, weight| *weight != 0);
    let mut actual = BTreeMap::new();
    for update in &batch.updates {
        ensure!(update.weight != 0, "batch contains a zero weight");
        ensure!(
            actual.insert(update.tuple.clone(), update.weight).is_none(),
            "batch contains duplicate tuples"
        );
    }
    ensure!(actual == expected, "weighted batch differs from SQL snapshot difference");
    Ok(())
}
