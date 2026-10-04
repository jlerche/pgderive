use crate::engine::{
    Batch, BatchBuilder,
    reader::{BatchData, BatchReader},
    trace::TraceSnapshot,
    weights::Accumulator,
};
use anyhow::{Result, ensure};
use std::collections::{BTreeMap, BTreeSet};

type Joined<K, L, R> = Batch<K, (L, R)>;

pub(super) async fn count<K: BatchData, L: BatchData, R: BatchData, G: BatchData>(
    joined: &Joined<K, L, R>,
    prior: &TraceSnapshot<G, i64>,
    group: impl Fn(&K, &L, &R) -> Result<Option<G>>,
) -> Result<Batch<G, i64>> {
    let mut totals = Accumulator::default();
    let mut affected = BTreeSet::new();
    for ((key, (left, right)), weight) in joined.iter() {
        if let Some(group) = group(key, left, right)? {
            affected.insert(group.clone());
            totals.add(group, *weight);
        }
    }
    let mut old = BTreeMap::new();
    let mut cursor = prior.cursor().await?;
    for key in &affected {
        cursor.seek_key(key).await?;
        if let Some((group, count, weight)) = cursor.current()
            && group == key
        {
            ensure!(weight == 1 && *count != 0, "invalid grouped count arrangement");
            old.insert(key.clone(), *count);
            totals.add(key.clone(), *count);
            cursor.advance().await?;
            ensure!(
                cursor.current().is_none_or(|(group, _, _)| group != key),
                "multiple count rows for group"
            );
        }
    }
    let next = totals.finish()?;
    let mut output = BatchBuilder::default();
    for key in affected {
        let before = old.get(&key).copied().unwrap_or_default();
        let after = next.get(&key).copied().unwrap_or_default();
        if before == after {
            continue;
        }
        if before != 0 {
            output.push(key.clone(), before, -1);
        }
        if after != 0 {
            output.push(key, after, 1);
        }
    }
    output.finish()
}
