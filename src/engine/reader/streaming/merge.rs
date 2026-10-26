//! Merge authenticated block fences before opening payloads. Strictly disjoint
//! full-identity blocks can be retained verbatim; overlapping identities still
//! pass through exact signed consolidation before narrowing.
use super::{WriteLimits, Writer};
use crate::engine::{
    reader::{BatchData, BlockCache, Cursor, Entry, ObjectBatch, index::Leaf},
    trace::Run,
};
use anyhow::{Context as _, Result};
use object_store::ObjectStore;
use std::sync::Arc;

struct Input<K: BatchData, V: BatchData> {
    object: Option<ObjectBatch<K, V>>,
    reusable: bool,
    ordinal: usize,
    leaf: Option<Leaf<(K, V)>>,
    cursor: Option<Cursor<K, V>>,
}
impl<K: BatchData, V: BatchData> Input<K, V> {
    async fn open(run: &Run<K, V>, store: &Arc<dyn ObjectStore>, schema: &str) -> Result<Self> {
        let mut input = match run {
            Run::Memory(batch) => Self {
                object: None,
                reusable: false,
                ordinal: 0,
                leaf: None,
                cursor: Some(Cursor::memory(batch.shared_entries())),
            },
            Run::Object(batch) => {
                let reusable =
                    Arc::ptr_eq(&batch.store, store) && batch.reference.schema() == schema;
                let mut batch = batch.clone();
                batch.cache = BlockCache::cursor(batch.cache.clone());
                Self { object: Some(batch), reusable, ordinal: 0, leaf: None, cursor: None }
            }
        };
        input.refresh().await?;
        Ok(input)
    }
    async fn refresh(&mut self) -> Result<()> {
        if let Some(object) = &self.object {
            self.leaf = if self.ordinal < object.block_count() {
                Some(object.leaf(self.ordinal).await?)
            } else {
                None
            };
        }
        Ok(())
    }
    fn first(&self) -> Option<(&K, &V)> {
        if let Some(cursor) = &self.cursor {
            return cursor.current().map(|(key, value, _)| (key, value));
        }
        self.leaf.as_ref().map(|leaf| (&leaf.block.first.0, &leaf.block.first.1))
    }
    fn last(&self) -> Option<(&K, &V)> {
        if let Some(object) = &self.object {
            return object.index.node.as_ref().map(|node| (&node.last.0, &node.last.1));
        }
        self.cursor.as_ref()?.current_span().last().map(|row| (&row.0.0, &row.0.1))
    }
    async fn load(&mut self, read_ahead: bool) -> Result<()> {
        if self.cursor.is_none()
            && let Some(object) = &self.object
        {
            let leaf = self.leaf.as_ref().context("missing loaded leaf")?;
            let rows = if read_ahead {
                object.read_leaf(leaf).await?
            } else {
                object.read_leaf_exact(leaf).await?
            };
            self.cursor = Some(Cursor::memory(rows));
        }
        Ok(())
    }
    async fn consume(&mut self, count: usize) -> Result<()> {
        let cursor = self.cursor.as_mut().context("missing compaction cursor")?;
        cursor.advance_rows(count).await?;
        if cursor.current().is_none() && self.object.is_some() {
            self.cursor = None;
            self.ordinal += 1;
            self.refresh().await?;
        }
        Ok(())
    }
    async fn retain(&mut self) -> Result<Leaf<(K, V)>> {
        let leaf = self.leaf.take().context("missing retained leaf")?;
        self.ordinal += 1;
        self.refresh().await?;
        Ok(leaf)
    }
}

impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    pub(crate) async fn compact_resident(
        store: Arc<dyn ObjectStore>,
        runs: &[Run<K, V>],
        schema: &str,
        target: (Option<&str>, WriteLimits),
        cache: Arc<BlockCache>,
    ) -> Result<Vec<Self>> {
        let mut writer = Writer::new(&store, schema, target, Some(cache))?;
        let mut inputs = Vec::new();
        for run in runs {
            inputs.push(Input::open(run, &store, schema).await?);
        }
        merge(&mut inputs, &mut writer).await?;
        Ok(writer.finish(&store).await?.1)
    }
}

async fn merge<K: BatchData, V: BatchData>(
    inputs: &mut [Input<K, V>],
    writer: &mut Writer<K, V>,
) -> Result<()> {
    let reuse_bytes = u64::try_from(super::TARGET_BLOCK_BYTES)?;
    loop {
        let Some(ordinal) = inputs
            .iter()
            .enumerate()
            .filter_map(|(index, input)| input.first().map(|tuple| (index, tuple)))
            .min_by_key(|(_, tuple)| *tuple)
            .map(|(index, _)| index)
        else {
            return Ok(());
        };
        let other = inputs
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != ordinal)
            .filter_map(|(_, input)| input.first())
            .min()
            .map(|(key, value)| (key.clone(), value.clone()));
        if inputs[ordinal].reusable
            && inputs[ordinal].cursor.is_none()
            && writer.limits.block_bytes == crate::engine::reader::bounded::MAX_BYTES
            && inputs[ordinal].leaf.as_ref().is_some_and(|leaf| {
                leaf.block.rows <= writer.limits.block_rows
                    // Repack tiny fragments instead of accumulating one leaf per
                    // short delta. Large payloads remain worth retaining even
                    // when a wide row forces a short block.
                    && (leaf.block.rows >= writer.limits.block_rows.min(crate::engine::reader::index::FANOUT)
                        || leaf.block.length >= reuse_bytes)
                    && other.as_ref().is_none_or(|other| leaf.block.last < *other)
            })
        {
            writer.reuse(inputs[ordinal].retain().await?).await?;
            continue;
        }
        let read_ahead = coalesces(inputs, ordinal);
        inputs[ordinal].load(read_ahead).await?;
        let span = inputs[ordinal].cursor.as_ref().context("missing loaded cursor")?.current_span();
        let length =
            other.as_ref().map_or(span.len(), |other| span.partition_point(|row| row.0 < *other));
        if length > 0 {
            for row in &span[..length] {
                writer.push(row.clone()).await?;
            }
            inputs[ordinal].consume(length).await?;
        } else {
            consolidate(inputs, ordinal, writer).await?;
        }
    }
}

fn coalesces<K: BatchData, V: BatchData>(inputs: &[Input<K, V>], ordinal: usize) -> bool {
    let input = &inputs[ordinal];
    if input.cursor.is_some() {
        return false;
    }
    input.leaf.as_ref().is_some_and(|leaf| {
        inputs.iter().enumerate().any(|(index, input)| {
            index != ordinal
                && input
                    .first()
                    .is_some_and(|first| first <= (&leaf.block.last.0, &leaf.block.last.1))
                && input.last().is_some_and(|last| last > (&leaf.block.last.0, &leaf.block.last.1))
        })
    })
}

async fn consolidate<K: BatchData, V: BatchData>(
    inputs: &mut [Input<K, V>],
    ordinal: usize,
    writer: &mut Writer<K, V>,
) -> Result<()> {
    let (key, value) = inputs[ordinal].first().context("missing compaction head")?;
    let tuple = (key.clone(), value.clone());
    let mut weight = num_bigint::BigInt::default();
    let loads = inputs
        .iter()
        .enumerate()
        .filter(|(_, input)| input.cursor.is_none() && input.first() == Some((&tuple.0, &tuple.1)))
        .map(|(index, _)| (index, coalesces(inputs, index)))
        .collect::<Vec<_>>();
    for (index, read_ahead) in loads {
        inputs[index].load(read_ahead).await?;
    }
    for input in inputs {
        if input.first() != Some((&tuple.0, &tuple.1)) {
            continue;
        }
        weight += input.cursor.as_ref().and_then(Cursor::current).context("missing loaded row")?.2;
        input.consume(1).await?;
    }
    if weight != num_bigint::BigInt::default() {
        let weight =
            i64::try_from(weight).map_err(|_| crate::engine::trace::CoefficientOverflow)?;
        let row: Entry<K, V> = (tuple, weight);
        writer.push(row).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/compaction.rs"]
mod tests;
