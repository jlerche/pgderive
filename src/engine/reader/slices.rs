//! Coarse immutable run extents. Fences remain in authenticated object-local trees.
use super::{
    BatchData, ObjectBatch, bounded,
    index::{Leaf, Node},
};
use anyhow::{Context as _, Result, ensure};
use std::{ops::Range, sync::Arc};

type Pages<K, V> = Arc<Vec<Leaf<(K, V)>>>;

#[derive(Clone, Debug)]
pub(super) struct Span<K, V> {
    pub(super) range: Range<usize>,
    pub(super) first: (K, V),
    pub(super) last: (K, V),
    pub(super) payload_bytes: u64,
    pub(super) pages: Option<Pages<K, V>>,
}
impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    pub(super) fn relative_block(&self, absolute: usize) -> usize {
        absolute
            .saturating_sub(self.span.as_ref().map_or(0, |span| span.range.start))
            .min(self.block_count())
    }
    pub(super) fn span_charge(&self) -> Result<usize> {
        let Some(span) = &self.span else {
            return Ok(0);
        };
        let first = bounded::encode(&span.first)?.len();
        let last = bounded::encode(&span.last)?.len();
        let pages =
            span.pages.as_deref().map(bounded::encode).transpose()?.map_or(0, |bytes| bytes.len());
        first
            .checked_add(last)
            .and_then(|bytes| bytes.checked_add(pages))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Span<K, V>>()))
            .context("extent metadata charge overflow")
    }
    pub(crate) async fn slice(&self, range: Range<usize>) -> Result<Self> {
        ensure!(range.start < range.end && range.end <= self.block_count(), "invalid run slice");
        if range.start == 0 && range.end == self.block_count() {
            return Ok(self.clone());
        }
        let start = self.span.as_ref().map_or(0, |span| span.range.start);
        let mut copy = self.clone();
        // A descriptor prepared for another extent must not pass owner validation.
        copy.index = Arc::new(self.index.as_ref().clone());
        copy.span = None;
        copy.install_span(start + range.start..start + range.end).await?;
        Ok(copy)
    }
    pub(super) async fn install_span(&mut self, range: Range<usize>) -> Result<()> {
        let root = self.index.node.as_ref().context("slice of empty object")?;
        ensure!(range.start < range.end, "empty run slice");
        ensure!(range.end <= root.blocks, "invalid run slice end");
        let mut stack = vec![(root.clone(), 0_usize)];
        let mut rows = 0_usize;
        let mut max_weight = 0_u64;
        let mut payload_bytes = 0_u64;
        while let Some((address, offset)) = stack.pop() {
            super::work::spend(0).await;
            let end = offset.checked_add(address.blocks).context("slice block overflow")?;
            if end <= range.start || offset >= range.end {
                continue;
            }
            if range.start <= offset && end <= range.end {
                payload_bytes = payload_bytes
                    .checked_add(address.payload_bytes)
                    .context("slice payload byte overflow")?;
                rows = rows.checked_add(address.rows).context("slice row overflow")?;
                max_weight = max_weight.max(address.max_weight);
                continue;
            }
            match self.node(&address).await?.as_ref() {
                Node::Branch(children) => {
                    let mut offset = offset;
                    for child in children {
                        stack.push((child.clone(), offset));
                        offset =
                            offset.checked_add(child.blocks).context("slice child overflow")?;
                    }
                }
                Node::Leaf(leaves) => {
                    for (_, leaf) in leaves
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| range.contains(&(offset + index)))
                    {
                        payload_bytes = payload_bytes
                            .checked_add(leaf.block.length)
                            .context("slice payload byte overflow")?;
                        rows = rows.checked_add(leaf.block.rows).context("slice row overflow")?;
                        max_weight = max_weight.max(leaf.block.max_weight);
                    }
                }
            }
        }
        let first = self.leaf(range.start).await?.block.first;
        let last = self.leaf(range.end - 1).await?.block.last;
        self.rows = rows;
        self.max_weight = max_weight;
        self.reference.blocks = Some(range.clone());
        self.span = Some(Arc::new(Span { range, first, last, payload_bytes, pages: None }));
        Ok(())
    }
    pub(crate) async fn prepare_extent(&mut self) -> Result<()> {
        if self.span.is_none() {
            return Ok(());
        }
        let pages = self.extent_pages(&(0..self.block_count())).await?;
        Arc::make_mut(self.span.as_mut().context("missing prepared extent")?).pages = pages;
        Ok(())
    }
    // Small selected extents carry the authenticated fences already read by
    // planning into execution. Large cold extents retain only coarse bounds.
    async fn extent_pages(&self, range: &Range<usize>) -> Result<Option<Pages<K, V>>> {
        if range.len() > super::index::FANOUT {
            return Ok(None);
        }
        let mut pages = Vec::new();
        let mut bytes = 2_usize;
        for ordinal in range.clone() {
            let leaf = self.leaf(ordinal).await?;
            bytes = bytes
                .checked_add(bounded::encode(&leaf)?.len())
                .and_then(|bytes| bytes.checked_add(usize::from(!pages.is_empty())))
                .context("extent fence charge overflow")?;
            if bytes > super::index::INLINE_BYTES {
                return Ok(None);
            }
            pages.push(leaf);
        }
        Ok(Some(Arc::new(pages)))
    }
    pub(crate) async fn key_blocks(&self, lower: &K, upper: &K) -> Result<Range<usize>> {
        if let Some((first, last)) = self.key_bounds() {
            if lower <= first && last <= upper {
                return Ok(0..self.block_count());
            }
            if upper < first || last < lower {
                return Ok(0..0);
            }
        }
        let start = self.seek_block(|last| last.0 < *lower).await?;
        let end = self.seek_block(|last| last.0 <= *upper).await?;
        // The block containing the upper fence belongs wholly to the merge.
        let end = if end < self.block_count() && self.leaf(end).await?.block.first.0 <= *upper {
            end + 1
        } else {
            end
        };
        Ok(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        Batch,
        reader::{BatchReader as _, BlockCache, Cursor, KeyIdentity},
    };
    use object_store::{ObjectStore, memory::InMemory};

    #[tokio::test]
    async fn nested_extents_preserve_signed_navigation_and_cold_reopen() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = Batch::from_updates((0..256_i64).map(|n| ((n / 80, n), weight(n))))?;
        let reference = ObjectBatch::write(store.clone(), &batch, "extent", 1).await?;
        let full = ObjectBatch::<i64, i64>::open(store.clone(), reference, "extent").await?;
        let outer = full.slice(30..220).await?;
        let inner = outer.slice(50..130).await?;
        assert_eq!(inner.reference.blocks, Some(80..160));
        assert_eq!(inner.row_count(), 80);
        assert_eq!(inner.max_weight(), 2);
        assert_eq!(inner.key_bounds(), Some((&1, &1)));
        let old_page = outer.key_page(&1).await?.context("missing outer page")?;
        assert!(inner.validate_page(&old_page).is_err());
        let cold = ObjectBatch::<i64, i64>::open_with_cache(
            store.clone(),
            inner.reference.clone(),
            "extent",
            Arc::new(BlockCache::new(1, 1)),
        )
        .await?;
        check_navigation(&inner).await?;
        check_navigation(&cold).await?;
        assert!(full.slice(4..4).await.is_err());
        assert!(full.slice(0..257).await.is_err());
        for (start, end) in [(0, 0), (256, 257), (10, 9)] {
            let mut reference = full.reference.clone();
            reference.blocks = Some(start..end);
            assert!(
                ObjectBatch::<i64, i64>::open(store.clone(), reference, "extent").await.is_err()
            );
        }
        let mut old = serde_json::to_value(full.reference())?;
        old.as_object_mut().context("reference object")?.remove("blocks");
        assert!(serde_json::from_value::<super::super::ObjectRef>(old).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn payload_summaries_match_encoded_ranges_and_nested_extents() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rows = Batch::from_updates((0..256_i64).map(|n| ((n, n.to_string().repeat(20)), 1)))?;
        let reference = ObjectBatch::write(store.clone(), &rows, "payload-bytes", 1).await?;
        let full =
            ObjectBatch::<i64, String>::open(store.clone(), reference, "payload-bytes").await?;
        let mut exact = 0;
        let mut selected = 0;
        for ordinal in 0..256 {
            let (_, range) = full.block_range(ordinal).await?.context("missing payload block")?;
            exact += range.end - range.start;
            if (80..160).contains(&ordinal) {
                selected += range.end - range.start;
            }
        }
        assert_eq!(full.payload_bytes(), exact);
        let outer = full.slice(30..220).await?;
        let inner = outer.slice(50..130).await?;
        assert_eq!(inner.payload_bytes(), selected);
        let clone = inner.clone();
        assert!(Arc::ptr_eq(
            clone.span.as_ref().context("span")?,
            inner.span.as_ref().context("span")?
        ));
        let reopened =
            ObjectBatch::<i64, String>::open(store, inner.reference().clone(), "payload-bytes")
                .await?;
        assert_eq!(reopened.payload_bytes(), selected);
        Ok(())
    }
    fn weight(n: i64) -> i64 {
        if n == 0 {
            i64::MAX
        } else if n % 2 == 0 {
            2
        } else {
            -1
        }
    }
    async fn check_navigation(object: &ObjectBatch<i64, i64>) -> Result<()> {
        let mut cursor = object.cursor().await?;
        for n in 80..160_i64 {
            assert_eq!(cursor.current(), Some((&1, &n, weight(n))));
            cursor.advance().await?;
        }
        assert!(cursor.current().is_none());
        check_weights(object).await?;
        check_key_navigation(object).await?;
        check_weight_pages(object).await?;
        Ok(())
    }
    async fn check_weights(object: &ObjectBatch<i64, i64>) -> Result<()> {
        for n in [0, 79, 80, 159, 160, 255] {
            assert_eq!(
                object.tuple_weight(&(n / 80, n)).await?,
                i64::from((80..160).contains(&n)) * weight(n)
            );
        }
        Ok(())
    }
    async fn check_weight_pages(object: &ObjectBatch<i64, i64>) -> Result<()> {
        let tuples = [(0, 0), (1, 80), (1, 159), (2, 160)];
        let refs = tuples.iter().collect::<Vec<_>>();
        let mut position = 0;
        let mut found = Vec::new();
        while let Some(page) = object.next_weight_page(&refs, &mut position).await? {
            found.extend(page.range);
        }
        assert_eq!(found, [1, 2]);
        Ok(())
    }
    async fn check_key_navigation(object: &ObjectBatch<i64, i64>) -> Result<()> {
        assert!(object.key_page(&0).await?.is_none());
        assert!(object.key_page(&2).await?.is_none());
        let page = object.key_page(&1).await?.context("missing inner page")?;
        let mut cursor =
            Cursor::object_key_page(object.clone(), &KeyIdentity::new(&1)?, page, None)
                .await?
                .context("missing key cursor")?;
        cursor.seek_key(&2).await?;
        assert!(cursor.current().is_none());
        Ok(())
    }
}
