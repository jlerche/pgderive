//! Resolve-to-read protection is one SQL transaction, never an expiring object lease.
use super::{
    checksum,
    manifest::{Catalog, Chunk, Manifest, chunk},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStoreExt as _, path::Path};
use std::{ops::Range, sync::Arc};

impl Manifest {
    pub(super) async fn read(
        &self,
        catalog: &Catalog,
        name: &str,
        range: Option<Range<u64>>,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<Option<(Chunk, Bytes)>> {
        for _attempt in 0..8 {
            if let Some(result) = self.read_pinned(catalog, name, range.clone(), store).await? {
                return Ok(Some(result));
            }
            if self.lookup(catalog, name).await?.is_none() {
                return Ok(None);
            }
        }
        anyhow::bail!("virtual object kept relocating while resolving read pin")
    }
    async fn read_pinned(
        &self,
        catalog: &Catalog,
        name: &str,
        range: Option<Range<u64>>,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<Option<(Chunk, Bytes)>> {
        let mut session = self.pool.acquire().await?;
        let tx = session.connection()?.transaction().await?;
        let schema = &self.config.schema;
        let row=tx.query_opt(&format!("SELECT c.name,c.namespace,c.pack::text,p.path,c.offset_bytes,c.length,c.checksum,c.created_micros,c.catalog::text FROM {schema}.chunks c JOIN {schema}.packs p ON p.id=c.pack AND p.tenant=c.tenant WHERE c.tenant=$1 AND c.catalog=$2::text::uuid AND c.name=$3 AND p.state='ready' FOR SHARE OF p"),&[&catalog.tenant,&catalog.catalog,&name]).await?;
        let Some(row) = row else { return Ok(None) };
        let chunk = chunk(&row)?;
        let range = range.unwrap_or(0..chunk.length);
        ensure!(
            range.end <= chunk.length && !range.is_empty(),
            "virtual object range out of bounds"
        );
        let physical = chunk.offset.checked_add(range.start).context("read offset overflow")?
            ..chunk.offset.checked_add(range.end).context("read end overflow")?;
        let bytes = store.get_range(&Path::from(chunk.path.clone()), physical).await?;
        ensure!(
            u64::try_from(bytes.len())? == range.end - range.start,
            "physical object returned wrong range length"
        );
        if range == (0..chunk.length) {
            ensure!(checksum(&bytes) == chunk.checksum, "virtual object checksum mismatch");
        }
        // A lost SQL session invalidates this read. Returning already-buffered
        // bytes is safe only after the pin transaction has confirmed completion.
        tx.commit().await.context("physical read pin lost before completion")?;
        drop(session);
        Ok(Some((chunk, bytes)))
    }
}
