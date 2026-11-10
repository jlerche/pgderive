//! Tenant physical reclamation never consults a source database.
use super::{
    checksum,
    manifest::{Chunk, Pack, chunk},
    upload::{Owner, lock},
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, path::Path};
use std::sync::Arc;

impl Owner {
    pub(super) async fn collect(
        &mut self,
        tenant: &str,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<bool> {
        ensure!(tenant == self.tenant, "cross-tenant collection rejected");
        let scope = format!("pgderive-tenant-compaction:{tenant}");
        let acquired: bool = self
            .sql
            .client
            .query_one("SELECT pg_try_advisory_lock(hashtextextended($1,0))", &[&scope])
            .await?
            .try_get(0)?;
        if !acquired {
            return Ok(false);
        }
        let result = self.collect_owned(tenant, store).await;
        self.sql
            .client
            .query_one("SELECT pg_advisory_unlock(hashtextextended($1,0))", &[&scope])
            .await?;
        result
    }
    async fn collect_owned(
        &mut self,
        tenant: &str,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<bool> {
        let preparing = async {
            self.fence(tenant).await?;
            let retired = self.retire_empty(tenant).await?;
            if let Some(pack) = self.fragmented(tenant).await? {
                self.relocate(tenant, &pack, store).await?;
                return Ok(true);
            }
            Ok(retired)
        }
        .await;
        // Reader pins or failed relocation must not starve independent physical
        // deletion and late-PUT cleanup. Keep all failures for diagnostics.
        let deleting = self.delete_retired(tenant, store).await;
        let inventory = self.inventory(tenant, store).await;
        let deleted = deleting.as_ref().copied().unwrap_or(false);
        let result = crate::outcome::combine(preparing, deleting.map(|_| ()), "retired pack sweep");
        crate::outcome::combine(result, inventory, "physical inventory sweep")
            .map(|progress| progress || deleted || self.inventory_cursor.is_some())
    }
    async fn fence(&mut self, tenant: &str) -> Result<()> {
        let schema = &self.manifest.config.schema;
        let candidates=self.sql.client.query(&format!("SELECT id::text FROM {schema}.owners o WHERE tenant=$1 AND active AND NOT EXISTS(SELECT FROM pg_locks l WHERE l.locktype='advisory' AND l.granted AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.objsubid=1 AND l.classid=((hashtextextended('pgderive-storage-owner:'||o.id::text,0)>>32)&4294967295)::oid AND l.objid=(hashtextextended('pgderive-storage-owner:'||o.id::text,0)&4294967295)::oid) ORDER BY id LIMIT 32"),&[&tenant]).await?;
        for row in candidates {
            let owner: String = row.try_get(0)?;
            let tx = self.sql.client.transaction().await?;
            let fenced: bool = tx
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    &[&lock(&owner)],
                )
                .await?
                .try_get(0)?;
            if fenced {
                tx.execute(
                    &format!("UPDATE {schema}.owners SET active=false WHERE id=$1::text::uuid"),
                    &[&owner],
                )
                .await?;
            }
            tx.commit().await?;
        }
        self.sql.client.execute(&format!("UPDATE {schema}.packs p SET state='retired' FROM {schema}.owners o WHERE p.owner=o.id AND p.tenant=$1 AND p.state='writing' AND NOT o.active"),&[&tenant]).await?;
        Ok(())
    }
    async fn retire_empty(&self, tenant: &str) -> Result<bool> {
        let schema = &self.manifest.config.schema;
        let retired=self.sql.client.execute(&format!("WITH candidates AS (SELECT p.id FROM {schema}.packs p WHERE p.tenant=$1 AND p.state='ready' AND NOT EXISTS(SELECT FROM {schema}.chunks c WHERE c.pack=p.id) ORDER BY p.id LIMIT 32 FOR UPDATE OF p SKIP LOCKED) UPDATE {schema}.packs p SET state='retired' FROM candidates c WHERE p.id=c.id"),&[&tenant]).await?;
        Ok(retired > 0)
    }
    async fn fragmented(&self, tenant: &str) -> Result<Option<Pack>> {
        let schema = &self.manifest.config.schema;
        let row=self.sql.client.query_opt(&format!("SELECT p.id::text,p.owner::text,p.path,p.length,p.checksum FROM {schema}.packs p JOIN {schema}.chunks c ON c.pack=p.id WHERE p.tenant=$1 AND p.state='ready' GROUP BY p.id HAVING SUM(c.length)>0 AND SUM(c.length)*2<p.length ORDER BY p.id LIMIT 1"),&[&tenant]).await?;
        row.as_ref().map(pack).transpose()
    }
    async fn relocate(
        &mut self,
        tenant: &str,
        old: &Pack,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<()> {
        let chunks = self.live_chunks(tenant, &old.id).await?;
        if chunks.is_empty() {
            return Ok(());
        }
        ensure!(old.length <= 8 * 1024 * 1024 + 65_536, "physical pack exceeds read budget");
        let path = Path::from(old.path.clone());
        ensure!(store.head(&path).await?.size == old.length, "physical pack size changed");
        let original = store.get_range(&path, 0..old.length).await?;
        ensure!(
            u64::try_from(original.len())? == old.length && checksum(&original) == old.checksum,
            "physical compaction pack corruption"
        );
        let mut bytes = Vec::new();
        for chunk in &chunks {
            let start = usize::try_from(chunk.offset)?;
            let end = start
                .checked_add(usize::try_from(chunk.length)?)
                .context("relocation range overflow")?;
            let slice = original.get(start..end).context("relocation exceeds pack")?;
            ensure!(checksum(slice) == chunk.checksum, "relocation chunk checksum mismatch");
            bytes.extend_from_slice(slice);
        }
        let replacement = self.reserve(tenant, &bytes).await?;
        store
            .put_opts(
                &Path::from(replacement.path.clone()),
                bytes.into(),
                PutOptions::from(PutMode::Create),
            )
            .await?;
        self.remap(tenant, old, &replacement, &chunks).await
    }
    async fn live_chunks(&self, tenant: &str, id: &str) -> Result<Vec<Chunk>> {
        let schema = &self.manifest.config.schema;
        let rows=self.sql.client.query(&format!("SELECT c.name,c.namespace,c.pack::text,p.path,c.offset_bytes,c.length,c.checksum,c.created_micros,c.catalog::text FROM {schema}.chunks c JOIN {schema}.packs p ON p.id=c.pack WHERE c.tenant=$1 AND c.pack=$2::text::uuid ORDER BY c.catalog,c.name"),&[&tenant,&id]).await?;
        rows.iter().map(chunk).collect()
    }
    async fn remap(
        &mut self,
        tenant: &str,
        old: &Pack,
        new: &Pack,
        chunks: &[Chunk],
    ) -> Result<()> {
        let schema = &self.manifest.config.schema;
        let tx = self.sql.client.transaction().await?;
        // Exclusive source-pack lock waits for buffered readers to finish. No
        // logical mapping can subsequently be installed against a ready pack.
        let state: String = tx
            .query_one(
                &format!("SELECT state FROM {schema}.packs WHERE id=$1::text::uuid FOR UPDATE"),
                &[&old.id],
            )
            .await?
            .try_get(0)?;
        ensure!(state == "ready", "relocation source pack changed");
        let active: bool = tx
            .query_one(
                &format!("SELECT active FROM {schema}.owners WHERE id=$1::text::uuid FOR SHARE"),
                &[&self.id],
            )
            .await?
            .try_get(0)?;
        ensure!(active, "relocation uploader fenced");
        let mut offset = 0_i64;
        for chunk in chunks {
            tx.execute(&format!("UPDATE {schema}.chunks SET pack=$1::text::uuid,offset_bytes=$2 WHERE tenant=$3 AND catalog=$4::text::uuid AND name=$5 AND pack=$6::text::uuid AND checksum=$7"),&[&new.id,&offset,&tenant,&chunk.catalog,&chunk.name,&old.id,&chunk.checksum]).await?;
            offset = offset
                .checked_add(i64::try_from(chunk.length)?)
                .context("relocation offset overflow")?;
        }
        ensure!(
            tx.query_one(
                &format!(
                    "SELECT NOT EXISTS(SELECT FROM {schema}.chunks WHERE pack=$1::text::uuid)"
                ),
                &[&old.id]
            )
            .await?
            .try_get::<_, bool>(0)?,
            "relocation left uncopied mappings"
        );
        let installed=tx.execute(&format!("UPDATE {schema}.packs SET state='ready' WHERE id=$1::text::uuid AND state='writing' AND owner=$2::text::uuid"),&[&new.id,&self.id]).await?;
        ensure!(installed == 1, "relocation replacement intent changed");
        tx.execute(
            &format!("UPDATE {schema}.packs SET state='retired' WHERE id=$1::text::uuid"),
            &[&old.id],
        )
        .await?;
        tx.commit().await.context("relocation COMMIT uncertain; keep both physical packs")?;
        Ok(())
    }
    async fn inventory(
        &mut self,
        tenant: &str,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<()> {
        if tokio::time::Instant::now() < self.inventory_due {
            return Ok(());
        }
        let page = store.page("owners/", self.inventory_cursor.clone(), 256).await?;
        for object in page.result.objects {
            let path = object.location.to_string();
            let Some(owner) = path.strip_prefix("owners/").and_then(|p| p.split('/').next()) else {
                continue;
            };
            if !owner.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-') || owner.len() != 36 {
                continue;
            }
            let schema = &self.manifest.config.schema;
            let row=self.sql.client.query_opt(&format!("SELECT o.active,p.state FROM {schema}.owners o LEFT JOIN {schema}.packs p ON p.owner=o.id AND p.path=$3 WHERE o.tenant=$1 AND o.id=$2::text::uuid"),&[&tenant,&owner,&path]).await?;
            if let Some(row) = row {
                let active: bool = row.try_get(0)?;
                let state: Option<String> = row.try_get(1)?;
                if state.as_deref() == Some("deleted") || (!active && state.is_none()) {
                    store.delete(&object.location).await?;
                }
            }
        }
        self.inventory_cursor = page.page_token;
        if self.inventory_cursor.is_none() {
            self.inventory_due = tokio::time::Instant::now() + std::time::Duration::from_mins(5);
        }
        let schema = &self.manifest.config.schema;
        self.sql.client.execute(&format!("DELETE FROM {schema}.packs p USING {schema}.owners o WHERE p.owner=o.id AND p.tenant=$1 AND p.state='deleted' AND NOT o.active"),&[&tenant]).await?;
        Ok(())
    }
    async fn delete_retired(
        &self,
        tenant: &str,
        store: &Arc<dyn super::super::PageStore>,
    ) -> Result<bool> {
        let schema = &self.manifest.config.schema;
        let rows=self.sql.client.query(&format!("SELECT id::text,path FROM {schema}.packs WHERE tenant=$1 AND state='retired' ORDER BY id LIMIT 16"),&[&tenant]).await?;
        if rows.is_empty() {
            return Ok(false);
        }
        let paths = rows
            .iter()
            .map(|row| Ok(Path::from(row.try_get::<_, String>(1)?)))
            .collect::<Result<Vec<_>>>()?;
        store
            .delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed())
            .try_collect::<Vec<_>>()
            .await?;
        let ids = rows
            .iter()
            .map(|row| row.try_get::<_, String>(0))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.sql.client.execute(&format!("UPDATE {schema}.packs SET state='deleted' WHERE tenant=$1 AND id=ANY($2::text[]::uuid[]) AND state='retired'"),&[&tenant,&ids]).await?;
        Ok(true)
    }
}
fn pack(row: &tokio_postgres::Row) -> Result<Pack> {
    Ok(Pack {
        id: row.try_get(0)?,
        owner: row.try_get(1)?,
        path: row.try_get(2)?,
        length: u64::try_from(row.try_get::<_, i64>(3)?)?,
        checksum: row.try_get(4)?,
    })
}
