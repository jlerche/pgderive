//! Owned physical PUT intents and atomic logical mapping installation.
use super::{
    manifest::{Catalog, Manifest, Pack},
    namespace,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use std::sync::Arc;

pub(super) struct Chunk {
    pub(super) catalog: Catalog,
    pub(super) name: String,
    pub(super) bytes: Bytes,
    pub(super) checksum: String,
}
pub(super) struct Owner {
    pub(super) id: String,
    pub(super) tenant: String,
    pub(super) manifest: Arc<Manifest>,
    pub(super) sql: crate::postgres::Session,
    pub(super) inventory_cursor: Option<String>,
    pub(super) inventory_due: tokio::time::Instant,
}
#[derive(Clone, Debug)]
pub(super) enum Installed {
    Created,
    Exists,
    Sealed,
}
impl Owner {
    pub(super) async fn connect(manifest: Arc<Manifest>, tenant: &str) -> Result<Self> {
        let mut sql = crate::postgres::Session::connect(&manifest.config.postgres).await?;
        let tx = sql.client.transaction().await?;
        let id: String = tx
            .query_one(
                &format!(
                    "INSERT INTO {}.owners(tenant) VALUES($1) RETURNING id::text",
                    manifest.config.schema
                ),
                &[&tenant],
            )
            .await?
            .try_get(0)?;
        tx.query_one("SELECT pg_advisory_lock(hashtextextended($1,0))", &[&lock(&id)]).await?;
        tx.commit().await.context("upload owner installation COMMIT uncertain")?;
        Ok(Self {
            id,
            tenant: tenant.into(),
            manifest,
            sql,
            inventory_cursor: None,
            inventory_due: tokio::time::Instant::now(),
        })
    }
    pub(super) async fn reserve(&mut self, tenant: &str, bytes: &[u8]) -> Result<Pack> {
        ensure!(tenant == self.tenant, "cross-tenant physical intent rejected");
        let checksum = super::checksum(bytes);
        let length = u64::try_from(bytes.len())?;
        let tx = self.sql.client.transaction().await?;
        active(&tx, &self.manifest.config.schema, &self.id).await?;
        let id: String =
            tx.query_one("SELECT pg_catalog.gen_random_uuid()::text", &[]).await?.try_get(0)?;
        let path = format!("owners/{}/{id}-{checksum}", self.id);
        tx.execute(&format!("INSERT INTO {}.packs(id,tenant,owner,path,length,checksum,state) VALUES($1::text::uuid,$2,$3::text::uuid,$4,$5,$6,'writing')",self.manifest.config.schema),&[&id,&tenant,&self.id,&path,&i64::try_from(length)?,&checksum]).await?;
        tx.commit().await.context("physical upload intent COMMIT uncertain; do not issue PUT")?;
        Ok(Pack { id, owner: self.id.clone(), path, length, checksum })
    }
    pub(super) async fn install(
        &mut self,
        pack: &Pack,
        chunks: &[Chunk],
    ) -> Result<Vec<Installed>> {
        ensure!(chunks.iter().all(|c|c.catalog.tenant==self.tenant && super::checksum(&c.bytes)==c.checksum),"upload chunk tenant/checksum mismatch");
        ensure!(pack.owner == self.id, "physical pack belongs to another uploader");
        let tx = self.sql.client.transaction().await?;
        let schema = &self.manifest.config.schema;
        active(&tx, schema, &self.id).await?;
        let row=tx.query_one(&format!("SELECT state,length,checksum FROM {schema}.packs WHERE id=$1::text::uuid AND owner=$2::text::uuid FOR UPDATE"),&[&pack.id,&self.id]).await?;
        ensure!(
            row.try_get::<_, String>(0)? == "writing"
                && u64::try_from(row.try_get::<_, i64>(1)?)? == pack.length
                && row.try_get::<_, String>(2)? == pack.checksum,
            "physical intent identity changed"
        );
        let result = install_chunks(&tx, schema, pack, chunks).await?;
        tx.execute(
            &format!("UPDATE {schema}.packs SET state='ready' WHERE id=$1::text::uuid"),
            &[&pack.id],
        )
        .await?;
        tx.commit()
            .await
            .context("logical object mapping COMMIT uncertain; retain physical intent")?;
        Ok(result)
    }
    pub(super) async fn close(&self) -> Result<()> {
        self.sql
            .client
            .execute(
                &format!(
                    "UPDATE {}.owners SET active=false WHERE id=$1::text::uuid",
                    self.manifest.config.schema
                ),
                &[&self.id],
            )
            .await?;
        Ok(())
    }
}
async fn active(tx: &tokio_postgres::Transaction<'_>, schema: &str, id: &str) -> Result<()> {
    let active: bool = tx
        .query_one(
            &format!("SELECT active FROM {schema}.owners WHERE id=$1::text::uuid FOR SHARE"),
            &[&id],
        )
        .await?
        .try_get(0)?;
    ensure!(active, "physical uploader fenced");
    Ok(())
}
async fn install_chunks(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    pack: &Pack,
    chunks: &[Chunk],
) -> Result<Vec<Installed>> {
    lock_namespaces(tx, schema, chunks).await?;
    let mut result = Vec::with_capacity(chunks.len());
    let mut offset = 0_i64;
    for chunk in chunks {
        let length = i64::try_from(chunk.bytes.len())?;
        result.push(install_chunk(tx, schema, pack, chunk, (offset, length)).await?);
        offset = offset.checked_add(length).context("pack offset overflow")?;
    }
    ensure!(u64::try_from(offset)? == pack.length, "pack length and logical extents disagree");
    Ok(result)
}
async fn lock_namespaces(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    chunks: &[Chunk],
) -> Result<()> {
    let spaces = chunks
        .iter()
        .map(|chunk| {
            Ok((
                chunk.catalog.tenant.clone(),
                chunk.catalog.catalog.clone(),
                namespace(&chunk.name)?,
            ))
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    // Concurrent process batches may arrive in opposite catalog order. Acquire
    // namespace locks consistently before installing any chunk mappings.
    for (tenant, catalog, space) in spaces {
        tx.execute(&format!("INSERT INTO {schema}.namespaces(tenant,catalog,namespace) VALUES($1,$2::text::uuid,$3) ON CONFLICT DO NOTHING"),&[&tenant,&catalog,&space]).await?;
        tx.query_one(&format!("SELECT sealed FROM {schema}.namespaces WHERE tenant=$1 AND catalog=$2::text::uuid AND namespace=$3 FOR UPDATE"),&[&tenant,&catalog,&space]).await?;
    }
    Ok(())
}
async fn install_chunk(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    pack: &Pack,
    chunk: &Chunk,
    extent: (i64, i64),
) -> Result<Installed> {
    let catalog = &chunk.catalog;
    let space = namespace(&chunk.name)?;
    let sealed:bool=tx.query_one(&format!("SELECT sealed FROM {schema}.namespaces WHERE tenant=$1 AND catalog=$2::text::uuid AND namespace=$3 FOR UPDATE"),&[&catalog.tenant,&catalog.catalog,&space]).await?.try_get(0)?;
    if sealed {
        return Ok(Installed::Sealed);
    }
    let count=tx.execute(&format!("INSERT INTO {schema}.chunks(tenant,catalog,name,namespace,pack,offset_bytes,length,checksum) VALUES($1,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7,$8) ON CONFLICT DO NOTHING"),&[&catalog.tenant,&catalog.catalog,&chunk.name,&space,&pack.id,&extent.0,&extent.1,&chunk.checksum]).await?;
    Ok(if count == 1 { Installed::Created } else { Installed::Exists })
}
pub(super) fn lock(owner: &str) -> String {
    format!("pgderive-storage-owner:{owner}")
}
