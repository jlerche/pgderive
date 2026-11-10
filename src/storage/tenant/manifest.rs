//! Coarse logical-object membership and mutable physical locations.
use super::{Config, pool::Pool};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::sync::Arc;

pub(super) struct Manifest {
    pub(super) config: Config,
    pub(super) pool: Arc<Pool>,
}
#[derive(Clone, Debug)]
pub(super) struct Catalog {
    pub(super) tenant: String,
    pub(super) catalog: String,
}
#[derive(Clone, Debug)]
pub(super) struct Chunk {
    pub(super) name: String,
    pub(super) catalog: String,
    pub(super) path: String,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) checksum: String,
    pub(super) created: u64,
}
#[derive(Clone, Debug)]
pub(super) struct Pack {
    pub(super) id: String,
    pub(super) owner: String,
    pub(super) path: String,
    pub(super) length: u64,
    pub(super) checksum: String,
}
impl Manifest {
    pub(super) async fn connect(config: Config, pool: Arc<Pool>) -> Result<Arc<Self>> {
        config.validate()?;
        let mut sql = pool.acquire().await?;
        let tx = sql.connection()?.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("pgderive-tenant-storage:{}", config.schema)],
        )
        .await?;
        let exists: bool = tx
            .query_one("SELECT to_regnamespace($1) IS NOT NULL", &[&config.schema])
            .await?
            .try_get(0)?;
        if exists {
            let owned: bool = tx
                .query_one(
                    "SELECT to_regclass($1) IS NOT NULL",
                    &[&format!("{}.version", config.schema)],
                )
                .await?
                .try_get(0)?;
            if !owned {
                let populated:bool=tx.query_one("SELECT EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1)",&[&config.schema]).await?.try_get(0)?;
                ensure!(!populated, "unbound tenant manifest requires a fresh metadata schema");
            }
        }
        if !exists {
            tx.batch_execute(&format!("CREATE SCHEMA {}", config.schema)).await?;
        }
        tx.batch_execute(&format!(include_str!("schema.sql"), schema = config.schema)).await?;
        let version: i32 = tx
            .query_one(
                &format!("SELECT version FROM {}.version WHERE singleton", config.schema),
                &[],
            )
            .await?
            .try_get(0)?;
        ensure!(version == 1, "unsupported tenant storage manifest version");
        tx.commit().await.context("manifest installation COMMIT uncertain")?;
        drop(sql);
        Ok(Arc::new(Self { config, pool }))
    }
    pub(super) async fn tenant(&self, tenant: &str, storage: &Value) -> Result<String> {
        let mut sql = self.pool.acquire().await?;
        let tx = sql.connection()?.transaction().await?;
        tx.execute(
            &format!(
                "INSERT INTO {}.tenants(tenant,storage) VALUES($1,$2) ON CONFLICT DO NOTHING",
                self.config.schema
            ),
            &[&tenant, storage],
        )
        .await?;
        let row = tx
            .query_one(
                &format!(
                    "SELECT identity::text,storage FROM {}.tenants WHERE tenant=$1",
                    self.config.schema
                ),
                &[&tenant],
            )
            .await?;
        ensure!(
            row.try_get::<_, Value>(1)? == *storage,
            "tenant physical storage ownership mismatch"
        );
        let id = row.try_get(0)?;
        tx.commit().await?;
        drop(sql);
        Ok(id)
    }
    pub(super) async fn catalog(
        &self,
        catalog: &Catalog,
        source: &Value,
        prefix: &str,
    ) -> Result<()> {
        let mut sql = self.pool.acquire().await?;
        let tx = sql.connection()?.transaction().await?;
        tx.execute(&format!("INSERT INTO {}.catalogs(tenant,catalog,source,prefix) VALUES($1,$2::text::uuid,$3,$4) ON CONFLICT DO NOTHING",self.config.schema),&[&catalog.tenant,&catalog.catalog,source,&prefix]).await?;
        let row=tx.query_one(&format!("SELECT source,prefix FROM {}.catalogs WHERE tenant=$1 AND catalog=$2::text::uuid",self.config.schema),&[&catalog.tenant,&catalog.catalog]).await?;
        ensure!(
            row.try_get::<_, Value>(0)? == *source && row.try_get::<_, String>(1)? == prefix,
            "logical catalog storage ownership mismatch"
        );
        tx.commit().await?;
        drop(sql);
        Ok(())
    }
    pub(super) async fn lookup(&self, catalog: &Catalog, name: &str) -> Result<Option<Chunk>> {
        let mut sql = self.pool.acquire().await?;
        let row=sql.connection()?.query_opt(&format!("SELECT c.name,c.namespace,c.pack::text,p.path,c.offset_bytes,c.length,c.checksum,c.created_micros,c.catalog::text FROM {0}.chunks c JOIN {0}.packs p ON p.id=c.pack AND p.tenant=c.tenant WHERE c.tenant=$1 AND c.catalog=$2::text::uuid AND c.name=$3 AND p.state='ready'",self.config.schema),&[&catalog.tenant,&catalog.catalog,&name]).await?;
        drop(sql);
        row.as_ref().map(chunk).transpose()
    }
}
pub(super) fn chunk(row: &tokio_postgres::Row) -> Result<Chunk> {
    let name: String = row.try_get(0)?;
    ensure!(
        super::namespace(&name)? == row.try_get::<_, String>(1)?,
        "logical chunk namespace mismatch"
    );
    Ok(Chunk {
        name,
        catalog: row.try_get(8)?,
        path: row.try_get(3)?,
        offset: u64::try_from(row.try_get::<_, i64>(4)?)?,
        length: u64::try_from(row.try_get::<_, i64>(5)?)?,
        checksum: row.try_get(6)?,
        created: u64::try_from(row.try_get::<_, i64>(7)?)?,
    })
}
