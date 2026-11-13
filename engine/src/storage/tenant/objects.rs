//! Catalog-isolated virtual listing and terminal namespace deletion.
use super::{
    manifest::{Catalog, Chunk, Manifest, chunk},
    namespace,
};
use anyhow::{Result, ensure};

impl Manifest {
    pub(super) async fn remove(&self, catalog: &Catalog, name: &str) -> Result<()> {
        let space = namespace(name)?;
        let schema = &self.config.schema;
        let mut session = self.pool.acquire().await?;
        let tx = session.connection()?.transaction().await?;
        // Sealing precedes DELETE in the same transaction. Every pending installer
        // locks this namespace and checks sealed again after its physical PUT.
        tx.execute(&format!("INSERT INTO {schema}.namespaces(tenant,catalog,namespace,sealed) VALUES($1,$2::text::uuid,$3,true) ON CONFLICT(tenant,catalog,namespace) DO UPDATE SET sealed=true"),&[&catalog.tenant,&catalog.catalog,&space]).await?;
        tx.execute(
            &format!(
                "DELETE FROM {schema}.chunks WHERE tenant=$1 AND catalog=$2::text::uuid AND name=$3"
            ),
            &[&catalog.tenant, &catalog.catalog, &name],
        )
        .await?;
        tx.commit().await?;
        drop(session);
        Ok(())
    }
    pub(super) async fn page(
        &self,
        catalog: &Catalog,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Chunk>> {
        ensure!((1..=1000).contains(&limit), "invalid virtual object page limit");
        let mut sql = self.pool.acquire().await?;
        let schema = &self.config.schema;
        let rows=sql.connection()?.query(&format!("SELECT c.name,c.namespace,c.pack::text,p.path,c.offset_bytes,c.length,c.checksum,c.created_micros,c.catalog::text FROM {schema}.chunks c JOIN {schema}.packs p ON p.id=c.pack AND p.tenant=c.tenant WHERE c.tenant=$1 AND c.catalog=$2::text::uuid AND starts_with(c.name,$3) AND ($4::text IS NULL OR c.name>$4) AND p.state='ready' ORDER BY c.name LIMIT $5"),&[&catalog.tenant,&catalog.catalog,&prefix,&after,&i64::try_from(limit)?]).await?;
        drop(sql);
        rows.iter().map(chunk).collect()
    }
}
