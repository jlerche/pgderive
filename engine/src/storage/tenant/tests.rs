//! Sequential live protocol qualification, invoked explicitly by the common PG gate.
use super::{
    batch::Group,
    config::Batching,
    manifest::{Catalog, Manifest},
    pool::Pool,
    store::Store as Virtual,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, path::Path};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::Semaphore;
mod faults;
mod protocol;
mod sweep;

struct Fixture {
    manifest: Arc<Manifest>,
    group: Arc<Group>,
    physical: Arc<crate::engine::tests::store::Store>,
    catalogs: [Catalog; 2],
}
impl Fixture {
    async fn open() -> Result<Self> {
        let port = std::env::var("PGDERIVE_TEST_POSTGRES_PORT")
            .context("live tenant storage requires explicit owned PostgreSQL port")?
            .parse::<u16>()?;
        let postgres = serde_json::from_value(
            serde_json::json!({"host":"127.0.0.1","port":port,"database":"pgderive_dev","user":"postgres","password":std::env::var("PGPASSWORD").unwrap_or_else(|_|"postgres".into()),"tls":"disable"}),
        )?;
        let sql = crate::postgres::Session::connect(&postgres).await?;
        let identity: String = sql
            .client
            .query_one("SELECT replace(gen_random_uuid()::text,'-','')", &[])
            .await?
            .try_get(0)?;
        let config = super::Config {
            postgres,
            schema: format!("tenant_live_{identity}"),
            object_prefix: format!("tenant-live-{identity}"),
            batching: Batching { delay_ms: 25, ..Batching::default() },
        };
        println!("Tenant storage failure fixture: {}", config.schema);
        let manifest =
            Manifest::connect(config.clone(), Pool::new(config.postgres.clone())).await?;
        manifest.tenant("alice", &serde_json::json!({"fixture":identity})).await?;
        let a: String =
            sql.client.query_one("SELECT gen_random_uuid()::text", &[]).await?.try_get(0)?;
        let b: String =
            sql.client.query_one("SELECT gen_random_uuid()::text", &[]).await?.try_get(0)?;
        let catalogs = [
            Catalog { tenant: "alice".into(), catalog: a },
            Catalog { tenant: "alice".into(), catalog: b },
        ];
        for (index, catalog) in catalogs.iter().enumerate() {
            manifest
                .catalog(
                    catalog,
                    &serde_json::json!({"unavailable_source_database":index}),
                    "same-logical-alias",
                )
                .await?;
        }
        let physical = Arc::new(crate::engine::tests::store::Store::new(None));
        let group = Group::start(
            manifest.clone(),
            "alice".into(),
            physical.clone(),
            Arc::new(Semaphore::new(64 * 1024 * 1024)),
            Arc::new(Semaphore::new(1)),
        )
        .await?;
        Ok(Self { manifest, group, physical, catalogs })
    }
    fn store(&self, index: usize) -> Arc<Virtual> {
        Arc::new(Virtual { group: self.group.clone(), catalog: self.catalogs[index].clone() })
    }
    async fn put(&self, index: usize, name: &str, bytes: Vec<u8>) -> Result<()> {
        self.group
            .put(self.catalogs[index].clone(), name.into(), Bytes::from(bytes).into())
            .await?;
        Ok(())
    }
    async fn uuid(&self) -> Result<String> {
        let mut sql = self.manifest.pool.acquire().await?;
        let value =
            sql.connection()?.query_one("SELECT gen_random_uuid()::text", &[]).await?.try_get(0)?;
        drop(sql);
        Ok(value)
    }
    async fn cleanup(&self) -> Result<()> {
        self.group.close().await?;
        let mut sql = self.manifest.pool.acquire().await?;
        sql.connection()?
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.manifest.config.schema))
            .await?;
        drop(sql);
        Ok(())
    }
    async fn preserve(&self, error: &anyhow::Error) -> Result<()> {
        use futures_util::TryStreamExt as _;
        let path =
            std::path::Path::new("artifacts/tenant-storage").join(&self.manifest.config.schema);
        std::fs::create_dir_all(&path)?;
        std::fs::write(path.join("error.txt"), format!("{error:#}"))?;
        let mut objects = Vec::new();
        for meta in self.physical.list(None).try_collect::<Vec<_>>().await? {
            let bytes = self.physical.get(&meta.location).await?.bytes().await?;
            objects.push((meta.location.to_string(), bytes.to_vec()));
        }
        std::fs::write(path.join("physical-objects.json"), serde_json::to_vec(&objects)?)?;
        let pg = &self.manifest.config.postgres;
        std::process::Command::new("pg_dump")
            .args([
                "-h",
                &pg.host,
                "-p",
                &pg.port.to_string(),
                "-U",
                &pg.user,
                "-d",
                &pg.database,
                "--schema",
                &self.manifest.config.schema,
                "--file",
            ])
            .arg(path.join("manifest.sql"))
            .env("PGPASSWORD", &pg.password)
            .status()?;

        Ok(())
    }
}
#[tokio::test]
#[ignore = "sequential PostgreSQL protocol qualification is run by check_integration.sh"]
async fn tenant_storage_manifest_live() -> Result<()> {
    let fixture = Fixture::open().await?;
    let result = async {
        sweep::pinned_relocation_does_not_starve_sweep(&fixture).await?;
        protocol::packed_relocation(&fixture).await?;
        protocol::cancelled_contributor(&fixture).await?;
        faults::terminal_delete(&fixture).await?;
        faults::late_put(&fixture).await?;
        faults::ambiguous_mapping_commit(&fixture).await?;
        faults::tenant_isolation(&fixture).await?;
        protocol::range_and_integrity(&fixture).await?;
        super::registry::verify_recovery_retention(
            fixture.manifest.clone(),
            fixture.catalogs[0].clone(),
        )
        .await?;
        protocol::cancelled_shutdown(&fixture).await
    }
    .await;
    if let Err(error) = result {
        fixture.preserve(&error).await?;
        return Err(error);
    }
    fixture.cleanup().await?;
    drop(fixture);
    println!(
        "Tenant shared packs: batching, independent catalogs, relocation/pins, cancellation, terminal deletion, uncertain commits and tenant isolation passed"
    );
    Ok(())
}
