use crate::Config;
use anyhow::{Context, Result, ensure};
use tokio::{
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_postgres::{Client, NoTls};

pub(super) struct Session {
    pub(super) client: Client,
    task: JoinHandle<Result<(), tokio_postgres::Error>>,
}
impl Session {
    pub(super) async fn connect(config: &Config) -> Result<Self> {
        let pg = &config.postgres;
        let mut settings = tokio_postgres::Config::new();
        settings
            .host(&pg.host)
            .port(pg.port)
            .dbname(&pg.database)
            .user(&pg.user)
            .password(&pg.password)
            .connect_timeout(Duration::from_secs(10));
        let (client, task) = if pg.tls == "disable" {
            let (client, connection) = settings.connect(NoTls).await?;
            (client, tokio::spawn(connection))
        } else {
            settings.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut tls = native_tls::TlsConnector::builder();
            if let Some(path) = &pg.ca_file {
                tls.add_root_certificate(native_tls::Certificate::from_pem(&std::fs::read(path)?)?);
            }
            let (client, connection) =
                settings.connect(postgres_native_tls::MakeTlsConnector::new(tls.build()?)).await?;
            (client, tokio::spawn(connection))
        };
        let session = Self { client, task };
        session.client.batch_execute("SET statement_timeout='30s'; SET lock_timeout='10s'").await?;
        Ok(session)
    }
    pub(super) async fn own(&self, schema: &str, query: &str) -> Result<()> {
        let scope = format!("pgderive-worker:{schema}:{query}");
        let row = timeout(
            Duration::from_secs(10),
            self.client.query_one(
                "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended($1,0))",
                &[&scope],
            ),
        )
        .await
        .context("worker ownership acquisition timed out")??;
        ensure!(row.try_get::<_, bool>(0)?, "another worker owns this query");
        Ok(())
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.task.abort();
    }
}
