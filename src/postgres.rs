//! Owned SQL transport shared by source catalogs and the tenant storage authority.
use anyhow::Result;
use tokio::{task::JoinHandle, time::Duration};
use tokio_postgres::NoTls;

pub struct Session {
    pub(crate) client: crate::catalog::Connection,
    task: JoinHandle<Result<(), tokio_postgres::Error>>,
}
impl Session {
    pub(crate) async fn connect(pg: &crate::configuration::Postgres) -> Result<Self> {
        Box::pin(Self::connect_inner(pg)).await
    }
    async fn connect_inner(pg: &crate::configuration::Postgres) -> Result<Self> {
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
        let session = Self { client: client.into(), task };
        // A successful manifest commit is part of the source acknowledgement
        // boundary. Role/database defaults must never weaken that durability.
        session
            .client
            .batch_execute(
                "SET synchronous_commit=on; SET statement_timeout='30s'; SET lock_timeout='10s'",
            )
            .await?;
        crate::source::Environment::configure(&*session.client).await?;
        Ok(session)
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.task.abort();
    }
}
