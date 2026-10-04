use crate::{Config, catalog::Lsn};
use anyhow::{Context, Result, ensure};
use replication_control::{Client, NoTls, SimpleQueryMessage, config::ReplicationMode};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_postgres::{IsolationLevel, Transaction};

/// Cluster and database identity reported on the replication connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Immutable cluster system identifier, represented as unsigned decimal text.
    pub system: String,
    /// Current source timeline; timeline changes require explicit recovery validation.
    pub timeline: u32,
    /// Database name selected by the replication connection.
    pub database: String,
}
impl Identity {
    /// Validate the SQL session is the same cluster, timeline and database.
    /// Requires EXECUTE on `pg_control_system` and `pg_control_checkpoint`.
    ///
    /// # Errors
    /// Rejects misrouted SQL connections or unavailable identity inspection privileges.
    pub async fn verify_sql(
        &self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
    ) -> Result<()> {
        let row = sql.query_one("SELECT s.system_identifier::text,c.timeline_id::bigint,current_database() FROM pg_catalog.pg_control_system() s CROSS JOIN pg_catalog.pg_control_checkpoint() c", &[]).await?;
        ensure!(
            row.try_get::<_, String>(0)? == self.system
                && row.try_get::<_, i64>(1)? == i64::from(self.timeline)
                && row.try_get::<_, String>(2)? == self.database,
            "SQL source cluster/timeline/database identity mismatch"
        );
        Ok(())
    }
    /// Validate the live slot cannot skip WAL beyond the durable engine boundary.
    ///
    /// # Errors
    /// Rejects changed identity, active/advanced/invalid slots or incompatible retention.
    pub async fn validate_resume(&self, config: &Config, end: Lsn) -> Result<()> {
        config.validate()?;
        let connection = connect(config).await?;
        ensure!(identify(&connection.sql).await? == *self, "registered source identity changed");
        let slot = &config.replication.slot;
        let rows = connection.sql.simple_query(&format!("SELECT plugin,database,temporary::text,active::text,confirmed_flush_lsn::text,restart_lsn::text,wal_status FROM pg_replication_slots WHERE slot_name='{slot}'")).await?;
        let row = only_row(&rows)?;
        ensure!(
            row.get("plugin") == Some("pgoutput")
                && row.get("database") == Some(self.database.as_str())
                && row.get("temporary") == Some("false")
                && row.get("active") == Some("false")
                && matches!(row.get("wal_status"), Some("reserved" | "extended")),
            "source slot is incompatible or has lost WAL"
        );
        let confirmed: Lsn = row
            .get("confirmed_flush_lsn")
            .context("source slot has no confirmed position")?
            .parse()?;
        let restart: Lsn =
            row.get("restart_lsn").context("source slot has no restart position")?.parse()?;
        ensure!(
            confirmed <= end && restart <= end,
            "source slot advanced beyond durable engine position"
        );
        drop(connection.sql);
        connection.task.await.context("resume validation control task failed")??;
        Ok(())
    }
    /// Inspect cluster/database identity without creating or advancing a slot.
    ///
    /// # Errors
    /// Returns configuration, TLS, control protocol or connection shutdown errors.
    pub async fn inspect(config: &Config) -> Result<Self> {
        config.validate()?;
        let connection = connect(config).await?;
        let identity = identify(&connection.sql).await?;
        drop(connection.sql);
        connection.task.await.context("identity control task failed")??;
        Ok(identity)
    }
}
struct Connection {
    sql: Client,
    task: JoinHandle<Result<(), replication_control::Error>>,
}
/// Slot-consistent exported snapshot, owning its idle exporting connection.
///
/// Keep this owner alive until the initial source read finishes. Closing it does
/// not drop the persistent slot; failures retain the slot and WAL for investigation.
pub struct Export {
    connection: Connection,
    /// Verified cluster/database identity used to bind future source progress.
    identity: Identity,
    /// Newly created persistent pgoutput slot.
    slot: String,
    /// Exact WAL boundary separating initial snapshot rows from subsequent CDC.
    consistent: Lsn,
    snapshot: String,
}
impl Export {
    /// Cluster/database identity reported before snapshot export.
    #[must_use]
    pub const fn identity(&self) -> &Identity {
        &self.identity
    }
    /// Newly created slot name.
    #[must_use]
    pub fn slot(&self) -> &str {
        &self.slot
    }
    /// Exact WAL boundary for initial rows and subsequent CDC.
    #[must_use]
    pub const fn consistent(&self) -> Lsn {
        self.consistent
    }

    /// Create a new logical slot and export its matching consistent snapshot.
    /// This never replaces an existing slot or acknowledges source data.
    ///
    /// # Errors
    /// Returns configuration, TLS, connection, existing-slot or protocol errors.
    pub async fn create(config: &Config, slot: &str) -> Result<Self> {
        config.validate()?;
        ensure!(crate::configuration::identifier(slot), "invalid bootstrap slot identifier");
        let connection = connect(config).await?;
        let identity = identify(&connection.sql).await?;
        ensure!(
            identity.database == config.postgres.database,
            "replication database identity mismatch"
        );
        let rows = connection
            .sql
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput EXPORT_SNAPSHOT"
            ))
            .await?;
        let row = only_row(&rows)?;
        ensure!(
            row.get("slot_name") == Some(slot) && row.get("output_plugin") == Some("pgoutput"),
            "unexpected slot creation response"
        );
        let consistent =
            row.get("consistent_point").context("missing slot consistent point")?.parse()?;
        let snapshot = row.get("snapshot_name").context("missing exported snapshot")?.to_owned();
        ensure!(snapshot_name(&snapshot), "invalid exported snapshot identifier");
        Ok(Self { connection, identity, slot: slot.into(), consistent, snapshot })
    }
    /// Import this snapshot before any query in a read-only repeatable-read transaction.
    /// Source copying must use the returned transaction, not another connection.
    ///
    /// # Errors
    /// Returns expired snapshot, database mismatch, or transaction setup failures.
    pub async fn import<'a>(&self, sql: &'a mut tokio_postgres::Client) -> Result<Transaction<'a>> {
        let tx = sql
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        tx.batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", self.snapshot)).await?;
        self.identity.verify_sql(&tx).await?;
        Ok(tx)
    }
    /// Close the exporter after the source transaction finishes, retaining the slot.
    ///
    /// # Errors
    /// Returns exporting connection shutdown failures.
    pub async fn close(self) -> Result<()> {
        drop(self.connection.sql);
        self.connection.task.await.context("snapshot exporter task failed")?.map_err(Into::into)
    }
}
async fn connect(config: &Config) -> Result<Connection> {
    let pg = &config.postgres;
    let mut options = replication_control::Config::new();
    options
        .host(&pg.host)
        .port(pg.port)
        .user(&pg.user)
        .password(&pg.password)
        .dbname(&pg.database)
        .replication_mode(ReplicationMode::Logical);
    if pg.tls == "disable" {
        let (sql, connection) = options.connect(NoTls).await?;
        return Ok(Connection { sql, task: tokio::spawn(connection) });
    }
    let mut tls = native_tls::TlsConnector::builder();
    if let Some(path) = &pg.ca_file {
        let certificate = native_tls::Certificate::from_pem(&std::fs::read(path)?)?;
        tls.add_root_certificate(certificate);
    }
    options.ssl_mode(replication_control::config::SslMode::Require);
    let tls = replication_control_tls::MakeTlsConnector::new(tls.build()?);
    let (sql, connection) = options.connect(tls).await?;
    Ok(Connection { sql, task: tokio::spawn(connection) })
}
async fn identify(sql: &Client) -> Result<Identity> {
    let rows = sql.simple_query("IDENTIFY_SYSTEM").await?;
    let row = only_row(&rows)?;
    let system = row.get("systemid").context("missing source system identity")?.to_owned();
    system.parse::<u64>()?;
    let timeline = row.get("timeline").context("missing source timeline")?.parse()?;
    let database = row.get("dbname").context("missing source database")?.to_owned();
    Ok(Identity { system, timeline, database })
}
fn only_row(rows: &[SimpleQueryMessage]) -> Result<&replication_control::SimpleQueryRow> {
    let mut rows = rows.iter().filter_map(|message| {
        if let SimpleQueryMessage::Row(row) = message { Some(row) } else { None }
    });
    let row = rows.next().context("missing replication control row")?;
    ensure!(rows.next().is_none(), "multiple replication control rows");
    Ok(row)
}
fn snapshot_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}
