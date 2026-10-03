use crate::{
    Config, listener,
    transaction::{Operation, Row, Transaction},
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_postgres::{Client, NoTls};

type Model = BTreeMap<(String, String), Row>;
type ConnectionTask = JoinHandle<Result<(), tokio_postgres::Error>>;

struct Fixture {
    schema: String,
    slot: String,
    publication: String,
}

/// Insert, update, and delete Nexmark-shaped rows while validating live CDC.
///
/// Creates an isolated schema and an unused `pgderive_` publication/slot, then
/// removes them after the run. Existing publication or slot names are rejected.
/// The fixture compares reconstructed state with source SQL after each commit.
///
/// # Errors
/// Returns setup, stream, oracle, timeout, or cleanup errors. The local harness
/// requires a non-TLS connection; the standalone listener also supports TLS.
pub async fn run_harness(config: Config) -> Result<()> {
    ensure!(config.postgres.tls == "disable", "local harness requires postgres.tls=disable");
    ensure!(
        config.replication.slot.starts_with("pgderive_"),
        "harness slot must start with pgderive_"
    );
    ensure!(
        config.replication.publication.starts_with("pgderive_"),
        "harness publication must start with pgderive_"
    );
    let (mut sql, connection) = connect(&config).await?;
    let fixture = Fixture::prepare(&mut sql, &config).await?;
    let result = execute(&sql, &fixture, config).await;
    let cleanup = fixture.cleanup(&sql).await;
    drop(sql);
    let disconnected = connection.await.context("SQL connection task failed")?;
    result?;
    cleanup?;
    disconnected?;
    eprintln!(
        "harness passed: three transactions, full old/new rows, rollback excluded, source SQL oracle, no acknowledgement; fixture cleaned up"
    );
    Ok(())
}

async fn connect(config: &Config) -> Result<(Client, ConnectionTask)> {
    let pg = &config.postgres;
    let (client, connection) = tokio_postgres::Config::new()
        .host(&pg.host)
        .port(pg.port)
        .dbname(&pg.database)
        .user(&pg.user)
        .password(&pg.password)
        .connect(NoTls)
        .await
        .context("connecting harness SQL session")?;
    Ok((client, tokio::spawn(connection)))
}

impl Fixture {
    async fn prepare(sql: &mut Client, config: &Config) -> Result<Self> {
        let fixture = Self {
            schema: format!("pgderive_harness_{}", std::process::id()),
            slot: config.replication.slot.clone(),
            publication: config.replication.publication.clone(),
        };
        let used: bool = sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1) OR EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$2)", &[&fixture.slot, &fixture.publication]).await?.get(0);
        ensure!(
            !used,
            "harness requires unused slot/publication names; existing resources were not changed"
        );
        let schema = &fixture.schema;
        let ddl = format!("CREATE SCHEMA {schema};
            CREATE TABLE {schema}.person(id bigint PRIMARY KEY, name text);
            CREATE TABLE {schema}.auction(id bigint PRIMARY KEY, seller bigint, category bigint);
            CREATE TABLE {schema}.bid(id bigint PRIMARY KEY, auction bigint, bidder bigint, price bigint);
            ALTER TABLE {schema}.person REPLICA IDENTITY FULL;
            ALTER TABLE {schema}.auction REPLICA IDENTITY FULL;
            ALTER TABLE {schema}.bid REPLICA IDENTITY FULL;
            CREATE PUBLICATION {} FOR TABLE {schema}.person, {schema}.auction, {schema}.bid;", fixture.publication);
        let tx = sql.transaction().await?;
        tx.batch_execute(&ddl).await?;
        tx.commit().await?;
        if let Err(error) = sql
            .query_one(
                "SELECT * FROM pg_create_logical_replication_slot($1,'pgoutput')",
                &[&fixture.slot],
            )
            .await
        {
            sql.batch_execute(&format!(
                "DROP PUBLICATION {}; DROP SCHEMA {schema} CASCADE",
                fixture.publication
            ))
            .await?;
            return Err(error).context("creating harness replication slot");
        }
        Ok(fixture)
    }

    async fn cleanup(&self, sql: &Client) -> Result<()> {
        timeout(Duration::from_secs(10), async {
            loop {
                let active: bool = sql
                    .query_one(
                        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
                        &[&self.slot],
                    )
                    .await?
                    .get(0);
                if !active {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            sql.query_one("SELECT pg_drop_replication_slot($1)", &[&self.slot]).await?;
            sql.batch_execute(&format!(
                "DROP PUBLICATION {}; DROP SCHEMA {} CASCADE",
                self.publication, self.schema
            ))
            .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("fixture cleanup timed out")??;
        Ok(())
    }
}

async fn execute(sql: &Client, fixture: &Fixture, mut config: Config) -> Result<()> {
    config.listener.max_transactions = 3;
    let (ready, connected) = oneshot::channel();
    let (observed, mut received) = mpsc::channel(8);
    let mut task = tokio::spawn(listener::run(config, Some(ready), Some(observed)));
    let result = async {
        timeout(Duration::from_secs(10), connected)
            .await
            .context("listener connection timed out")?
            .context("listener failed to connect")?;
        drive(sql, fixture, &mut received).await
    }
    .await;
    if result.is_err() {
        if task.is_finished() {
            task.await.context("listener task failed")??;
        } else {
            task.abort();
            let _cancelled = task.await;
        }
        return result;
    }
    let completed = timeout(Duration::from_secs(20), &mut task).await;
    if completed.is_err() {
        task.abort();
    }
    completed.context("listener shutdown timed out")?.context("listener task failed")?
}

async fn drive(
    sql: &Client,
    fixture: &Fixture,
    received: &mut mpsc::Receiver<Transaction>,
) -> Result<()> {
    let schema = &fixture.schema;
    let initial: String = sql
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
            &[&fixture.slot],
        )
        .await?
        .get(0);
    let cases = [
        (
            format!(
                "BEGIN;
            INSERT INTO {schema}.person VALUES(1,'Alice');
            INSERT INTO {schema}.auction VALUES(1,1,10);
            INSERT INTO {schema}.bid SELECT n,1,1,100+n FROM generate_series(1,10) n; COMMIT;"
            ),
            12,
            Operation::Insert,
        ),
        (
            format!(
                "BEGIN; INSERT INTO {schema}.person VALUES(99,'rolled back'); ROLLBACK;
            BEGIN; UPDATE {schema}.person SET name=NULL WHERE id=1;
            UPDATE {schema}.bid SET price=price+100; COMMIT;"
            ),
            11,
            Operation::Update,
        ),
        (format!("BEGIN; DELETE FROM {schema}.bid WHERE id%2=0; COMMIT;"), 5, Operation::Delete),
    ];
    let mut model = Model::new();
    for (statements, expected, operation) in cases {
        sql.batch_execute(&statements).await?;
        let transaction = timeout(Duration::from_secs(20), received.recv())
            .await
            .context("waiting for source transaction")?
            .context("listener stopped before transaction")?;
        ensure!(transaction.changes.len() == expected, "unexpected transaction size");
        ensure!(
            transaction
                .changes
                .iter()
                .all(|change| change.operation == operation && change.schema == *schema),
            "unexpected operation or source schema"
        );
        apply(&mut model, &transaction)?;
        ensure!(
            model == source_state(sql, schema).await?,
            "reconstructed CDC state differs from PostgreSQL"
        );
        let current: String = sql
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
                &[&fixture.slot],
            )
            .await?
            .get(0);
        ensure!(initial == current, "diagnostic listener advanced durable source progress");
    }
    Ok(())
}

fn row_key(table: &str, row: &Row) -> Result<(String, String)> {
    let id = row.get("id").and_then(Option::as_ref).context("missing fixture primary key")?;
    Ok((table.to_owned(), id.clone()))
}

fn apply(model: &mut Model, transaction: &Transaction) -> Result<()> {
    for change in &transaction.changes {
        if let Some(old) = &change.old {
            let removed = model.remove(&row_key(&change.table, old)?);
            ensure!(removed.as_ref() == Some(old), "old row does not match reconstructed state");
        }
        if let Some(new) = &change.new {
            ensure!(
                model.insert(row_key(&change.table, new)?, new.clone()).is_none(),
                "duplicate fixture row"
            );
        }
    }
    Ok(())
}

async fn source_state(sql: &Client, schema: &str) -> Result<Model> {
    let mut state = Model::new();
    for (table, columns) in [
        ("person", "'id',id::text,'name',name"),
        ("auction", "'id',id::text,'seller',seller::text,'category',category::text"),
        ("bid", "'id',id::text,'auction',auction::text,'bidder',bidder::text,'price',price::text"),
    ] {
        for row in sql
            .query(&format!("SELECT jsonb_build_object({columns}) FROM {schema}.{table}"), &[])
            .await?
        {
            let value: serde_json::Value = row.get(0);
            let decoded: Row = serde_json::from_value(value)?;
            state.insert(row_key(table, &decoded)?, decoded);
        }
    }
    Ok(state)
}
