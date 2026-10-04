//! `PostgreSQL` coarse checkpoint catalog; sink/progress publication follows later.
mod read;
mod write;

use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Result, ensure};
use tokio_postgres::Client;

/// Catalog bound to an owned `PostgreSQL` schema and a stable query name.
#[derive(Debug, Clone)]
pub struct Catalog {
    schema: String,
    query: String,
}
/// One authoritative metadata checkpoint and its optimistic publication epoch.
#[derive(Debug, Clone)]
pub struct Stored {
    /// Catalog generation used to reject stale checkpoint writers.
    pub epoch: u64,
    /// Complete registered arrangement membership.
    pub checkpoint: Checkpoint,
}
impl Catalog {
    /// Bind to an existing owned schema; query names are SQL values, not identifiers.
    ///
    /// # Errors
    /// Rejects unsafe schema identifiers or empty/oversized query names.
    pub fn new(schema: &str, query: &str) -> Result<Self> {
        ensure!(crate::configuration::identifier(schema), "invalid catalog schema identifier");
        ensure!(!query.is_empty() && query.len() <= 200, "invalid catalog query name");
        Ok(Self { schema: schema.into(), query: query.into() })
    }
    /// Create catalog tables in the caller's existing owned schema.
    /// No source/application schema or data is reset.
    ///
    /// # Errors
    /// Returns `PostgreSQL` DDL, permission, or schema compatibility failures.
    pub async fn install(&self, sql: &Client) -> Result<()> {
        sql.batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {0}.pgderive_queries (
                query_id text PRIMARY KEY, format_version integer NOT NULL CHECK(format_version=1),
                plan_identity text NOT NULL, definition jsonb NOT NULL,
                logical_time bigint NOT NULL CHECK(logical_time>=0),
                epoch bigint NOT NULL CHECK(epoch>0));
             CREATE TABLE IF NOT EXISTS {0}.pgderive_arrangements (
                query_id text NOT NULL REFERENCES {0}.pgderive_queries(query_id) ON DELETE CASCADE,
                arrangement_id text NOT NULL, schema_id text NOT NULL,
                logical_time bigint NOT NULL CHECK(logical_time>=0),
                generation bigint NOT NULL CHECK(generation>=logical_time),
                object_count bigint NOT NULL CHECK(object_count>=0), membership_digest text NOT NULL,
                PRIMARY KEY(query_id,arrangement_id));
             CREATE TABLE IF NOT EXISTS {0}.pgderive_objects (
                query_id text NOT NULL, arrangement_id text NOT NULL,
                ordinal bigint NOT NULL CHECK(ordinal>=0), reference jsonb NOT NULL,
                PRIMARY KEY(query_id,arrangement_id,ordinal),
                FOREIGN KEY(query_id,arrangement_id) REFERENCES {0}.pgderive_arrangements(query_id,arrangement_id) ON DELETE CASCADE);",
            self.schema
        )).await?;
        Ok(())
    }
    /// Atomically store a metadata-only snapshot with optimistic epoch fencing.
    /// Epoch zero requires a new query; later epochs require exact current identity.
    /// This does not apply destination DML, record source progress, or authorize ACK.
    ///
    /// # Errors
    /// Returns incompatible state, stale writer, or `PostgreSQL` failures. An uncertain
    /// COMMIT requires reloading the authoritative catalog before choosing a retry.
    pub async fn checkpoint(
        &self,
        sql: &mut Client,
        plan: &Plan,
        checkpoint: &Checkpoint,
        expected_epoch: u64,
    ) -> Result<u64> {
        checkpoint.validate(plan)?;
        write::save(self, sql, plan, checkpoint, expected_epoch).await
    }
    /// Read one consistent checkpoint, checking complete plan/schema membership.
    ///
    /// # Errors
    /// Returns incompatible/corrupt metadata or `PostgreSQL` read failures.
    pub async fn load(&self, sql: &mut Client, plan: &Plan) -> Result<Option<Stored>> {
        read::load(self, sql, plan).await
    }
}
