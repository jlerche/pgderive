//! `PostgreSQL` coarse checkpoint catalog and atomic sink/progress publication.
mod bootstrap;
pub use bootstrap::Snapshot;
pub(crate) mod epoch;
mod maintenance;
mod memtable;
mod memtable_guard;
mod position;
mod publication;
mod read;
mod recovery;
pub use recovery::{Durable, Resolution};
mod storage;
pub(crate) use storage::validate_additions;
pub use storage::{Collection, GcCache, GcLimits, Protection};
mod sink;
mod write;
pub use position::{Lsn, Progress};
pub use publication::{Binding, Publication, Writer};
pub use sink::{Deltas, ResultColumn, Sink, SinkOutput};
#[cfg(test)]
mod tests;

use crate::engine::plan::Checkpoint;
use anyhow::{Context as _, Result, ensure};
use tokio_postgres::Client;

/// Catalog bound to an owned `PostgreSQL` schema and a stable query name.
#[derive(Debug, Clone)]
pub struct Catalog {
    schema: String,
    query: String,
}
/// One authoritative metadata checkpoint and its optimistic publication epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// Catalog generation used to reject stale checkpoint writers.
    pub epoch: u64,
    /// Complete registered arrangement membership.
    pub checkpoint: Checkpoint,
}
impl Catalog {
    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

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
            "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('pgderive-catalog-install:{0}',0));
             CREATE TABLE IF NOT EXISTS {0}.pgderive_queries (
                query_id text PRIMARY KEY, format_version integer NOT NULL CONSTRAINT pgderive_queries_memtable_format_check CHECK(format_version=3),
                plan_identity text NOT NULL, definition jsonb NOT NULL,
                logical_time bigint NOT NULL CHECK(logical_time>=0),
                epoch bigint NOT NULL CHECK(epoch>0),
                checkpoint jsonb NOT NULL,checkpoint_digest text NOT NULL);
             CREATE OR REPLACE VIEW {0}.pgderive_roots AS
                SELECT q.query_id,m.value->>'id' AS arrangement_id,r.ordinality-1 AS ordinal,r.value AS reference
                FROM {0}.pgderive_queries q
                CROSS JOIN LATERAL jsonb_array_elements(q.checkpoint->'arrangements') m
                CROSS JOIN LATERAL jsonb_array_elements(m.value->'trace'->'objects') WITH ORDINALITY r(value,ordinality);
             CREATE TABLE IF NOT EXISTS {0}.pgderive_source_slots (
                source_key text PRIMARY KEY, query_id text NOT NULL UNIQUE REFERENCES {0}.pgderive_queries(query_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);
             CREATE TABLE IF NOT EXISTS {0}.pgderive_protections (
                token text PRIMARY KEY, id bigint GENERATED ALWAYS AS IDENTITY UNIQUE, namespace text UNIQUE, active boolean NOT NULL DEFAULT true, query_id text NOT NULL, owner_fence bigint NOT NULL CHECK(owner_fence>=0), uploading boolean NOT NULL, recovering boolean NOT NULL DEFAULT false, quiescent boolean NOT NULL DEFAULT false, gc_candidate bigint NOT NULL DEFAULT 0, gc_seen bigint NOT NULL DEFAULT 0, roots jsonb NOT NULL CHECK(jsonb_typeof(roots)='array'));
             CREATE TABLE IF NOT EXISTS {0}.pgderive_gc (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), generation bigint NOT NULL CHECK(generation>=0), cursor text CHECK(octet_length(cursor)<=16384), in_progress boolean NOT NULL);
             INSERT INTO {0}.pgderive_gc(singleton,generation,cursor,in_progress) VALUES(true,0,NULL,false) ON CONFLICT DO NOTHING;
             CREATE TABLE IF NOT EXISTS {0}.pgderive_progress (
                query_id text PRIMARY KEY REFERENCES {0}.pgderive_queries(query_id) ON DELETE CASCADE,
                binding jsonb NOT NULL, sink_table text NOT NULL UNIQUE,
                fence bigint NOT NULL CHECK(fence>0), commit_lsn pg_lsn NOT NULL,
                end_lsn pg_lsn NOT NULL CHECK(end_lsn>=commit_lsn),
                xid bigint CHECK(xid>=0 AND xid<=4294967295),
                checkpoint_lsn pg_lsn NOT NULL,published_time bigint NOT NULL);
             CREATE TABLE IF NOT EXISTS {0}.pgderive_sink_ownership (
                sink_table text PRIMARY KEY,query_id text NOT NULL UNIQUE
                REFERENCES {0}.pgderive_queries(query_id) ON DELETE CASCADE,
                relation_oid oid NOT NULL);
             CREATE TABLE IF NOT EXISTS {0}.pgderive_epoch_boundaries (
                query_id text NOT NULL REFERENCES {0}.pgderive_queries(query_id) ON DELETE CASCADE,
                logical_time bigint NOT NULL CHECK(logical_time>1),
                previous_end_lsn pg_lsn NOT NULL, commit_lsn pg_lsn NOT NULL,
                end_lsn pg_lsn NOT NULL CHECK(end_lsn>=commit_lsn AND commit_lsn>=previous_end_lsn),
                xid bigint NOT NULL CHECK(xid>=0 AND xid<=4294967295),
                transactions bigint NOT NULL CHECK(transactions>0),
                PRIMARY KEY(query_id,logical_time));",
            self.schema
        )).await?;
        sql.query(&format!("SELECT gc_candidate,gc_seen FROM {}.pgderive_protections LIMIT 0", self.schema), &[]).await.context("incompatible GC catalog layout; rebuild this development catalog before starting this version")?;
        Ok(())
    }
}

fn checkpoint_digest(checkpoint: &Checkpoint) -> Result<String> {
    use sha2::{Digest, Sha256};
    for member in &checkpoint.arrangements {
        i64::try_from(member.trace.generation)?;
        i64::try_from(member.trace.objects.len())?;
    }
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(checkpoint)?)))
}
