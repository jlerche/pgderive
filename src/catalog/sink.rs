use super::Catalog;
use crate::engine::{Batch, reader::BatchData};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
mod batching;
mod layout;
mod lookup;
mod rows;
pub use rows::ResultColumn;
use tokio_postgres::Transaction;

/// Owned destination containing physical result occurrences, never persisted weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sink {
    /// Owned ordinary typed result table.
    pub table: String,
    /// Native output metadata.
    pub columns: Vec<ResultColumn>,
}
impl Sink {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            crate::configuration::identifier(self.table.as_str())
                && !self.table.as_str().starts_with("pgderive_"),
            "invalid or reserved sink table"
        );
        rows::validate(&self.columns)
    }
    pub(super) async fn install(&self, tx: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
        if self.owned(tx, catalog).await?.is_some() {
            return self.prepare_index(tx, catalog).await;
        }
        let columns = rows::declaration(&self.columns)?;
        tx.batch_execute(&format!(
            "CREATE TABLE {}.{} ({columns})",
            catalog.schema,
            self.table.as_str()
        ))
        .await?;
        tx.batch_execute(&format!(
            "ALTER TABLE {}.{} REPLICA IDENTITY FULL",
            catalog.schema,
            self.table.as_str()
        ))
        .await?;
        let table = format!("{}.{}", catalog.schema, self.table.as_str());
        let oid: u32 =
            tx.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&table]).await?.try_get(0)?;
        tx.execute(
            &format!("INSERT INTO {}.pgderive_sink_ownership VALUES($1,$2,$3)", catalog.schema),
            &[&self.table.as_str(), &catalog.query, &oid],
        )
        .await?;
        self.prepare_index(tx, catalog).await
    }
    pub(super) async fn prepare_index(
        &self,
        tx: &Transaction<'_>,
        catalog: &Catalog,
    ) -> Result<()> {
        lookup::ensure(tx, catalog, self).await
    }
    pub(crate) async fn owned(
        &self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
        catalog: &Catalog,
    ) -> Result<Option<u32>> {
        let Some(row) = sql.query_opt(&format!("SELECT query_id,relation_oid FROM {}.pgderive_sink_ownership WHERE sink_table=$1",catalog.schema), &[&self.table.as_str()]).await? else { return Ok(None); };
        ensure!(
            row.try_get::<_, String>(0)? == catalog.query,
            "destination belongs to another query"
        );
        let oid: u32 = row.try_get(1)?;
        let table = format!("{}.{}", catalog.schema, self.table.as_str());
        let actual: Option<u32> =
            sql.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&table]).await?.try_get(0)?;
        ensure!(actual == Some(oid), "owned destination was removed or replaced");
        rows::verify(sql, self, oid).await?;
        Ok(Some(oid))
    }
}
/// Immutable destination mutations encoded from a complete canonical edge batch.
/// Construct these from the exact preparation whose memberships are published.
pub struct Deltas {
    pub(super) rows: Vec<(Value, i64)>,
}
impl Deltas {
    /// Preserve full `(key,value)` weighted identity in the destination bag.
    ///
    /// # Errors
    /// Returns a JSON codec failure.
    pub fn bag<K: BatchData, V: BatchData>(batch: &Batch<K, V>) -> Result<Self> {
        let rows = batch
            .iter()
            .map(|(tuple, weight)| Ok((serde_json::to_value(tuple)?, *weight)))
            .collect::<Result<_>>()?;
        Ok(Self { rows })
    }
    pub(super) async fn apply(
        &self,
        tx: &Transaction<'_>,
        catalog: &Catalog,
        sink: &Sink,
    ) -> Result<()> {
        let table = format!("{}.{}", catalog.schema, sink.table.as_str());
        batching::apply(tx, &table, sink, &self.rows).await
    }
}
