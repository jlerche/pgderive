use super::Catalog;
use crate::engine::{Batch, reader::BatchData};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use tokio_postgres::Transaction;

/// Owned full-tuple weighted-bag destination encoding.
/// The registered JSON codec must preserve full tuple identity under JSONB equality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Sink {
    /// One full encoded tuple and its nonzero signed bag coefficient.
    Bag(String),
    /// Pure scalar map over the persisted raw bag, evaluated atomically at publication.
    MappedBag {
        /// Owned destination table.
        table: String,
        /// Bound built-in function signatures and environment.
        map: super::Terminal,
    },
}
impl Sink {
    pub(crate) fn table(&self) -> &str {
        match self {
            Self::Bag(table) | Self::MappedBag { table, .. } => table,
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            crate::configuration::identifier(self.table())
                && !self.table().starts_with("pgderive_"),
            "invalid or reserved sink table"
        );
        if let Self::MappedBag { map, .. } = self {
            map.validate()?;
        }
        Ok(())
    }
    pub(super) async fn install(&self, tx: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
        if self.owned(tx, catalog).await?.is_some() {
            return Ok(());
        }
        let columns = "tuple jsonb PRIMARY KEY, weight bigint NOT NULL CHECK(weight<>0)";
        tx.batch_execute(&format!("CREATE TABLE {}.{} ({columns})", catalog.schema, self.table()))
            .await?;
        let table = format!("{}.{}", catalog.schema, self.table());
        let oid: u32 =
            tx.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&table]).await?.try_get(0)?;
        tx.execute(
            &format!("INSERT INTO {}.pgderive_sink_ownership VALUES($1,$2,$3)", catalog.schema),
            &[&self.table(), &catalog.query, &oid],
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn owned(
        &self,
        sql: &(impl tokio_postgres::GenericClient + Sync),
        catalog: &Catalog,
    ) -> Result<Option<u32>> {
        let Some(row) = sql.query_opt(&format!("SELECT query_id,relation_oid FROM {}.pgderive_sink_ownership WHERE sink_table=$1",catalog.schema), &[&self.table()]).await? else { return Ok(None); };
        ensure!(
            row.try_get::<_, String>(0)? == catalog.query,
            "destination belongs to another query"
        );
        let oid: u32 = row.try_get(1)?;
        let table = format!("{}.{}", catalog.schema, self.table());
        let actual: Option<u32> =
            sql.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&table]).await?.try_get(0)?;
        ensure!(actual == Some(oid), "owned destination was removed or replaced");
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
        let table = format!("{}.{}", catalog.schema, sink.table());
        let rows: Cow<'_, [(Value, i64)]> = match sink {
            Sink::Bag(_) => Cow::Borrowed(&self.rows),
            Sink::MappedBag { map, .. } => Cow::Owned(map.evaluate(tx, &self.rows).await?),
        };
        for (tuple, delta) in rows.iter() {
            apply_bag(tx, &table, tuple, *delta).await?;
        }
        Ok(())
    }
}
async fn apply_bag(tx: &Transaction<'_>, table: &str, tuple: &Value, delta: i64) -> Result<()> {
    let row = tx
        .query_opt(&format!("SELECT weight FROM {table} WHERE tuple=$1 FOR UPDATE"), &[tuple])
        .await?;
    let prior = row.as_ref().map(|row| row.try_get::<_, i64>(0)).transpose()?.unwrap_or_default();
    let next = prior.checked_add(delta).context("destination bag coefficient overflow")?;
    let affected = if next == 0 {
        tx.execute(&format!("DELETE FROM {table} WHERE tuple=$1"), &[tuple]).await?
    } else if row.is_some() {
        tx.execute(&format!("UPDATE {table} SET weight=$2 WHERE tuple=$1"), &[tuple, &next]).await?
    } else {
        tx.execute(&format!("INSERT INTO {table}(tuple,weight) VALUES($1,$2)"), &[tuple, &next])
            .await?
    };
    ensure!(affected == 1, "destination bag DML was suppressed");
    Ok(())
}
