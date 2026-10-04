use super::Catalog;
use crate::engine::{Batch, plan::query::AggregateRow, reader::BatchData};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio_postgres::Transaction;

/// Initial explicit destination encoding; SQL lowering will select native columns.
/// The registered JSON codec must preserve full tuple identity under JSONB equality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Sink {
    /// One full encoded tuple and its nonzero signed bag coefficient.
    Bag(String),
    /// One encoded group, COUNT(*) and nullable SUM, with unit row multiplicity.
    Grouped(String),
}
impl Sink {
    pub(super) fn table(&self) -> &str {
        match self {
            Self::Bag(table) | Self::Grouped(table) => table,
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            crate::configuration::identifier(self.table())
                && !self.table().starts_with("pgderive_"),
            "invalid or reserved sink table"
        );
        Ok(())
    }
    pub(super) async fn install(&self, tx: &Transaction<'_>, catalog: &Catalog) -> Result<()> {
        let columns = match self {
            Self::Bag(_) => "tuple jsonb PRIMARY KEY, weight bigint NOT NULL CHECK(weight<>0)",
            Self::Grouped(_) => {
                "group_key jsonb PRIMARY KEY, row_count bigint NOT NULL CHECK(row_count>0), total bigint"
            }
        };
        tx.batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({columns})",
            catalog.schema,
            self.table()
        ))
        .await?;
        Ok(())
    }
}
/// Immutable destination mutations encoded from a complete canonical edge batch.
/// Construct these from the exact preparation whose memberships are published.
pub struct Deltas {
    pub(super) rows: Rows,
}
pub(super) enum Rows {
    Bag(Vec<(Value, i64)>),
    Grouped(Vec<GroupChange>),
}
pub(super) struct GroupChange {
    key: Value,
    before: Option<AggregateRow>,
    after: Option<AggregateRow>,
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
        Ok(Self { rows: Rows::Bag(rows) })
    }
    /// Encode exact old/new grouped rows; NULL SUM and absent groups stay distinct.
    ///
    /// # Errors
    /// Rejects non-unit coefficients, invalid counts, or multiple old/new rows.
    pub fn grouped<G: BatchData>(batch: &Batch<G, AggregateRow>) -> Result<Self> {
        let mut groups = BTreeMap::<G, GroupChange>::new();
        for ((key, value), weight) in batch.iter() {
            ensure!(
                value.0 > 0 && (*weight == -1 || *weight == 1),
                "invalid grouped sink row coefficient/count"
            );
            let change = groups.entry(key.clone()).or_insert(GroupChange {
                key: serde_json::to_value(key)?,
                before: None,
                after: None,
            });
            let target = if *weight == -1 { &mut change.before } else { &mut change.after };
            ensure!(target.replace(*value).is_none(), "multiple grouped sink rows for one group");
        }
        Ok(Self { rows: Rows::Grouped(groups.into_values().collect()) })
    }
    pub(super) async fn apply(
        &self,
        tx: &Transaction<'_>,
        catalog: &Catalog,
        sink: &Sink,
    ) -> Result<()> {
        let table = format!("{}.{}", catalog.schema, sink.table());
        match (&self.rows, sink) {
            (Rows::Bag(rows), Sink::Bag(_)) => {
                for (tuple, delta) in rows {
                    apply_bag(tx, &table, tuple, *delta).await?;
                }
            }
            (Rows::Grouped(rows), Sink::Grouped(_)) => {
                for change in rows {
                    apply_group(tx, &table, change).await?;
                }
            }
            _ => anyhow::bail!("destination encoding does not match registered sink"),
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
async fn apply_group(tx: &Transaction<'_>, table: &str, change: &GroupChange) -> Result<()> {
    let row = tx
        .query_opt(
            &format!("SELECT row_count,total FROM {table} WHERE group_key=$1 FOR UPDATE"),
            &[&change.key],
        )
        .await?;
    let prior = row
        .map(|row| -> Result<_> {
            Ok((row.try_get::<_, i64>(0)?, row.try_get::<_, Option<i64>>(1)?))
        })
        .transpose()?;
    ensure!(prior == change.before, "destination grouped row does not match expected prior state");
    let affected = match (change.before, change.after) {
        (None, Some((count, sum))) => {
            tx.execute(
                &format!("INSERT INTO {table}(group_key,row_count,total) VALUES($1,$2,$3)"),
                &[&change.key, &count, &sum],
            )
            .await?
        }
        (Some(_), Some((count, sum))) => {
            tx.execute(
                &format!("UPDATE {table} SET row_count=$2,total=$3 WHERE group_key=$1"),
                &[&change.key, &count, &sum],
            )
            .await?
        }
        (Some(_), None) => {
            tx.execute(&format!("DELETE FROM {table} WHERE group_key=$1"), &[&change.key]).await?
        }
        (None, None) => anyhow::bail!("empty grouped sink mutation"),
    };
    ensure!(affected == 1, "destination grouped DML was suppressed");
    Ok(())
}
