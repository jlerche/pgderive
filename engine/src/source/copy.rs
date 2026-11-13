use super::{Contract, Relation};
use crate::{
    engine::execution::{CapacityExceeded, Limits},
    transaction::Row,
    weighted::{Batch, Tuple, Update},
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt as _;
use std::{collections::BTreeSet, pin::Pin};
use tokio_postgres::{RowStream, Transaction};

/// Bounded batches from one imported snapshot. PK uniqueness makes every native
/// row one positive full-tuple contribution; no global source consolidation is needed.
pub struct SnapshotReader {
    contract: Contract,
    limits: Limits,
    columns: Vec<Vec<super::Column>>,
    relation: usize,
    rows: Option<Pin<Box<RowStream>>>,
    pending: Option<Update>,
}
impl SnapshotReader {
    /// Validate the imported contract and freeze snapshot codec settings.
    ///
    /// # Errors
    /// Returns contract, codec, resource or `PostgreSQL` errors.
    pub async fn new(
        sql: &Transaction<'_>,
        contract: &Contract,
        projection: &[BTreeSet<String>],
        limits: Limits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        sql.batch_execute("SET LOCAL row_security=off; SET LOCAL search_path=pg_catalog; SET LOCAL DateStyle='ISO,MDY'; SET LOCAL TimeZone='UTC'; SET LOCAL IntervalStyle='iso_8601'; SET LOCAL extra_float_digits=3").await?;
        contract.validate()?;
        contract.verify(sql).await?;
        ensure!(projection.len() == contract.relations.len(), "snapshot projection count mismatch");
        let columns = contract
            .relations
            .iter()
            .zip(projection)
            .map(|(relation, selected)| {
                ensure!(
                    selected.iter().all(|name| relation.columns.iter().any(|c| c.name == *name)),
                    "unknown snapshot column"
                );
                Ok(relation
                    .columns
                    .iter()
                    .filter(|c| !selected.is_empty() && (c.primary || selected.contains(&c.name)))
                    .cloned()
                    .collect())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            contract: contract.clone(),
            limits,
            columns,
            relation: 0,
            rows: None,
            pending: None,
        })
    }
    /// Read at most one resident-budget batch, retaining only one pending row.
    /// The protocol row stream applies backpressure without a FETCH utility tuplestore.
    ///
    /// # Errors
    /// Rejects oversized rows, invalid PKs, codecs or SQL failures.
    pub async fn next(&mut self, sql: &Transaction<'_>) -> Result<Option<Batch>> {
        let mut batch = Batch { updates: Vec::new() };
        let mut bytes = 0_usize;
        let entries = self
            .limits
            .resident_entries
            .min(self.limits.output_entries)
            .min(self.limits.contributions);
        let budget = u64::try_from(self.limits.resident_bytes)?.min(self.limits.output_bytes);
        while batch.updates.len() < entries {
            let update = if let Some(update) = self.pending.take() {
                update
            } else {
                let Some(update) = self.read(sql).await? else { break };
                update
            };
            let size =
                crate::engine::execution::record_size(&update.tuple, self.limits.record_bytes)?;
            ensure!(
                u64::try_from(size)? <= budget,
                CapacityExceeded("snapshot row exceeds batch byte limit")
            );
            if u64::try_from(bytes.checked_add(size).context("snapshot byte count overflow")?)?
                > budget
            {
                self.pending = Some(update);
                break;
            }
            bytes += size;
            batch.updates.push(update);
        }
        Ok((!batch.updates.is_empty()).then_some(batch))
    }
    async fn read(&mut self, sql: &Transaction<'_>) -> Result<Option<Update>> {
        while let Some(relation) = self.contract.relations.get(self.relation) {
            if self.columns[self.relation].is_empty() {
                self.relation += 1;
                continue;
            }
            if self.rows.is_none() {
                self.rows = Some(Box::pin(
                    open(sql, relation, &self.columns[self.relation], self.limits.record_bytes)
                        .await?,
                ));
            }
            let stream = self.rows.as_mut().context("snapshot cursor missing")?;
            let Some(row) = stream.try_next().await? else {
                self.rows = None;
                self.relation += 1;
                continue;
            };
            ensure!(
                row.try_get::<_, bool>(0)?,
                CapacityExceeded("source snapshot row byte limit exceeded")
            );
            let row: Row = self.columns[self.relation]
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    Ok((column.name.clone(), row.try_get::<_, Option<String>>(index + 1)?))
                })
                .collect::<Result<std::collections::BTreeMap<_, _>>>()?
                .into();
            ensure!(
                relation
                    .columns
                    .iter()
                    .all(|column| !column.primary
                        || row.get(&column.name).is_some_and(Option::is_some)),
                "source snapshot has null primary key"
            );
            return Ok(Some(Update {
                tuple: Tuple {
                    schema: relation.schema.clone(),
                    table: relation.table.clone(),
                    row,
                },
                weight: 1,
            }));
        }
        Ok(None)
    }
}
async fn open(
    sql: &Transaction<'_>,
    relation: &Relation,
    columns: &[super::Column],
    limit: usize,
) -> Result<RowStream> {
    let expressions = columns
        .iter()
        .map(|column| {
            let name = super::contract::quote(&column.name);
            if column.oid == 16 {
                format!("CASE WHEN {name} IS NULL THEN NULL WHEN {name} THEN 't' ELSE 'f' END")
            } else {
                format!("{name}::pg_catalog.text")
            }
        })
        .collect::<Vec<_>>();
    let aliases = (0..expressions.len()).map(|index| format!("v{index}")).collect::<Vec<_>>();
    let fields = expressions
        .iter()
        .zip(&aliases)
        .map(|(expr, name)| format!("{expr} AS {name}"))
        .collect::<Vec<_>>()
        .join(",");
    let size = aliases
        .iter()
        .map(|name| format!("COALESCE(pg_catalog.octet_length({name})::bigint,0)"))
        .collect::<Vec<_>>()
        .join("+");
    let values = aliases
        .iter()
        .map(|name| format!("CASE WHEN valid THEN {name} ELSE NULL END"))
        .collect::<Vec<_>>()
        .join(",");
    // OFFSET 0 is a pipelined subquery boundary: prevent target-list pull-up
    // from repeating every cast and the full size sum once per output column.
    let query = format!(
        "SELECT valid,{values} FROM (SELECT projected.*,({size})<={limit} AS valid FROM (SELECT {fields} FROM {}.{} OFFSET 0) projected OFFSET 0) guarded",
        super::contract::quote(&relation.schema),
        super::contract::quote(&relation.table)
    );
    sql.query_raw(&query, std::iter::empty::<&str>()).await.map_err(Into::into)
}
