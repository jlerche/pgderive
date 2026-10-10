use super::{Contract, Relation};
use crate::{
    engine::execution::{Consolidator, Limits},
    transaction::Row,
    weighted::{Batch, Tuple, Update},
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Transaction;

/// Copy complete native rows through a bounded cursor from the imported snapshot.
/// Full tuple identity is retained; primary keys are only source contract validation.
///
/// # Errors
/// Rejects oversized rows/output/scratch, unsupported contracts, schema drift or SQL errors.
pub async fn copy(
    snapshot: &Transaction<'_>,
    contract: &Contract,
    limits: Limits,
) -> Result<Batch> {
    snapshot.batch_execute("SET LOCAL row_security=off; SET LOCAL DateStyle='ISO,MDY'; SET LOCAL TimeZone='UTC'; SET LOCAL IntervalStyle='iso_8601'").await?;
    contract.validate()?;
    contract.verify(snapshot).await?;
    let mut weights = Consolidator::new(limits)?;
    for relation in &contract.relations {
        read_relation(snapshot, relation, limits.record_bytes, &mut weights).await?;
    }
    let mut updates = Vec::new();
    weights.finish(|tuple, weight| {
        updates.push(Update { tuple, weight: i64::try_from(weight)? });
        Ok(())
    })?;
    Ok(Batch { updates })
}
async fn read_relation(
    sql: &Transaction<'_>,
    relation: &Relation,
    limit: usize,
    weights: &mut Consolidator<Tuple>,
) -> Result<()> {
    let columns = relation
        .columns
        .iter()
        .map(|column| {
            let name = super::contract::quote(&column.name);
            if column.oid == 16 {
                format!(
                    "CASE WHEN {name} IS NULL THEN NULL WHEN {name} THEN 't' ELSE 'f' END AS {name}"
                )
            } else {
                format!("{name}::text AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    // Bound the encoded row on the server before transferring it to the client.
    // A null sentinel makes oversize input fail rather than silently omitting a row.
    sql.batch_execute(&format!("DECLARE pgderive_snapshot_cursor NO SCROLL CURSOR FOR SELECT CASE WHEN octet_length(encoded::text)<={limit} THEN encoded ELSE NULL END FROM (SELECT to_jsonb(source_row) AS encoded FROM (SELECT {columns} FROM {}.{}) AS source_row) AS source_rows", super::contract::quote(&relation.schema),super::contract::quote(&relation.table))).await?;
    loop {
        let rows = sql.query("FETCH FORWARD 64 FROM pgderive_snapshot_cursor", &[]).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let encoded: serde_json::Value = row
                .try_get::<_, Option<serde_json::Value>>(0)?
                .context("source snapshot row byte limit exceeded")?;
            let row: Row = serde_json::from_value(encoded)?;
            ensure!(
                relation
                    .columns
                    .iter()
                    .all(|column| !column.primary
                        || row.get(&column.name).is_some_and(Option::is_some)),
                "source snapshot has null primary key"
            );
            weights.add(
                Tuple { schema: relation.schema.clone(), table: relation.table.clone(), row },
                1,
            )?;
        }
    }
    sql.batch_execute("CLOSE pgderive_snapshot_cursor").await?;
    Ok(())
}
