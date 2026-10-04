use super::Catalog;
use crate::engine::plan::{Checkpoint, Plan};
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, Transaction};

pub(super) async fn save(
    catalog: &Catalog,
    sql: &mut Client,
    plan: &Plan,
    checkpoint: &Checkpoint,
    expected: u64,
) -> Result<u64> {
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    let epoch = stage(&tx, catalog, plan, checkpoint, expected).await?;
    let active = tx
        .query_opt(
            &format!("SELECT query_id FROM {}.pgderive_progress WHERE query_id=$1", catalog.schema),
            &[&catalog.query],
        )
        .await?;
    ensure!(active.is_none(), "metadata checkpoint cannot overwrite a publication query");
    tx.commit()
        .await
        .context("catalog checkpoint COMMIT outcome requires authoritative reload on failure")?;
    Ok(epoch)
}

/// Stage membership in the caller's transaction so sink/progress can share COMMIT.
pub(super) async fn stage(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    plan: &Plan,
    checkpoint: &Checkpoint,
    expected: u64,
) -> Result<u64> {
    let epoch = expected.checked_add(1).context("catalog epoch overflow")?;
    let next = i64::try_from(epoch)?;
    let expected = i64::try_from(expected)?;
    let time = i64::try_from(checkpoint.time)?;
    let definition = serde_json::to_value(plan.definition())?;
    let affected = if expected == 0 {
        tx.execute(&format!("INSERT INTO {}.pgderive_queries(query_id,format_version,plan_identity,definition,logical_time,epoch)
            VALUES($1,1,$2,$3,$4,$5) ON CONFLICT(query_id) DO NOTHING", catalog.schema),
            &[&catalog.query, &plan.identity(), &definition, &time, &next]).await?
    } else {
        tx.execute(&format!("UPDATE {}.pgderive_queries SET logical_time=$4,epoch=$5
            WHERE query_id=$1 AND plan_identity=$2 AND definition=$3 AND epoch=$6 AND logical_time<=$4 AND format_version=1", catalog.schema),
            &[&catalog.query, &plan.identity(), &definition, &time, &next, &expected]).await?
    };
    ensure!(affected == 1, "stale or incompatible catalog checkpoint writer");
    tx.execute(
        &format!("DELETE FROM {}.pgderive_arrangements WHERE query_id=$1", catalog.schema),
        &[&catalog.query],
    )
    .await?;
    membership(tx, catalog, checkpoint).await?;
    Ok(epoch)
}
async fn membership(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    checkpoint: &Checkpoint,
) -> Result<()> {
    for member in &checkpoint.arrangements {
        let time = i64::try_from(member.trace.time)?;
        let generation = i64::try_from(member.trace.generation)?;
        let count = i64::try_from(member.trace.objects.len())?;
        let digest = member.digest()?;
        tx.execute(&format!("INSERT INTO {}.pgderive_arrangements(query_id,arrangement_id,schema_id,logical_time,generation,object_count,membership_digest) VALUES($1,$2,$3,$4,$5,$6,$7)", catalog.schema),
            &[&catalog.query, &member.id, &member.schema, &time, &generation, &count, &digest]).await?;
        for (ordinal, object) in member.trace.objects.iter().enumerate() {
            let ordinal = i64::try_from(ordinal)?;
            let reference = serde_json::to_value(object)?;
            tx.execute(&format!("INSERT INTO {}.pgderive_objects(query_id,arrangement_id,ordinal,reference) VALUES($1,$2,$3,$4)", catalog.schema),
                &[&catalog.query, &member.id, &ordinal, &reference]).await?;
        }
    }
    Ok(())
}
