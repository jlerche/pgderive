use super::{Catalog, Stored};
use crate::engine::{
    plan::{Checkpoint, Membership, Plan},
    trace::Manifest,
};
use anyhow::{Result, ensure};
use tokio_postgres::{Client, IsolationLevel, Transaction};

pub(super) async fn load(
    catalog: &Catalog,
    sql: &mut Client,
    plan: &Plan,
) -> Result<Option<Stored>> {
    let tx = sql
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let stored = boundary(&tx, catalog, plan).await?;
    tx.commit().await?;
    Ok(stored)
}
pub(super) async fn boundary(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    plan: &Plan,
) -> Result<Option<Stored>> {
    let row = tx.query_opt(&format!("SELECT format_version,plan_identity,definition,logical_time,epoch FROM {}.pgderive_queries WHERE query_id=$1", catalog.schema), &[&catalog.query]).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    ensure!(row.try_get::<_, i32>(0)? == 1, "unsupported catalog query format");
    let identity: String = row.try_get(1)?;
    let definition: serde_json::Value = row.try_get(2)?;
    let registered = Plan::new(serde_json::from_value(definition)?)?;
    ensure!(
        registered.identity() == identity && identity == plan.identity(),
        "catalog registered plan mismatch"
    );
    let time = u64::try_from(row.try_get::<_, i64>(3)?)?;
    let epoch = u64::try_from(row.try_get::<_, i64>(4)?)?;
    ensure!(epoch > 0, "invalid catalog epoch");
    let arrangements = membership(tx, catalog).await?;
    let checkpoint = Checkpoint { version: 1, plan_identity: identity, time, arrangements };
    checkpoint.validate(plan)?;
    Ok(Some(Stored { epoch, checkpoint }))
}
async fn membership(tx: &Transaction<'_>, catalog: &Catalog) -> Result<Vec<Membership>> {
    let rows = tx.query(&format!("SELECT arrangement_id,schema_id,logical_time,generation,object_count,membership_digest FROM {}.pgderive_arrangements WHERE query_id=$1 ORDER BY arrangement_id", catalog.schema), &[&catalog.query]).await?;
    let mut members = Vec::new();
    for row in rows {
        let id: String = row.try_get(0)?;
        let schema: String = row.try_get(1)?;
        let time = u64::try_from(row.try_get::<_, i64>(2)?)?;
        let generation = u64::try_from(row.try_get::<_, i64>(3)?)?;
        let rows = tx.query(&format!("SELECT ordinal,reference FROM {}.pgderive_objects WHERE query_id=$1 AND arrangement_id=$2 ORDER BY ordinal", catalog.schema), &[&catalog.query, &id]).await?;
        let mut objects = Vec::new();
        for (ordinal, object) in rows.into_iter().enumerate() {
            ensure!(
                object.try_get::<_, i64>(0)? == i64::try_from(ordinal)?,
                "incomplete catalog object ordinals"
            );
            let reference: serde_json::Value = object.try_get(1)?;
            objects.push(serde_json::from_value(reference)?);
        }
        ensure!(
            objects.len() == usize::try_from(row.try_get::<_, i64>(4)?)?,
            "incomplete catalog object membership"
        );
        let member =
            Membership { id, schema, trace: Manifest { version: 1, time, generation, objects } };
        ensure!(
            member.digest()? == row.try_get::<_, String>(5)?,
            "catalog membership checksum mismatch"
        );
        members.push(member);
    }
    Ok(members)
}
