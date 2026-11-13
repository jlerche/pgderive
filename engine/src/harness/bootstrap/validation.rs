//! The same boundary mutations must fail in physical and full publication paths.
use crate::{
    catalog::{Connection, Publication, Writer},
    engine::plan::Plan,
};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;

pub(super) async fn check(
    sql: &mut Connection,
    schema: &str,
    plan: &Plan,
    writer: &mut Writer,
    publication: Publication<'_>,
) -> Result<()> {
    sql.batch_execute(&format!("CREATE TEMP TABLE validation_query AS SELECT * FROM {schema}.pgderive_queries WHERE query_id='snapshot_grouped';
        CREATE TEMP TABLE validation_progress AS SELECT * FROM {schema}.pgderive_progress WHERE query_id='snapshot_grouped';
        CREATE TEMP TABLE validation_slot AS SELECT * FROM {schema}.pgderive_source_slots WHERE query_id='snapshot_grouped'")) .await?;
    let prior = writer.confirmed(sql, plan).await?;
    settle_ack(sql, &prior).await?;
    for maintenance in [false, true] {
        exercise(sql, (schema, plan), writer, publication, maintenance).await?;
    }
    sql.batch_execute("DROP TABLE validation_query,validation_progress,validation_slot").await?;
    eprintln!(
        "MVP full publication and maintenance rejected shared boundary mutations without sink/progress/ACK changes"
    );
    Ok(())
}
async fn exercise(
    sql: &mut Connection,
    context: (&str, &Plan),
    writer: &mut Writer,
    publication: Publication<'_>,
    maintenance: bool,
) -> Result<()> {
    let (schema, plan) = context;
    let mutations: BTreeMap<String, String> =
        serde_json::from_str(include_str!("../../../scripts/publication_mutations.json"))?;
    let prior = writer.confirmed(sql, plan).await?;
    let initial_audit = audit(sql, schema).await?;
    for (name, mutation) in &mutations {
        sql.batch_execute(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; {} WHERE query_id='snapshot_grouped'; COMMIT",
            mutation.replace("{s}", schema)
        ))
        .await?;
        let changed = metadata(sql, schema).await?;
        let result = if maintenance {
            writer.maintain(sql, plan, &prior.stored.checkpoint).await
        } else {
            writer.publish(sql, plan, publication).await
        };
        rejected(result, name, maintenance)?;
        unchanged(sql, schema, &initial_audit, &changed).await?;
        restore(sql, schema).await?;
        ensure!(
            writer.confirmed(sql, plan).await? == prior,
            "restored validation boundary changed"
        );
    }
    Ok(())
}

async fn settle_ack(sql: &Connection, prior: &crate::catalog::Durable) -> Result<()> {
    // The stream's previous successful acknowledgement is sent asynchronously.
    // Wait for that approved boundary before testing that rejection sends no ACK.
    for _ in 0..100 {
        let row = sql
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
                &[&prior.binding.source.slot],
            )
            .await?;
        let position: crate::catalog::Lsn = row.try_get::<_, String>(0)?.parse()?;
        ensure!(
            position <= prior.checkpoint_end,
            "ACK exceeded durable checkpoint before validation test"
        );
        if position == prior.checkpoint_end {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    anyhow::bail!("previous approved ACK did not settle")
}

async fn unchanged(
    sql: &Connection,
    schema: &str,
    initial_audit: &serde_json::Value,
    changed: &serde_json::Value,
) -> Result<()> {
    ensure!(&audit(sql, schema).await? == initial_audit, "boundary rejection changed sink or ACK");
    ensure!(&metadata(sql, schema).await? == changed, "boundary rejection changed metadata");
    Ok(())
}

fn rejected(result: Result<()>, name: &str, maintenance: bool) -> Result<()> {
    let error = result.err().ok_or_else(|| {
        anyhow::anyhow!("boundary mutation accepted: {name}, maintenance={maintenance}")
    })?;
    let message = format!("{error:#}");
    ensure!(
        message.contains("memtable worker fenced")
            || message.contains("memtable query or progress disappeared"),
        "mutation rejected outside boundary guard: {name}: {message}"
    );
    Ok(())
}

async fn metadata(sql: &Connection, schema: &str) -> Result<serde_json::Value> {
    Ok(sql.query_one(&format!("SELECT jsonb_build_array(
        (SELECT to_jsonb(q) FROM {schema}.pgderive_queries q WHERE query_id='snapshot_grouped'),
        (SELECT to_jsonb(p) FROM {schema}.pgderive_progress p WHERE query_id='snapshot_grouped'),
        (SELECT to_jsonb(s) FROM {schema}.pgderive_source_slots s WHERE query_id='snapshot_grouped'))"), &[]).await?.try_get(0)?)
}
async fn audit(sql: &Connection, schema: &str) -> Result<serde_json::Value> {
    Ok(sql.query_one(&format!("SELECT jsonb_build_array(
        (SELECT jsonb_agg(jsonb_build_array(ctid::text,xmin::text,to_jsonb(s)) ORDER BY ctid) FROM {schema}.snapshot_groups s),
        (SELECT jsonb_agg(jsonb_build_array(slot_name,confirmed_flush_lsn::text) ORDER BY slot_name) FROM pg_replication_slots WHERE slot_name=(SELECT binding->'source'->>'slot' FROM validation_progress)))"), &[]).await?.try_get(0)?)
}
async fn restore(sql: &Connection, schema: &str) -> Result<()> {
    sql.batch_execute(&format!("BEGIN; SET LOCAL synchronous_commit=on; UPDATE {schema}.pgderive_queries q SET
        (format_version,plan_identity,definition,logical_time,epoch,checkpoint,checkpoint_digest)=
        (v.format_version,v.plan_identity,v.definition,v.logical_time,v.epoch,v.checkpoint,v.checkpoint_digest)
        FROM validation_query v WHERE q.query_id=v.query_id;
        DELETE FROM {schema}.pgderive_progress WHERE query_id='snapshot_grouped';
        INSERT INTO {schema}.pgderive_progress SELECT * FROM validation_progress;
        DELETE FROM {schema}.pgderive_source_slots WHERE query_id='snapshot_grouped';
        INSERT INTO {schema}.pgderive_source_slots SELECT * FROM validation_slot; COMMIT")).await?;
    Ok(())
}
