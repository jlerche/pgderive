//! Typed collision checks; synthetic positions never authorize slot ACK.
use crate::{
    catalog::{Catalog, Deltas, Progress, Publication, ResultColumn, Sink, Writer},
    engine::{
        Batch,
        plan::{Checkpoint, Plan},
    },
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Native(Option<String>, Option<String>, Option<Vec<Option<String>>>, Option<String>);
fn native(number: &str, id: &str) -> Native {
    Native(
        Some(number.into()),
        Some("é🙂'".repeat(20000)),
        Some(vec![Some("x".into()), None, Some(String::new())]),
        Some(id.into()),
    )
}
fn sink() -> Sink {
    let columns = [
        ("number", 1700, None),
        ("odd\" label", 25, Some(("pg_catalog".into(), "C".into()))),
        ("items", 1009, Some(("pg_catalog".into(), "C".into()))),
        ("id", 23, None),
    ]
    .into_iter()
    .map(|(name, oid, collation)| ResultColumn { name: name.into(), oid, modifier: -1, collation })
    .collect();
    Sink {
        primary_key: vec![],
        output: crate::catalog::SinkOutput::Postgres,
        table: "bulk_occurrences".into(),
        columns,
    }
}
pub(super) async fn check(
    sql: &mut Client,
    schema: &str,
    initial: &super::initial::Initial<'_>,
) -> Result<()> {
    let catalog = Catalog::new(schema, "bulk_occurrences")?;
    let plan = initial.plan;
    let (mut writer, mut checkpoint) = initial.synthetic(sql, &catalog, sink()).await?;
    let first = native("1.0", "1");
    let second = native("1.00", "01");
    let nulls = Native(None, None, None, None);
    let large = (100..140).map(|id| native("2.0", &id.to_string())).collect::<Vec<_>>();
    let mut initial =
        vec![(((), first.clone()), 2), (((), second.clone()), 1), (((), nulls.clone()), 3)];
    initial.extend(large.iter().map(|value| (((), value.clone()), 1)));
    let initial = Deltas::bag(&Batch::from_updates(initial)?)?;
    publish(sql, &mut writer, plan, &mut checkpoint, &initial).await?;
    let row=sql.query_one(&format!("SELECT count(*),count(DISTINCT pg_catalog.hash_record_extended(ROW(number,\"odd\"\" label\",items,id),0)) FROM {schema}.bulk_occurrences WHERE id=1"),&[]).await?;
    ensure!(
        row.try_get::<_, i64>(0)? == 3 && row.try_get::<_, i64>(1)? == 1,
        "representation collision fixture failed"
    );
    // Distinct input spellings decode to one binary representation and must group.
    let alias = native("1.0", "01");
    let mut deletion =
        vec![(((), first), -1), (((), alias), -1), (((), second.clone()), -1), (((), nulls), -2)];
    deletion.extend(large.into_iter().map(|value| (((), value), -1)));
    let deletion = Deltas::bag(&Batch::from_updates(deletion)?)?;
    publish(sql, &mut writer, plan, &mut checkpoint, &deletion).await?;
    ensure!(sql.query_one(&format!("SELECT count(*) FROM {schema}.bulk_occurrences WHERE number IS NULL AND items IS NULL"),&[]).await?.try_get::<_,i64>(0)?==1,"binary collision or NULL multiplicity was lost");
    let before = catalog.load_durable(sql, plan).await?.context("missing bulk checkpoint")?;
    let missing = Deltas::bag(&Batch::from_updates([(((), second), -1)])?)?;
    ensure!(
        publish(sql, &mut writer, plan, &mut checkpoint, &missing).await.is_err(),
        "missing representation committed"
    );
    let after = catalog.load_durable(sql, plan).await?.context("missing bulk checkpoint")?;
    ensure!(after == before, "failed bulk publication changed membership");
    missing_index(sql, &catalog, plan, schema, &before).await?;
    eprintln!(
        "MVP batched sink native hash collisions, arrays, large values, decode aliases, underflow rollback and missing-index rejection verified"
    );
    Ok(())
}
async fn missing_index(
    sql: &mut Client,
    catalog: &Catalog,
    plan: &Plan,
    schema: &str,
    before: &crate::catalog::Durable,
) -> Result<()> {
    let oid: u32 = sql
        .query_one(
            "SELECT pg_catalog.to_regclass($1)::oid",
            &[&format!("{schema}.bulk_occurrences")],
        )
        .await?
        .try_get(0)?;
    let name = format!("{schema}.pgderive_sink_lookup_{oid}");
    let definition: String = sql
        .query_one("SELECT pg_catalog.pg_get_indexdef($1::text::regclass)", &[&name])
        .await?
        .try_get(0)?;
    sql.batch_execute(&format!("DROP INDEX {schema}.pgderive_sink_lookup_{oid}")).await?;
    ensure!(
        catalog.claim(sql, plan, before.binding.clone()).await.is_err(),
        "missing index was accepted"
    );
    sql.batch_execute(&definition).await?;
    ensure!(
        catalog.load_durable(sql, plan).await?.as_ref() == Some(before),
        "rejected claim changed durable state"
    );
    let reclaimed = catalog.claim(sql, plan, before.binding.clone()).await?;
    ensure!(
        reclaimed.epoch() == before.stored.epoch && reclaimed.end().to_string() == "0/4",
        "index repair changed progress"
    );
    Ok(())
}
async fn publish(
    sql: &mut Client,
    writer: &mut Writer,
    plan: &Plan,
    checkpoint: &mut Checkpoint,
    deltas: &Deltas,
) -> Result<()> {
    super::bags::advance(checkpoint);
    let time = checkpoint.time - 1;
    let progress = Progress::new(
        u32::try_from(time)?,
        &format!("0/{:X}", time * 2 - 1),
        &format!("0/{:X}", time * 2),
    )?;
    writer.publish(sql, plan, Publication { checkpoint, progress: &progress, deltas }).await
}
