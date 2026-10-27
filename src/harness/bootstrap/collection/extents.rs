//! Publication may narrow live immutable membership, but cannot widen it later.
use super::{Catalog, Query, Recovery, Writer};
use crate::engine::reader::{ObjectBatch, ObjectRef};
use anyhow::{Context as _, Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &mut Query,
    writer: &mut Writer,
    recovery: &Recovery,
) -> Result<()> {
    super::compact(sql, graph, writer, recovery, "gc-first").await?;
    let mut checkpoint = graph.checkpoint()?;
    let member = checkpoint.arrangements.first_mut().context("missing extent arrangement")?;
    let root = member.trace.objects.first_mut().context("missing extent root")?;
    let original = root.clone();
    let object = ObjectBatch::<String, crate::transaction::Row>::open(
        recovery.store.clone(),
        original.clone(),
        &member.schema,
    )
    .await?;
    ensure!(object.block_count() > 0, "empty extent fixture");
    let mut encoded = serde_json::to_value(&original)?;
    encoded["blocks"] = serde_json::json!({"start":0,"end":object.block_count()});
    *root = serde_json::from_value::<ObjectRef>(encoded)?;
    member.trace.generation =
        member.trace.generation.checked_add(1).context("extent generation overflow")?;
    writer.maintain(sql, graph.plan(), &checkpoint).await?;
    graph.restore_checkpoint(checkpoint.clone()).await?;
    checkpoint.arrangements[0].trace.objects[0] = original;
    checkpoint.arrangements[0].trace.generation = checkpoint.arrangements[0]
        .trace
        .generation
        .checked_add(1)
        .context("extent generation overflow")?;
    let rejected = writer.maintain(sql, graph.plan(), &checkpoint).await;
    ensure!(
        rejected.is_err_and(|error| error
            .to_string()
            .contains("new object has no active upload reservation")),
        "publication widened a sealed extent"
    );
    boundaries(sql, catalog, &graph.checkpoint()?).await?;
    eprintln!("MVP live extents retain physical dependencies; sealed extent widening rejected");
    Ok(())
}

// Rollback-only metadata isolates publication provenance from typed payload reads.
async fn boundaries(
    sql: &mut Client,
    catalog: &Catalog,
    checkpoint: &crate::engine::plan::Checkpoint,
) -> Result<()> {
    let original = serde_json::to_value(checkpoint)?;
    let mut current = original.clone();
    current["arrangements"][0]["trace"]["objects"][0]["blocks"] =
        serde_json::json!({"start":10,"end":20});
    let tx = sql.transaction().await?;
    let changed = tx
        .execute(
            &format!(
                "UPDATE {}.pgderive_queries SET checkpoint=$2 WHERE checkpoint=$1",
                catalog.schema()
            ),
            &[&original, &current],
        )
        .await?;
    ensure!(changed == 1, "extent provenance fixture not unique");
    for (start, end, accepted) in
        [(11, 19, true), (9, 19, false), (11, 21, false), (15, 15, false), (20, 10, false)]
    {
        let mut candidate = original.clone();
        candidate["arrangements"][0]["trace"]["objects"][0]["blocks"] =
            serde_json::json!({"start":start,"end":end});
        let candidate = serde_json::from_value(candidate)?;
        ensure!(
            crate::catalog::validate_additions(&tx, catalog, &candidate).await.is_ok() == accepted,
            "extent publication boundary mismatch: {start}..{end}"
        );
    }
    tx.rollback().await?;
    Ok(())
}
