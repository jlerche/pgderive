use super::{Batch, Key, Model, MvpFixture, Query, operators};
use crate::{
    catalog::{Catalog, Stored},
    engine::{execution::Limits, plan::Settings, reader::ObjectBatch},
    harness::recovery::{QUERY, Recovery, RecoveryReport},
};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;
use tokio_postgres::Client;

impl MvpFixture {
    pub(in crate::harness) async fn checkpoint_and_recover(
        &self,
        sql: &mut Client,
        schema: &str,
        recovery: &Recovery,
    ) -> Result<()> {
        let catalog = Catalog::new(schema, QUERY)?;
        let checkpoint = self.graph.checkpoint()?;
        let durable = catalog
            .load_durable(sql, self.graph.plan())
            .await?
            .context("missing published recovery boundary")?;
        ensure!(
            durable.stored.checkpoint == checkpoint,
            "published checkpoint differs from local state"
        );
        let report = recovery.recover(sql, schema).await?;
        ensure!(
            report.time == self.graph.time()
                && report.epoch == durable.stored.epoch
                && report.arrangements == self.graph.plan().definition().arrangements.len()
                && report.plan_identity == self.graph.plan().identity(),
            "fresh process recovered wrong boundary"
        );
        if checkpoint.time == 6 {
            incompatible_catalog(sql, &catalog, self.graph.plan()).await?;
            super::catalog_checks::corrupt_membership(
                sql,
                schema,
                &catalog,
                self.graph.plan(),
                recovery.store.clone(),
            )
            .await?;
            if recovery.prefix.is_some() && recovery.config.source_path.is_some() {
                self.missing_objects(sql, schema, recovery, &durable.stored).await?;
            }
        }
        eprintln!(
            "MVP durable manifest recovered all registered arrangements at tick {} epoch {}",
            report.time, report.epoch
        );
        Ok(())
    }
    async fn missing_objects(
        &self,
        sql: &mut Client,
        schema: &str,
        recovery: &Recovery,
        stored: &Stored,
    ) -> Result<()> {
        let checkpoint = &stored.checkpoint;
        let member = checkpoint
            .arrangements
            .iter()
            .find(|member| member.id == "output")
            .context("missing output member")?;
        let reference = member.trace.objects.first().context("missing output object")?;
        let object = ObjectBatch::<Key, crate::transaction::Row>::open(
            recovery.store.clone(),
            reference.clone(),
            &member.schema,
        )
        .await?;
        for path in [reference.path(), object.block_path(0).context("missing output block")?] {
            let path = Path::from(path);
            let bytes = recovery.store.get(&path).await?.bytes().await?;
            recovery.store.delete(&path).await?;
            let failure = recovery.child(schema).await?;
            let evidence = String::from_utf8_lossy(&failure.stderr);
            eprintln!("expected cold-recovery failure for {path}: {evidence}");
            ensure!(
                !failure.status.success() && evidence.contains(path.as_ref()),
                "missing object was accepted or unrelated failure occurred"
            );
            recovery.store.put(&path, bytes.into()).await?;
        }
        ensure!(
            recovery.recover(sql, schema).await?.epoch == stored.epoch,
            "missing-object recovery changed catalog epoch"
        );
        Ok(())
    }
}
async fn incompatible_catalog(
    sql: &mut Client,
    catalog: &Catalog,
    plan: &crate::engine::plan::Plan,
) -> Result<()> {
    let mut definition = plan.definition().clone();
    definition.revision.push_str("-incompatible");
    let incompatible = crate::engine::plan::Plan::new(definition)?;
    ensure!(
        catalog.load_durable(sql, &incompatible).await.is_err(),
        "incompatible plan was accepted"
    );
    Ok(())
}

pub(in crate::harness) async fn recover_plan(
    store: Arc<dyn ObjectStore>,
    limits: Limits,
    durable: crate::catalog::Durable,
    model: &Model,
    plan: crate::engine::plan::Plan,
) -> Result<RecoveryReport> {
    let stored = durable.stored;
    let checkpoint = stored.checkpoint;
    let report = RecoveryReport {
        time: checkpoint.time,
        epoch: stored.epoch,
        arrangements: checkpoint.arrangements.len(),
        objects: checkpoint.arrangements.iter().map(|member| member.trace.objects.len()).sum(),
        plan_identity: checkpoint.plan_identity.clone(),
        source_end: durable.end.to_string(),
    };
    let mut graph = Query::new(
        plan,
        operators(),
        &Settings { store, block_rows: 3, limits, encoded_cache: None },
    )?;
    graph.restore_checkpoint(checkpoint.clone()).await?;
    let state = graph.snapshot();
    let (left, right) = projected(model)?;
    ensure!(
        state.trace("left")?.materialize().await? == left
            && state.trace("right")?.materialize().await? == right,
        "recovered projected input arrangements differ from source SQL"
    );
    let sums = super::memory(model)?;
    ensure!(
        state.trace("sums")?.materialize().await? == sums,
        "recovered statistics differ from independent source recomputation"
    );
    let output = Batch::from_updates(
        sums.iter()
            .map(|((key, value), _)| {
                let category = serde_json::from_str(key)?;
                let rows = super::integer(value, "rows")?;
                let non_null = super::integer(value, "non_null")?;
                let sum = super::integer(value, "sum")?;
                Ok((
                    (
                        String::new(),
                        super::visible_row(category, rows, (non_null != 0).then_some(sum)),
                    ),
                    1,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )?;
    ensure!(
        state.trace("output")?.materialize().await? == output,
        "recovered COUNT/SUM rows differ from recomputation"
    );
    ensure!(graph.checkpoint()? == checkpoint, "reopen changed membership/generation/clock");
    Ok(report)
}
type Projected = (Batch<Key, crate::transaction::Row>, Batch<Key, crate::transaction::Row>);
fn projected(model: &Model) -> Result<Projected> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for ((table, id), row) in model {
        if table == "auction" {
            left.push((
                (
                    id.clone(),
                    crate::transaction::Row::from([(
                        "category".into(),
                        row.get("category").context("missing category")?.clone(),
                    )]),
                ),
                1,
            ));
        } else if table == "bid"
            && let Some(auction) = row.get("auction").and_then(Option::as_ref)
        {
            right.push((
                (
                    auction.clone(),
                    crate::transaction::Row::from([
                        ("bid_id".into(), Some(id.clone())),
                        ("price".into(), row.get("price").context("missing price")?.clone()),
                    ]),
                ),
                1,
            ));
        }
    }
    Ok((Batch::from_updates(left)?, Batch::from_updates(right)?))
}
