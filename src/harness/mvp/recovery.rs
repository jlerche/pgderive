use super::{Batch, Bid, Group, Key, Model, MvpFixture, Query, operators};
use crate::{
    catalog::{Catalog, Stored},
    engine::{
        execution::Limits,
        plan::Checkpoint,
        plan::query::{AggregateRow, Settings},
        reader::ObjectBatch,
    },
    harness::recovery::{QUERY, Recovery, RecoveryReport},
};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;
use tokio_postgres::Client;

impl MvpFixture {
    pub(in crate::harness) async fn checkpoint_and_recover(
        &mut self,
        sql: &mut Client,
        schema: &str,
        recovery: &Recovery,
    ) -> Result<()> {
        let catalog = Catalog::new(schema, QUERY)?;
        catalog.install(sql).await?;
        let checkpoint = self.graph.checkpoint()?;
        self.epoch = catalog.checkpoint(sql, self.graph.plan(), &checkpoint, self.epoch).await?;
        ensure!(
            catalog.checkpoint(sql, self.graph.plan(), &checkpoint, self.epoch - 1).await.is_err(),
            "stale checkpoint writer succeeded"
        );
        let mut unrepresentable = checkpoint.clone();
        unrepresentable.arrangements[0].trace.generation = u64::MAX;
        ensure!(
            catalog.checkpoint(sql, self.graph.plan(), &unrepresentable, self.epoch).await.is_err(),
            "unrepresentable generation was committed"
        );
        let unchanged = catalog
            .load(sql, self.graph.plan())
            .await?
            .context("checkpoint disappeared after failed write")?;
        ensure!(
            unchanged.epoch == self.epoch && unchanged.checkpoint == checkpoint,
            "failed catalog write changed membership/epoch"
        );
        let report = recovery.recover(sql, schema).await?;
        ensure!(
            report.time == self.graph.time()
                && report.epoch == self.epoch
                && report.arrangements == 4
                && report.plan_identity == self.graph.plan().identity(),
            "fresh process recovered wrong boundary"
        );
        if checkpoint.time == 5 {
            incompatible_catalog(sql, &catalog, self.graph.plan()).await?;
            super::catalog_checks::corrupt_membership(sql, schema, &catalog, self.graph.plan())
                .await?;
            if recovery.prefix.is_some() && recovery.config.source_path.is_some() {
                self.missing_objects(sql, schema, recovery, &checkpoint).await?;
            }
        }
        eprintln!(
            "MVP durable manifest recovered all four arrangements at tick {} epoch {}",
            report.time, report.epoch
        );
        Ok(())
    }
    async fn missing_objects(
        &self,
        sql: &mut Client,
        schema: &str,
        recovery: &Recovery,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        let member = checkpoint
            .arrangements
            .iter()
            .find(|member| member.id == "output")
            .context("missing output member")?;
        let reference = member.trace.objects.first().context("missing output object")?;
        let object = ObjectBatch::<Group, AggregateRow>::open(
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
            recovery.recover(sql, schema).await?.epoch == self.epoch,
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
    ensure!(catalog.load(sql, &incompatible).await.is_err(), "incompatible plan was accepted");
    Ok(())
}

pub(in crate::harness) async fn recover_plan(
    store: Arc<dyn ObjectStore>,
    limits: Limits,
    stored: Stored,
    model: &Model,
    plan: crate::engine::plan::Plan,
) -> Result<RecoveryReport> {
    let checkpoint = stored.checkpoint;
    let report = RecoveryReport {
        time: checkpoint.time,
        epoch: stored.epoch,
        arrangements: checkpoint.arrangements.len(),
        objects: checkpoint.arrangements.iter().map(|member| member.trace.objects.len()).sum(),
        plan_identity: checkpoint.plan_identity.clone(),
        source_end: None,
    };
    let graph = Query::reopen(
        plan,
        operators(),
        Settings { store, block_rows: 3, limits },
        checkpoint.clone(),
    )
    .await?;
    let state = graph.snapshot();
    let (left, right) = projected(model)?;
    ensure!(
        state.left.materialize().await? == left && state.right.materialize().await? == right,
        "recovered projected input arrangements differ from source SQL"
    );
    let sums = super::memory(model)?;
    ensure!(
        state.sums.materialize().await? == sums,
        "recovered statistics differ from independent source recomputation"
    );
    let output = Batch::from_updates(sums.iter().map(|((key, value), _)| {
        ((key.clone(), (value.rows, (value.non_null != 0).then_some(value.sum))), 1)
    }))?;
    ensure!(
        state.output.materialize().await? == output,
        "recovered COUNT/SUM rows differ from recomputation"
    );
    ensure!(graph.checkpoint()? == checkpoint, "reopen changed membership/generation/clock");
    Ok(report)
}
type Projected = (Batch<Key, Group>, Batch<Key, Bid>);
fn projected(model: &Model) -> Result<Projected> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for ((table, id), row) in model {
        if table == "auction" {
            left.push((
                (id.clone(), row.get("category").context("missing auction category")?.clone()),
                1,
            ));
        } else if table == "bid"
            && let Some(auction) = row.get("auction").and_then(Option::as_ref)
        {
            right.push((
                (auction.clone(), (id.clone(), super::projection_oracle::number(row, "price")?)),
                1,
            ));
        }
    }
    Ok((Batch::from_updates(left)?, Batch::from_updates(right)?))
}
