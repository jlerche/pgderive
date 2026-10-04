use super::{mvp, source_state};
use crate::{Config, catalog::Catalog};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::process::Command;
use tokio_postgres::Client;

pub(super) const QUERY: &str = "auction_bid_count_sum";
#[derive(Clone)]
pub(super) struct Recovery {
    pub(super) config: Config,
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) prefix: Option<String>,
}
/// Independently recovered coarse catalog/state summary from the live fixture.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// Restored complete logical tick.
    pub time: u64,
    /// Authoritative catalog publication epoch.
    pub epoch: u64,
    /// Number of reopened named arrangements.
    pub arrangements: usize,
    /// Number of coarse immutable run memberships, retaining repeats.
    pub objects: usize,
    /// Exact registered plan identity.
    pub plan_identity: String,
    /// Durable source end for a publication query; absent for metadata-only snapshots.
    pub source_end: Option<String>,
}
impl Recovery {
    pub(super) async fn recover(&self, sql: &mut Client, schema: &str) -> Result<RecoveryReport> {
        self.recover_query(sql, schema, QUERY).await
    }
    pub(super) async fn recover_query(
        &self,
        sql: &mut Client,
        schema: &str,
        query: &str,
    ) -> Result<RecoveryReport> {
        if self.config.object_store.is_none() || self.config.source_path.is_none() {
            let plan = recovery_plan(sql, schema, query).await?;
            let catalog = Catalog::new(schema, query)?;
            let (stored, end) = stored(sql, &catalog, &plan, query).await?;
            let mut report = mvp::recover_plan(
                self.store.clone(),
                self.config.execution,
                stored,
                &source_state(sql, schema).await?,
                plan,
            )
            .await?;
            report.source_end = end;
            return Ok(report);
        }
        let output = self.child_query(schema, query).await?;
        ensure!(
            output.status.success(),
            "fresh-process recovery failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    pub(super) async fn child(&self, schema: &str) -> Result<std::process::Output> {
        self.child_query(schema, QUERY).await
    }
    async fn child_query(&self, schema: &str, query: &str) -> Result<std::process::Output> {
        let path = self
            .config
            .source_path
            .as_ref()
            .context("fresh process requires a loaded config file")?;
        let prefix =
            self.prefix.as_ref().context("fresh process requires persistent object storage")?;
        let mut command = Command::new(std::env::current_exe()?);
        command.args(["--recover"]).arg(path).args([schema, query, prefix]).kill_on_drop(true);
        tokio::time::timeout(Duration::from_secs(45), command.output())
            .await
            .context("fresh-process recovery timed out")?
            .context("launching fresh-process recovery")
    }
}
/// Reopen every registered arrangement in a fresh live harness process.
/// Uses source SQL and an independent recomputation oracle; does not start CDC.
///
/// # Errors
/// Returns config, catalog, missing/corrupt state, or oracle failures.
pub async fn run_recovery(
    config: Config,
    schema: &str,
    query: &str,
    prefix: &str,
) -> Result<RecoveryReport> {
    config.validate()?;
    ensure!(config.postgres.tls == "disable", "local recovery harness requires TLS disabled");
    ensure!(
        prefix.starts_with(&format!("{schema}-"))
            && matches!(query, QUERY | "atomic_grouped" | "snapshot_grouped"),
        "invalid recovery fixture scope"
    );
    let catalog = Catalog::new(schema, query)?;
    let store = config
        .object_store
        .as_ref()
        .context("recovery requires persistent object storage")?
        .build(prefix)?;
    let (mut sql, connection) = super::connect(&config).await?;
    let result = async {
        let plan = recovery_plan(&sql, schema, query).await?;
        let (stored, end) = stored(&mut sql, &catalog, &plan, query).await?;
        let mut report = mvp::recover_plan(
            store,
            config.execution,
            stored,
            &source_state(&sql, schema).await?,
            plan,
        )
        .await?;
        report.source_end = end;
        Ok(report)
    }
    .await;
    drop(sql);
    let disconnected = connection.await.context("recovery SQL task failed")?.map_err(Into::into);
    crate::outcome::combine(result, disconnected, "recovery disconnect")
}

async fn stored(
    sql: &mut Client,
    catalog: &Catalog,
    plan: &crate::engine::plan::Plan,
    query: &str,
) -> Result<(crate::catalog::Stored, Option<String>)> {
    if matches!(query, "atomic_grouped" | "snapshot_grouped") {
        let durable =
            catalog.load_durable(sql, plan).await?.context("missing durable recovery boundary")?;
        return Ok((durable.stored, Some(durable.end.to_string())));
    }
    Ok((catalog.load(sql, plan).await?.context("missing recovery checkpoint")?, None))
}

async fn recovery_plan(
    sql: &Client,
    schema: &str,
    query: &str,
) -> Result<crate::engine::plan::Plan> {
    if query != "snapshot_grouped" {
        return mvp::registered_plan();
    }
    let binding: crate::catalog::Binding = serde_json::from_value(
        sql.query_one(
            &format!("SELECT binding FROM {schema}.pgderive_progress WHERE query_id=$1"),
            &[&query],
        )
        .await?
        .try_get(0)?,
    )?;
    let contract = binding.registered_source()?.context("missing registered bootstrap source")?;
    contract.verify(sql).await?;
    super::bootstrap::plan(&contract)
}
