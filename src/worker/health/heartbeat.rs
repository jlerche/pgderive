//! A separate diagnostic connection; source reception is never periodically cancelled.
use crate::{
    Config,
    catalog::Durable,
    worker::{open::Open, sql::Session},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{Duration, interval, timeout},
};
use tokio_postgres::{Client, GenericClient};

struct Owner {
    table: String,
    query: String,
    progress: String,
    scope: String,
    pid: i32,
    fence: i64,
    token: String,
}
pub(in crate::worker) struct Monitor {
    task: JoinHandle<Result<()>>,
    updates: watch::Sender<Value>,
    finish: mpsc::Sender<Value>,
    task_done: bool,
}
pub(in crate::worker) struct Reporter(watch::Sender<Value>);
impl Reporter {
    pub(in crate::worker) fn record(&self, open: &Open, phase: &str) -> Result<()> {
        self.0.send_replace(snapshot(open, phase)?);
        Ok(())
    }
}
impl Monitor {
    pub(in crate::worker) async fn start(
        config: &Config,
        open: &Open,
        durable: &Durable,
    ) -> Result<Self> {
        let owner = Owner {
            table: format!("{}.pgderive_worker_health", open.settings.catalog_schema),
            progress: format!("{}.pgderive_progress", open.settings.catalog_schema),
            query: open.settings.query_id.clone(),
            scope: format!(
                "pgderive-worker:{}:{}",
                open.settings.catalog_schema, open.settings.query_id
            ),
            pid: open.session.client.query_one("SELECT pg_backend_pid()", &[]).await?.try_get(0)?,
            fence: i64::try_from(durable.fence)?,
            token: open.nonce.clone(),
        };
        open.session.client.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {}(query_id text PRIMARY KEY,owner_pid integer NOT NULL,fence bigint NOT NULL,token text NOT NULL,heartbeat_at timestamptz NOT NULL,diagnostics jsonb NOT NULL)",owner.table)).await?;
        let session = Session::connect(config).await?;
        let initial = snapshot(open, "ready")?;
        timeout(
            Duration::from_secs(open.settings.health.stale_secs),
            owner.write(&session.client, &initial),
        )
        .await
        .context("health initial heartbeat timed out")??;
        let (updates, mut receiver) = watch::channel(initial);
        let (finish, mut final_state) = mpsc::channel::<Value>(1);
        let seconds = open.settings.health.heartbeat_secs;
        let deadline = Duration::from_secs(open.settings.health.stale_secs);
        let task = tokio::spawn(async move {
            let mut clock = interval(Duration::from_secs(seconds));
            loop {
                tokio::select! {
                    final_value = final_state.recv() => {
                        let value = final_value.context("health final state channel closed")?;
                        owner.write(&session.client, &value).await?;
                        return Ok(());
                    }
                    _ = clock.tick() => {}
                    changed = receiver.changed() => {
                        if changed.is_err() { return Ok(()); }
                    }
                }
                let current = receiver.borrow_and_update().clone();
                timeout(deadline, owner.write(&session.client, &current))
                    .await
                    .context("health heartbeat write timed out")??;
            }
        });
        Ok(Self { task, updates, finish, task_done: false })
    }
    pub(in crate::worker) fn reporter(&self) -> Reporter {
        Reporter(self.updates.clone())
    }
    pub(in crate::worker) async fn failed(&mut self) -> Result<()> {
        let result = (&mut self.task).await;
        self.task_done = true;
        result.context("health task failed")??;
        anyhow::bail!("health task unexpectedly stopped")
    }
    pub(in crate::worker) async fn finish(
        &mut self,
        open: &Open,
        failure: Option<&anyhow::Error>,
        attempts: u32,
    ) -> Result<()> {
        ensure!(!self.task_done, "health task unavailable for final diagnostics");
        let failed = failure.is_some();
        let mut state = snapshot(open, if failed { "failed" } else { "stopped" })?;
        state["failure"] = json!({"attempt":attempts,"retry_limit":open.settings.retry_attempts,
            "retry_budget_exhausted":failed && attempts >= open.settings.retry_attempts,
            "category":failure.map(super::failure_category)});
        self.finish.send(state).await.context("health final diagnostics channel failed")?;
        let result = (&mut self.task).await;
        self.task_done = true;
        result.context("health final diagnostics task failed")?
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Owner {
    async fn write(&self, sql: &Client, diagnostics: &Value) -> Result<()> {
        let count = sql.execute(&format!("INSERT INTO {table}(query_id,owner_pid,fence,token,heartbeat_at,diagnostics) SELECT $1,$2,$3,$4,clock_timestamp(),$5 WHERE EXISTS(SELECT 1 FROM {progress} WHERE query_id=$1 AND fence=$3) AND {owned} ON CONFLICT(query_id) DO UPDATE SET owner_pid=EXCLUDED.owner_pid,fence=EXCLUDED.fence,token=EXCLUDED.token,heartbeat_at=EXCLUDED.heartbeat_at,diagnostics=EXCLUDED.diagnostics WHERE {table}.fence<=EXCLUDED.fence",table=self.table,progress=self.progress,owned=ownership("$2","$6")), &[&self.query,&self.pid,&self.fence,&self.token,diagnostics,&self.scope]).await?;
        ensure!(count == 1, "health heartbeat lost worker ownership or fence");
        Ok(())
    }
}

pub(super) fn ownership(pid: &str, scope: &str) -> String {
    format!(
        "EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid={pid} AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()) AND objsubid=1 AND classid=((pg_catalog.hashtextextended({scope},0)>>32)&4294967295)::oid AND objid=(pg_catalog.hashtextextended({scope},0)&4294967295)::oid AND granted)"
    )
}

pub(super) async fn observed(
    sql: &(impl GenericClient + Sync),
    settings: &crate::worker::Settings,
) -> Result<Option<Value>> {
    let table = format!("{}.pgderive_worker_health", settings.catalog_schema);
    let present: bool =
        sql.query_one("SELECT to_regclass($1) IS NOT NULL", &[&table]).await?.try_get(0)?;
    if !present {
        return Ok(None);
    }
    let scope = format!("pgderive-worker:{}:{}", settings.catalog_schema, settings.query_id);
    let stale = i64::try_from(settings.health.stale_secs)?;
    sql.query_opt(&format!("SELECT jsonb_build_object('heartbeat_at',h.heartbeat_at,'age_seconds',greatest(0,extract(epoch FROM clock_timestamp()-h.heartbeat_at)),'recent',h.heartbeat_at>=clock_timestamp()-($2::bigint*interval '1 second'),'owner_pid',h.owner_pid,'owner_live',{owned},'fence',h.fence,'fence_current',EXISTS(SELECT 1 FROM {schema}.pgderive_progress p WHERE p.query_id=h.query_id AND p.fence=h.fence),'diagnostics',h.diagnostics) FROM {table} h WHERE query_id=$1",schema=settings.catalog_schema,owned=ownership("h.owner_pid","$3")), &[&settings.query_id,&stale,&scope]).await?.map(|row|row.try_get(0)).transpose().map_err(Into::into)
}

fn snapshot(open: &Open, phase: &str) -> Result<Value> {
    let cache = open.query.cache_stats()?;
    let limits = open.replication.execution;
    Ok(
        json!({"phase":phase,"logical_time":open.query.time(),"runs":open.query.run_count(),"cache":{"bytes":cache.bytes,"entries":cache.entries,"hits":cache.hits,"misses":cache.misses},
            "budgets":{"cache_bytes":limits.cache_bytes,"cache_entries":limits.cache_entries,
            "resident_bytes":limits.resident_bytes,"resident_entries":limits.resident_entries,
            "record_bytes":limits.record_bytes,"contributions":limits.contributions,
            "spill_runs":limits.spill_runs,"scratch_bytes":limits.scratch_bytes,
            "output_bytes":limits.output_bytes,"output_entries":limits.output_entries,
            "transaction_changes":open.replication.listener.max_transaction_changes}}),
    )
}
