//! Logical-query attempt observations, including failures before engine activation.
use crate::{
    Config,
    worker::{Settings, sql::Session},
};
use anyhow::Result;
use serde_json::{Value, json};
use tokio_postgres::{Client, GenericClient};

pub(in crate::worker) struct Ticket {
    token: String,
    installed: bool,
}
impl Ticket {
    pub(in crate::worker) const fn new() -> Self {
        Self { token: String::new(), installed: false }
    }
    pub(in crate::worker) async fn start(
        &mut self,
        config: &Config,
        settings: &Settings,
        initial: Option<Session>,
        failures: u32,
    ) -> Result<Session> {
        let session =
            if let Some(session) = initial { session } else { Session::connect(config).await? };
        session.own(&settings.catalog_schema, &settings.query_id).await?;
        self.token = session
            .client
            .query_one("SELECT pg_catalog.gen_random_uuid()::text", &[])
            .await?
            .try_get(0)?;
        session.client.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {}.pgderive_worker_attempts(query_id text PRIMARY KEY,owner_pid integer NOT NULL,updated_at timestamptz NOT NULL,diagnostics jsonb NOT NULL)", settings.catalog_schema)).await?;
        let value = self.value(settings, "starting", (failures, None));
        session.client.execute(&format!("INSERT INTO {}.pgderive_worker_attempts(query_id,owner_pid,updated_at,diagnostics) VALUES($1,pg_backend_pid(),clock_timestamp(),$2) ON CONFLICT(query_id) DO UPDATE SET owner_pid=EXCLUDED.owner_pid,updated_at=EXCLUDED.updated_at,diagnostics=EXCLUDED.diagnostics",settings.catalog_schema), &[&settings.query_id,&value]).await?;
        self.installed = true;
        Ok(session)
    }
    pub(in crate::worker) async fn failure(
        &self,
        config: &Config,
        settings: &Settings,
        failures: u32,
        error: &anyhow::Error,
    ) -> Result<bool> {
        anyhow::ensure!(self.installed, "attempt has no persistence identity");
        let session = Session::connect(config).await?;
        if !session.try_own(&settings.catalog_schema, &settings.query_id).await? {
            return Ok(false);
        }
        self.record(
            &session.client,
            settings,
            if failures >= settings.retry_attempts { "exhausted" } else { "backoff" },
            (failures, Some(error)),
        )
        .await
    }
    pub(in crate::worker) async fn record(
        &self,
        sql: &Client,
        settings: &Settings,
        phase: &str,
        failure: (u32, Option<&anyhow::Error>),
    ) -> Result<bool> {
        let scope = format!("pgderive-worker:{}:{}", settings.catalog_schema, settings.query_id);
        let value = self.value(settings, phase, failure);
        let count = sql.execute(&format!("UPDATE {schema}.pgderive_worker_attempts SET owner_pid=pg_backend_pid(),updated_at=clock_timestamp(),diagnostics=$2 WHERE query_id=$1 AND diagnostics->>'token'=$4 AND {owned}", schema=settings.catalog_schema, owned=super::heartbeat::ownership("pg_backend_pid()", "$3")), &[&settings.query_id,&value,&scope,&self.token]).await?;
        Ok(count == 1)
    }
    fn value(
        &self,
        settings: &Settings,
        phase: &str,
        failure: (u32, Option<&anyhow::Error>),
    ) -> Value {
        json!({"token":self.token,"phase":phase,"consecutive_failures":failure.0,"retry_limit":settings.retry_attempts,"failure_category":failure.1.map(super::failure_category)})
    }
}

pub(in crate::worker) async fn observed(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<Option<Value>> {
    let table = format!("{}.pgderive_worker_attempts", settings.catalog_schema);
    let present: bool =
        sql.query_one("SELECT to_regclass($1) IS NOT NULL", &[&table]).await?.try_get(0)?;
    if !present {
        return Ok(None);
    }
    let scope = format!("pgderive-worker:{}:{}", settings.catalog_schema, settings.query_id);
    sql.query_opt(&format!("SELECT jsonb_build_object('updated_at',a.updated_at,'owner_pid',a.owner_pid,'owner_live',{owned},'diagnostics',a.diagnostics) FROM {table} a WHERE query_id=$1", owned=super::heartbeat::ownership("a.owner_pid", "$2")), &[&settings.query_id,&scope]).await?.map(|row| row.try_get(0)).transpose().map_err(Into::into)
}
