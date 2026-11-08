//! Query attribution without changing authoritative publication behavior.
use crate::{Config, worker::Settings};
use serde::Serialize;
use serde_json::Value;
use std::{future::Future, io::Write};

#[derive(Clone, Debug, Serialize)]
pub struct Identity {
    tenant_id: String,
    host: String,
    port: u16,
    database: String,
    catalog_schema: String,
    query_id: String,
    #[serde(skip)]
    resources: Option<std::sync::Arc<crate::worker::resources::Resources>>,
}
tokio::task_local! { static IDENTITY: Identity; }

impl Identity {
    pub(crate) fn new(config: &Config, settings: &Settings) -> Self {
        Self {
            tenant_id: settings.tenant_id.clone(),
            host: config.postgres.host.clone(),
            port: config.postgres.port,
            database: config.postgres.database.clone(),
            catalog_schema: settings.catalog_schema.clone(),
            query_id: settings.query_id.clone(),
            resources: config.capacity.clone(),
        }
    }
}

pub async fn scope<F: Future>(identity: Identity, work: F) -> F::Output {
    IDENTITY.scope(identity, work).await
}

#[derive(Serialize)]
struct Tagged<'a> {
    #[serde(flatten)]
    payload: &'a Value,
    worker: &'a Identity,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity: Option<Value>,
}

pub fn write(output: &mut impl Write, event: &Value) -> serde_json::Result<()> {
    IDENTITY
        .try_with(|identity| write_for(output, event, identity))
        .unwrap_or_else(|_| serde_json::to_writer(output, event))
}

pub fn write_for(
    output: &mut impl Write,
    event: &Value,
    identity: &Identity,
) -> serde_json::Result<()> {
    let capacity = identity.resources.as_ref().map(|resources| {
        let usage = |pool: &crate::engine::capacity::Pool| -> anyhow::Result<Value> {
            Ok(serde_json::to_value(pool.observation(&identity.tenant_id)?)?)
        };
        Ok::<_, anyhow::Error>(serde_json::json!({"cache":usage(&resources.process.cache)?,"memtable":usage(&resources.process.memtable)?}))
    }).transpose().map_err(|error| serde_json::Error::io(std::io::Error::other(error)))?;
    serde_json::to_writer(output, &Tagged { payload: event, worker: identity, capacity })
}
