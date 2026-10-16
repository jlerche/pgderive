//! Stable logical query ownership over independently bootstrapped generations.
mod control;
mod rebuild;
mod tables;
use super::{spec::Settings, sql::Session};
use crate::Config;
use anyhow::{Context, Result, ensure};
pub use control::drop_query;
pub use rebuild::rebuild;
pub(super) use rebuild::run_owned as rebuild_owned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{GenericClient, Transaction};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Generation {
    query: String,
    slot: String,
    sink: String,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Base {
    slot: String,
    publication: String,
    sink: String,
    prefix: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    base: Base,
    active: Option<Generation>,
    pending: Option<Generation>,
    signature: String,
    phase: Phase,
    retired: Vec<Generation>,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum Phase {
    Active,
    Building,
    Dropping,
    Dropped,
}
pub(super) struct Resolved {
    pub config: Config,
    pub settings: Settings,
}
impl Base {
    fn new(config: &Config, settings: &Settings) -> Self {
        Self {
            slot: config.replication.slot.clone(),
            publication: config.replication.publication.clone(),
            sink: settings.sink_table.clone(),
            prefix: settings.object_prefix.clone(),
        }
    }
}
impl Generation {
    fn original(config: &Config, settings: &Settings) -> Self {
        Self {
            query: settings.query_id.clone(),
            slot: config.replication.slot.clone(),
            sink: settings.sink_table.clone(),
        }
    }
    fn apply(&self, config: &Config, settings: &Settings) -> Result<(Config, Settings)> {
        let mut config = config.clone();
        let mut settings = settings.clone();
        settings.query_id.clone_from(&self.query);
        settings.sink_table.clone_from(&self.sink);
        settings.validate()?;
        config.replication.slot.clone_from(&self.slot);
        config.worker = Some(settings.clone());
        config.validate()?;
        Ok((config, settings))
    }
}
async fn exists(sql: &(impl GenericClient + Sync), table: &str) -> Result<bool> {
    Ok(sql
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&table])
        .await?
        .try_get(0)?)
}
pub(super) async fn require_public(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<()> {
    let table = format!("{}.pgderive_generations", settings.catalog_schema);
    if exists(sql, &table).await? {
        ensure!(
            sql.query_opt(
                &format!("SELECT logical_query FROM {table} WHERE generation=$1"),
                &[&settings.query_id]
            )
            .await?
            .is_none(),
            "physical generations must be managed through their logical query"
        );
    }
    Ok(())
}
async fn read(sql: &(impl GenericClient + Sync), settings: &Settings) -> Result<Option<State>> {
    let table = format!("{}.pgderive_lifecycle", settings.catalog_schema);
    if !exists(sql, &table).await? {
        return Ok(None);
    }
    let Some(row) = sql
        .query_opt(
            &format!("SELECT state,octet_length(state::text) FROM {table} WHERE query_id=$1"),
            &[&settings.query_id],
        )
        .await?
    else {
        return Ok(None);
    };
    ensure!(row.try_get::<_, i32>(1)? <= 65_536, "lifecycle metadata limit exceeded");
    let state: State = serde_json::from_value(row.try_get(0)?)?;
    ensure!(
        state.version == 1 && state.retired.len() <= 128,
        "unsupported lifecycle version or generation count"
    );
    Ok(Some(state))
}
async fn save(sql: &Transaction<'_>, settings: &Settings, state: &State) -> Result<()> {
    ensure!(state.retired.len() <= 128, "retired generation limit exceeded");
    let serialized = serde_json::to_value(state)?;
    let bytes: i32 =
        sql.query_one("SELECT octet_length($1::jsonb::text)", &[&serialized]).await?.try_get(0)?;
    ensure!(bytes <= 65_536, "lifecycle metadata limit exceeded");
    sql.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {0}.pgderive_lifecycle(query_id text PRIMARY KEY,state jsonb NOT NULL); CREATE TABLE IF NOT EXISTS {0}.pgderive_generations(generation text PRIMARY KEY,logical_query text NOT NULL)",settings.catalog_schema)).await?;
    let operation = match state.phase {
        Phase::Building => "building",
        Phase::Active if state.retired.is_empty() => "retired",
        Phase::Active => "cutover",
        Phase::Dropping => "dropping",
        Phase::Dropped => "dropped",
    };
    sql.execute(&format!("/* lifecycle:{operation} */ INSERT INTO {}.pgderive_lifecycle VALUES($1,$2) ON CONFLICT(query_id) DO UPDATE SET state=EXCLUDED.state",settings.catalog_schema), &[&settings.query_id,&serialized]).await?;
    Ok(())
}
pub(super) async fn resolve(
    sql: &(impl GenericClient + Sync),
    config: &Config,
    settings: &Settings,
) -> Result<Resolved> {
    require_public(sql, settings).await?;
    let Some(state) = read(sql, settings).await? else {
        return Ok(Resolved { config: config.clone(), settings: settings.clone() });
    };
    ensure!(
        state.base == Base::new(config, settings),
        "logical query namespace/source configuration changed"
    );
    ensure!(
        state.phase == Phase::Active,
        "query lifecycle operation is pending or query is dropped; resume rebuild/drop explicitly"
    );
    let generation = state.active.context("active lifecycle has no generation")?;
    let owner: String = sql
        .query_one(
            &format!(
                "SELECT logical_query FROM {}.pgderive_generations WHERE generation=$1",
                settings.catalog_schema
            ),
            &[&generation.query],
        )
        .await?
        .try_get(0)?;
    ensure!(owner == settings.query_id, "generation logical owner changed");
    let (config, native) = generation.apply(config, settings)?;
    Ok(Resolved { config, settings: native })
}
pub(super) async fn current(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<(String, Option<Value>)> {
    require_public(sql, settings).await?;
    let state = read(sql, settings).await?;
    let query = state
        .as_ref()
        .and_then(|state| state.active.as_ref())
        .map_or_else(|| settings.query_id.clone(), |generation| generation.query.clone());
    Ok((query, state.map(serde_json::to_value).transpose()?))
}
pub(super) async fn recreate(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<bool> {
    require_public(sql, settings).await?;
    if let Some(state) = read(sql, settings).await? {
        return Ok(state.phase == Phase::Dropped
            || (state.phase == Phase::Building && state.active.is_none()));
    }
    Ok(super::retire::status(sql, settings).await? == Some("dropped"))
}
async fn own(config: &Config) -> Result<(Session, Settings)> {
    config.validate()?;
    let settings = config.worker.clone().context("configuration requires a worker section")?;
    let session = Session::connect(config).await?;
    session.own(&settings.catalog_schema, &settings.query_id).await?;
    require_public(&session.client, &settings).await?;
    Ok((session, settings))
}
