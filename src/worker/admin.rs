//! Operator-facing query inspection, separate from durable worker startup.
use super::{program, spec::QueryDefinition, sql::Session};
use crate::{
    Config,
    source::{Contract, Identity},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

/// Create a new query or explicitly restart its existing registration.
/// Ownership spans the precondition check and startup, preventing adoption races.
///
/// # Errors
/// Rejects occupied creation, missing restart, incompatible state or worker failures.
pub async fn start(config: Config, restart: bool) -> Result<()> {
    config.validate()?;
    let settings = config.worker.clone().context("configuration requires a worker section")?;
    let storage = config.object_store.as_ref().context("worker requires durable object storage")?;
    let store = storage.build(&settings.object_prefix)?;
    storage.preflight(store.as_ref()).await?;
    let mut session = Session::connect(&config).await?;
    session.own(&settings.catalog_schema, &settings.query_id).await?;
    if !restart && super::lifecycle::recreate(&session.client, &settings).await? {
        session = super::lifecycle::rebuild_owned(&config, &settings, session, true).await?;
        return super::drive::run_initial(config, settings, Some(session)).await;
    }
    let resolved = super::lifecycle::resolve(&session.client, &config, &settings).await?;
    let native = &resolved.settings;
    let table = format!("{}.pgderive_worker_registration", settings.catalog_schema);
    let exists: bool = session
        .client
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&table])
        .await?
        .try_get(0)?;
    let registered = exists
        && session
            .client
            .query_opt(
                &format!("SELECT query_id FROM {table} WHERE query_id=$1"),
                &[&native.query_id],
            )
            .await?
            .is_some();
    ensure!(
        registered == restart,
        "create requires an unregistered query; restart requires an existing registration"
    );
    if !restart {
        let catalog = crate::catalog::Catalog::new(&settings.catalog_schema, &settings.query_id)?;
        catalog.install(&session.client).await?;
        // Establish this invocation's journal while ownership is uninterrupted.
        // Bootstrap retries may then resume this exact registration. An uncertain
        // journal COMMIT returns an error and requires explicit status/restart.
        super::registration::source(&mut session.client, &config, &settings).await?;
    }
    super::drive::run_initial(config, settings, Some(session)).await
}

/// Resolve and describe a query against `PostgreSQL` without durable registration.
///
/// The plan identity uses the configured logical slot alias; creation binds an
/// independently journaled physical slot and therefore has its own identity.
///
/// # Errors
/// Returns configuration, catalog, unsupported SQL, type or connection failures.
pub async fn validate(config: &Config) -> Result<Value> {
    config.validate()?;
    let settings = config.worker.as_ref().context("configuration requires a worker section")?;
    let mut session = Session::connect(config).await?;
    crate::semantics::lease(&session.client).await?;
    super::lifecycle::require_public(&session.client, settings).await?;
    let identity = Identity::inspect(config).await?;
    let tx = session
        .client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    tx.batch_execute("SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,MDY'").await?;
    let source =
        Contract::inspect(&tx, identity, &config.replication.publication, &config.replication.slot)
            .await?;
    let (compiled, columns) = settings.query.describe(&tx, &source).await?;
    compiled.sink(&settings.sink_table, settings.sink_output).validate()?;
    let plan = program::plan(&source, &compiled)?;
    tx.commit().await?;
    Ok(json!({"version":1,"query_id":settings.query_id,"valid":true,
        "identity_scope":"configured-slot-alias","plan_identity":plan.identity(),
        "columns":columns,"source":source,"compiled":compiled}))
}

/// Inspect journaled query activation and source progress without changing state.
///
/// # Errors
/// Returns configuration, catalog compatibility or `PostgreSQL` connection errors.
pub async fn status(config: &Config) -> Result<Value> {
    config.validate()?;
    let settings = config.worker.as_ref().context("configuration requires a worker section")?;
    let mut session = Session::connect(config).await?;
    let tx = session
        .client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let (query_id, lifecycle) = super::lifecycle::current(&tx, settings).await?;
    let table = format!("{}.pgderive_worker_registration", settings.catalog_schema);
    let exists: bool = tx
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&table])
        .await?
        .try_get(0)?;
    let registration = if exists {
        tx.query_opt(
            &format!("SELECT pg_catalog.to_jsonb(r) FROM {table} r WHERE query_id=$1"),
            &[&query_id],
        )
        .await?
        .map(|row| row.try_get::<_, Value>(0))
        .transpose()?
    } else {
        None
    };
    let progress = if registration.is_some() {
        tx.query_opt(&format!(
            "SELECT pg_catalog.jsonb_build_object('logical_time',q.logical_time,'epoch',q.epoch,'fence',p.fence,'commit_lsn',p.commit_lsn::text,'end_lsn',p.end_lsn::text,'xid',p.xid) FROM {}.pgderive_queries q JOIN {}.pgderive_progress p USING(query_id) WHERE q.query_id=$1",
            settings.catalog_schema, settings.catalog_schema,
        ), &[&query_id]).await?.map(|row| row.try_get::<_, Value>(0)).transpose()?
    } else {
        None
    };
    let state = if progress.is_some() {
        "activated"
    } else if registration.is_some() {
        "registered"
    } else {
        "unregistered"
    };
    let mut native = settings.clone();
    native.query_id = query_id;
    let state = lifecycle
        .as_ref()
        .and_then(|value| value.get("phase"))
        .and_then(Value::as_str)
        .filter(|phase| *phase != "Active")
        .map_or(state, removal_state);
    let attempt = super::health::attempts::observed(&tx, settings).await?;
    let health =
        super::health::observe(&tx, &native, registration.as_ref(), progress.as_ref()).await?;
    tx.commit().await?;
    Ok(json!({"version":1,"query_id":settings.query_id,"state":state,
        "registration":registration,"progress":progress,"lifecycle":lifecycle,"health":health,"worker_attempt":attempt}))
}

pub(super) fn query_sql(query: &QueryDefinition) -> String {
    match query {
        QueryDefinition::Sql(query) => query.sql.clone(),
        QueryDefinition::Legacy(query) => format!(
            "SELECT l.{group},COUNT(*),SUM(r.{sum}) FROM {left_schema}.{left_table} l JOIN {right_schema}.{right_table} r ON l.{left_key}=r.{right_key} GROUP BY l.{group}",
            group = quote(&query.group),
            sum = quote(&query.sum),
            left_schema = quote(&query.left_schema),
            left_table = quote(&query.left_table),
            right_schema = quote(&query.right_schema),
            right_table = quote(&query.right_table),
            left_key = quote(&query.left_key),
            right_key = quote(&query.right_key),
        ),
    }
}

pub(super) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Begin or finish coordinated database-wide semantic maintenance.
///
/// # Errors
/// Rejects active worker leases, invalid configuration or database failures.
pub async fn environment(config: &Config, begin: bool) -> Result<Value> {
    config.validate()?;
    let mut session = Session::connect(config).await?;
    crate::semantics::change(&mut session.client, begin).await
}

// CLI removal states are independent of the journal enum's serialized spelling.
fn removal_state(phase: &str) -> &str {
    match phase {
        "Dropping" => "dropping",
        "Dropped" => "dropped",
        phase => phase,
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn removal_status_does_not_expose_journal_enum_casing() {
        for (journal, status) in
            [("Dropping", "dropping"), ("Dropped", "dropped"), ("Building", "Building")]
        {
            assert_eq!(super::removal_state(journal), status);
        }
    }
}
