//! Journaled query removal; reader roots remain protected after membership retirement.
use super::{results, spec::Settings, sql::Session};
use crate::{
    Config,
    catalog::{Catalog, GcLimits, Sink},
    source::Contract,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{Client, GenericClient};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Removal {
    version: u32,
    source: Contract,
    sink: Sink,
    sink_oid: Option<u32>,
    prefix: String,
    view: Option<String>,
    finished: bool,
}

async fn read(sql: &(impl GenericClient + Sync), settings: &Settings) -> Result<Option<Removal>> {
    let table = format!("{}.pgderive_removals", settings.catalog_schema);
    let exists: bool = sql
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&table])
        .await?
        .try_get(0)?;
    if !exists {
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
    ensure!(row.try_get::<_, i32>(1)? <= 131_072, "removal metadata limit exceeded");
    let removal: Removal = serde_json::from_value(row.try_get(0)?)?;
    ensure!(removal.version == 1, "unsupported removal journal version");
    removal.source.validate()?;
    removal.sink.validate()?;
    Ok(Some(removal))
}

pub(super) async fn require_active(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<()> {
    ensure!(
        read(sql, settings).await?.is_none(),
        "query removal is pending or complete; ordinary startup cannot recreate it"
    );
    Ok(())
}

pub(super) async fn status(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<Option<&'static str>> {
    Ok(read(sql, settings)
        .await?
        .map(|removal| if removal.finished { "dropped" } else { "dropping" }))
}

/// Retire the journaled slot, result and membership without resetting source data.
///
/// # Errors
/// Rejects live ownership, changed resources, dependencies or uncertain operations.
/// Repeating the command reconciles its durable intent; it never deletes a prefix.
pub async fn drop_query(config: &Config) -> Result<Value> {
    config.validate()?;
    let settings = config.worker.as_ref().context("configuration requires a worker section")?;
    let mut session = Session::connect(config).await?;
    session.own(&settings.catalog_schema, &settings.query_id).await?;
    let removal = match read(&session.client, settings).await? {
        Some(removal) => removal,
        None => intent(&mut session.client, settings).await?,
    };
    ensure!(removal.prefix == settings.object_prefix, "removal object namespace changed");
    removal.source.identity.verify_sql(&session.client).await?;
    slot(&session.client, &removal.source).await?;
    finish(&mut session.client, settings, &removal).await?;
    let store = config
        .object_store
        .as_ref()
        .context("removal requires durable object storage")?
        .build(&removal.prefix)?;
    let catalog = Catalog::new(&settings.catalog_schema, &settings.query_id)?;
    let collection = catalog.collect(&mut session.client, store, GcLimits::default()).await?;
    Ok(
        json!({"version":1,"query_id":settings.query_id,"state":"dropped","collection":format!("{collection:?}")}),
    )
}

async fn load_removal(sql: &Client, settings: &Settings) -> Result<Removal> {
    let registration = format!("{}.pgderive_worker_registration", settings.catalog_schema);
    let exists: bool = sql
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&registration])
        .await?
        .try_get(0)?;
    ensure!(exists, "drop requires an existing registration");
    let row = sql
        .query_opt(
            &format!(
                "SELECT source,sink,slot_name,object_prefix FROM {registration} WHERE query_id=$1"
            ),
            &[&settings.query_id],
        )
        .await?
        .context("drop requires an existing registration")?;
    let source: Contract = serde_json::from_value(row.try_get(0)?)?;
    let sink: Sink = serde_json::from_value(row.try_get(1)?)?;
    source.validate()?;
    sink.validate()?;
    ensure!(
        source.slot == row.try_get::<_, String>(2)?,
        "removal journal slot differs from native contract"
    );
    ensure!(
        !source
            .relations
            .iter()
            .any(|relation| relation.schema == settings.catalog_schema
                && relation.table == sink.table()),
        "removal destination is a source table"
    );
    let views = format!("{}.pgderive_result_views", settings.catalog_schema);
    let exists: bool = sql
        .query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL", &[&views])
        .await?
        .try_get(0)?;
    let view = if exists {
        sql.query_opt(
            &format!("SELECT view_name FROM {views} WHERE query_id=$1"),
            &[&settings.query_id],
        )
        .await?
        .map(|row| row.try_get(0))
        .transpose()?
    } else {
        None
    };
    let sink_oid = owned_sink(sql, settings, &source, &sink).await?;
    let removal = Removal {
        version: 1,
        source,
        sink,
        sink_oid,
        prefix: row.try_get(3)?,
        view,
        finished: false,
    };
    Ok(removal)
}

async fn owned_sink(
    sql: &Client,
    settings: &Settings,
    source: &Contract,
    sink: &Sink,
) -> Result<Option<u32>> {
    let catalog = Catalog::new(&settings.catalog_schema, &settings.query_id)?;
    let Some(progress) = sql
        .query_opt(
            &format!(
                "SELECT binding FROM {}.pgderive_progress WHERE query_id=$1",
                settings.catalog_schema
            ),
            &[&settings.query_id],
        )
        .await?
    else {
        return Ok(None);
    };
    let binding: crate::catalog::Binding = serde_json::from_value(progress.try_get(0)?)?;
    ensure!(
        binding.sink == *sink && binding.registered_source()?.as_ref() == Some(source),
        "removal journal differs from activated source/destination"
    );
    Ok(Some(
        sink.owned(sql, &catalog)
            .await?
            .context("activated destination has no creation ownership proof")?,
    ))
}

async fn intent(sql: &mut Client, settings: &Settings) -> Result<Removal> {
    let removal = load_removal(sql, settings).await?;
    ensure!(removal.prefix == settings.object_prefix, "removal object namespace changed");
    removal.source.identity.verify_sql(sql).await?;
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    barrier(&tx, settings).await?;
    tx.execute(
        &format!(
            "UPDATE {}.pgderive_progress SET fence=fence+1 WHERE query_id=$1",
            settings.catalog_schema
        ),
        &[&settings.query_id],
    )
    .await?;
    tx.execute(&format!("UPDATE {}.pgderive_protections SET active=false WHERE query_id=$1 AND active AND (uploading OR recovering)",settings.catalog_schema), &[&settings.query_id]).await?;
    tx.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {}.pgderive_removals(query_id text PRIMARY KEY,state jsonb NOT NULL)",settings.catalog_schema)).await?;
    tx.execute(
        &format!("INSERT INTO {}.pgderive_removals VALUES($1,$2)", settings.catalog_schema),
        &[&settings.query_id, &serde_json::to_value(&removal)?],
    )
    .await?;
    tx.commit().await.context("removal intent COMMIT uncertain; repeat drop to reconcile")?;
    Ok(removal)
}

async fn barrier(sql: &tokio_postgres::Transaction<'_>, settings: &Settings) -> Result<()> {
    let scope = format!("pgderive-object-lifecycle:{}", settings.catalog_schema);
    sql.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))",
        &[&scope],
    )
    .await?;
    Ok(())
}

async fn slot(sql: &Client, source: &Contract) -> Result<()> {
    let Some(row) = sql.query_opt("SELECT plugin,database,temporary,active FROM pg_catalog.pg_replication_slots WHERE slot_name=$1", &[&source.slot]).await? else { return Ok(()); };
    ensure!(
        row.try_get::<_, String>(0)? == "pgoutput"
            && row.try_get::<_, String>(1)? == source.identity.database
            && !row.try_get::<_, bool>(2)?
            && !row.try_get::<_, bool>(3)?,
        "journaled removal slot is incompatible or still active"
    );
    sql.query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&source.slot]).await?;
    Ok(())
}

async fn finish(sql: &mut Client, settings: &Settings, removal: &Removal) -> Result<()> {
    if removal.finished {
        return Ok(());
    }
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    barrier(&tx, settings).await?;
    if let Some(view) = &removal.view {
        results::remove(&tx, settings, view).await?;
    }
    if let Some(oid) = removal.sink_oid {
        let catalog = Catalog::new(&settings.catalog_schema, &settings.query_id)?;
        tx.batch_execute(&format!(
            "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
            settings.catalog_schema,
            removal.sink.table()
        ))
        .await?;
        ensure!(
            removal.sink.owned(&tx, &catalog).await? == Some(oid),
            "retirement destination ownership changed"
        );
        tx.batch_execute(&format!(
            "DROP TABLE {}.{} RESTRICT",
            settings.catalog_schema,
            removal.sink.table()
        ))
        .await?;
    }
    tx.execute(
        &format!("DELETE FROM {}.pgderive_queries WHERE query_id=$1", settings.catalog_schema),
        &[&settings.query_id],
    )
    .await?;
    // Keep the ownership journal as a tombstone: it also binds this schema's
    // object prefix while external reader pins outlive the retired membership.
    let mut completed = removal.clone();
    completed.finished = true;
    tx.execute(
        &format!(
            "UPDATE {}.pgderive_removals SET state=$2 WHERE query_id=$1",
            settings.catalog_schema
        ),
        &[&settings.query_id, &serde_json::to_value(completed)?],
    )
    .await?;
    tx.commit().await.context("removal completion COMMIT uncertain; repeat drop to reconcile")?;
    Ok(())
}
