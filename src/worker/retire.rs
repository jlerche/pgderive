//! Journaled query removal; reader roots remain protected after membership retirement.
use super::{spec::Settings, sql::Session};
use crate::{
    catalog::{Catalog, Sink},
    source::Contract,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Removal {
    query: String,
    source: Contract,
    sink: Sink,
    sink_oid: Option<u32>,
    prefix: String,
}

impl Removal {
    pub(super) fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.sink.validate()
    }
}

pub(super) async fn execute(
    settings: &Settings,
    logical: &Settings,
    session: &mut Session,
    state: &mut super::lifecycle::State,
) -> Result<()> {
    session.own(&settings.catalog_schema, &settings.query_id).await?;
    if state.removal.is_none() {
        intent(&mut session.client, settings, logical, state).await?;
    }
    let removal = state.removal.clone().context("missing generation removal intent")?;
    ensure!(removal.query == settings.query_id, "generation removal order changed");
    ensure!(removal.prefix == settings.object_prefix, "removal object namespace changed");
    removal.source.identity.verify_sql(&session.client).await?;
    super::registration::drop_slot(&session.client, &removal.source).await?;
    finish(&mut session.client, settings, logical, state, &removal).await?;
    Ok(())
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
    let sink_oid = owned_sink(sql, settings, &source, &sink).await?;
    let removal = Removal {
        query: settings.query_id.clone(),
        source,
        sink,
        sink_oid,
        prefix: row.try_get(3)?,
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

async fn intent(
    sql: &mut Client,
    settings: &Settings,
    logical: &Settings,
    state: &mut super::lifecycle::State,
) -> Result<()> {
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
    state.removal = Some(removal);
    super::lifecycle::save(&tx, logical, state, "removal-intent").await?;
    tx.commit().await.context("removal intent COMMIT uncertain; repeat drop to reconcile")?;
    Ok(())
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

async fn finish(
    sql: &mut Client,
    settings: &Settings,
    logical: &Settings,
    state: &mut super::lifecycle::State,
    removal: &Removal,
) -> Result<()> {
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    barrier(&tx, settings).await?;
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
    // Registration retains the object prefix while independent reader pins outlive
    // membership. Resource retirement and generation progress share this COMMIT.
    state.forget(&settings.query_id);
    super::lifecycle::save(&tx, logical, state, "removal-complete").await?;
    tx.commit().await.context("removal completion COMMIT uncertain; repeat drop to reconcile")?;
    Ok(())
}
