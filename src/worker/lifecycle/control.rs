//! Recoverable removal of the stable logical query and all its generations.
use super::{Phase, own, read, save};
use crate::{
    Config,
    worker::{results, retire},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};

/// Remove a logical query and reconcile any partially built or retired generations.
///
/// # Errors
/// Rejects live ownership, changed resources, dependencies and uncertain commits.
pub async fn drop_query(config: &Config) -> Result<Value> {
    let (mut session, settings) = own(config).await?;
    let Some(mut state) = read(&session.client, &settings).await? else {
        return retire::execute(config, &settings, &mut session).await;
    };
    anyhow::ensure!(
        state.base == super::Base::new(config, &settings),
        "logical query namespace/source configuration changed"
    );
    if state.phase == Phase::Dropped {
        return collected(config, &settings, &mut session).await;
    }
    state.phase = Phase::Dropping;
    let tx = session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    save(&tx, &settings, &state).await?;
    tx.commit().await.context("logical removal intent COMMIT uncertain; repeat drop")?;
    remove_view(&mut session, &settings, &state).await?;
    remove_generations(config, &mut session, &settings, &state).await?;
    state.active = None;
    state.pending = None;
    state.retired.clear();
    state.phase = Phase::Dropped;
    let tx = session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    save(&tx, &settings, &state).await?;
    tx.commit().await.context("logical removal completion COMMIT uncertain; repeat drop")?;
    collected(config, &settings, &mut session).await
}

async fn remove_view(
    session: &mut crate::worker::sql::Session,
    settings: &crate::worker::Settings,
    state: &super::State,
) -> Result<()> {
    if let Some(view) = &settings.result_view {
        let mut presentation = settings.clone();
        presentation.query_id = state.public_owner.clone();
        let table = format!("{}.pgderive_result_views", settings.catalog_schema);
        if super::exists(&session.client, &table).await?
            && session
                .client
                .query_opt(
                    &format!("SELECT query_id FROM {table} WHERE query_id=$1"),
                    &[&presentation.query_id],
                )
                .await?
                .is_some()
        {
            let tx = session.client.transaction().await?;
            tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
            results::remove(&tx, &presentation, view).await?;
            tx.commit().await.context("logical result removal COMMIT uncertain; repeat drop")?;
        }
    }
    Ok(())
}

async fn remove_generations(
    config: &Config,
    session: &mut crate::worker::sql::Session,
    settings: &crate::worker::Settings,
    state: &super::State,
) -> Result<()> {
    let generations = state.retired.iter().chain(state.active.iter()).chain(state.pending.iter());
    for generation in generations {
        if super::rebuild::registered(&session.client, settings, &generation.query).await? {
            let (native_config, native) = generation.apply(config, settings)?;
            retire::execute(&native_config, &native, session).await?;
        }
    }
    Ok(())
}

async fn collected(
    config: &Config,
    settings: &crate::worker::Settings,
    session: &mut crate::worker::sql::Session,
) -> Result<Value> {
    let store = config
        .object_store
        .as_ref()
        .context("removal requires durable object storage")?
        .build(&settings.object_prefix)?;
    let catalog = crate::catalog::Catalog::new(&settings.catalog_schema, &settings.query_id)?;
    let collection =
        catalog.collect(&mut session.client, store, crate::catalog::GcLimits::default()).await?;
    Ok(
        json!({"version":1,"query_id":settings.query_id,"state":"dropped","collection":format!("{collection:?}")}),
    )
}
