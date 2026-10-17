//! Recoverable removal of the stable logical query and all its generations.
use super::{Phase, own, read, save};
use crate::{Config, worker::retire};
use anyhow::{Context, Result};
use serde_json::{Value, json};

/// Remove a logical query and reconcile any partially built or retired generations.
///
/// # Errors
/// Rejects live ownership, changed resources, dependencies and uncertain commits.
pub async fn drop_query(config: &Config) -> Result<Value> {
    let (mut session, settings) = own(config).await?;
    let mut state = existing(config, &settings, &session).await?;
    anyhow::ensure!(
        state.base == super::Base::new(config, &settings),
        "logical query namespace/source configuration changed"
    );
    if state.phase == Phase::Dropped {
        return collected(config, &settings, &mut session).await;
    }
    // The first generation intent persists this phase together with its fence/resources.
    state.phase = Phase::Dropping;
    remove_generations(config, &mut session, &settings, &mut state).await?;
    state.active = None;
    state.pending = None;
    state.retired.clear();
    state.phase = Phase::Dropped;
    let tx = session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    save(&tx, &settings, &state, "dropped").await?;
    tx.commit().await.context("logical removal completion COMMIT uncertain; repeat drop")?;
    collected(config, &settings, &mut session).await
}

async fn existing(
    config: &Config,
    settings: &crate::worker::Settings,
    session: &crate::worker::sql::Session,
) -> Result<super::State> {
    if let Some(state) = read(&session.client, settings).await? {
        return Ok(state);
    }
    anyhow::ensure!(
        super::rebuild::registered(&session.client, settings, &settings.query_id).await?,
        "drop requires an existing registration"
    );
    Ok(super::State::original(config, settings))
}

async fn remove_generations(
    config: &Config,
    session: &mut crate::worker::sql::Session,
    settings: &crate::worker::Settings,
    state: &mut super::State,
) -> Result<()> {
    let generations: Vec<_> = state
        .retired
        .iter()
        .chain(state.active.iter())
        .chain(state.pending.iter())
        .cloned()
        .collect();
    for generation in generations {
        if super::rebuild::registered(&session.client, settings, &generation.query).await? {
            let (_, native) = generation.apply(config, settings)?;
            retire::execute(&native, settings, session, state).await?;
        }
    }
    Ok(())
}

pub(super) async fn collected(
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
