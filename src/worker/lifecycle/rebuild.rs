//! Fresh snapshot generations and an atomic application presentation switch.
use super::{Base, Generation, Phase, State, own, read, save};
use crate::{
    Config,
    worker::{admin, open::Open, spec::Settings, sql::Session},
};
use anyhow::{Context, Result, ensure};
use serde_json::Value;

/// Bootstrap a fresh generation, atomically switch its public result, and retire the old one.
///
/// # Errors
/// Rejects ownership, unsupported SQL, changed pending semantics or failed cutover/retirement.
pub async fn rebuild(config: &Config) -> Result<Value> {
    let (session, settings) = own(config).await?;
    let session = run_owned(config, &settings, session, false).await?;
    drop(session);
    admin::status(config).await
}

pub(in crate::worker) async fn run_owned(
    config: &Config,
    settings: &Settings,
    mut session: Session,
    recreate: bool,
) -> Result<Session> {
    let signature = signature(config).await?;
    let mut state = prepare(config, settings, &mut session, &signature, recreate).await?;
    if state.phase == Phase::Building {
        let generation =
            state.pending.clone().context("building lifecycle has no pending generation")?;
        if let Some(old) = &state.active {
            session.own(&settings.catalog_schema, &old.query).await?;
        }
        let (native_config, native_settings) = generation.apply(config, settings)?;
        let mut opened = Open::raw(&native_config, &native_settings, session).await?;
        boundary(&opened, settings, &state).await?;
        cutover(&mut opened, settings, &mut state, generation).await?;
        session = opened.session;
    }
    retire(config, settings, &mut session, &mut state).await?;
    super::control::collected(config, settings, &mut session).await?;
    Ok(session)
}

async fn signature(config: &Config) -> Result<String> {
    semantic_description(admin::validate(config).await?)
}

fn semantic_description(mut value: Value) -> Result<String> {
    let description = value.as_object_mut().context("invalid validation description")?;
    description.remove("query_id");
    // Placement is advisory and must not change a pending rebuild's semantic
    // signature merely because diagnostics improve between engine versions.
    Ok(serde_json::to_string(&value)?)
}

async fn prepare(
    config: &Config,
    settings: &Settings,
    session: &mut Session,
    signature: &str,
    recreate: bool,
) -> Result<State> {
    let prior = read(&session.client, settings).await?;
    if let Some(state) = &prior {
        ensure!(
            state.base == Base::new(config, settings),
            "logical query namespace/source configuration changed"
        );
        if state.phase == Phase::Building
            || (state.phase == Phase::Active && !state.retired.is_empty())
        {
            ensure!(
                state.signature == signature,
                "pending rebuild semantics changed; resume the original SQL or drop explicitly"
            );
            return Ok(state.clone());
        }
        ensure!(state.phase != Phase::Dropping, "query removal is pending; resume drop");
        ensure!(!recreate || state.phase == Phase::Dropped, "create requires a dropped query");
    }
    if prior.is_none() {
        ensure!(
            registered(&session.client, settings, &settings.query_id).await?,
            "rebuild requires an existing registration"
        );
    }
    let token: String = session
        .client
        .query_one("SELECT replace(pg_catalog.gen_random_uuid()::text,'-','')", &[])
        .await?
        .try_get(0)?;
    let generation = Generation {
        query: format!("generation:{token}"),
        slot: format!("generation_{token}"),
        sink: format!("result_{token}"),
    };
    let mut state = prior.unwrap_or_else(|| State::original(config, settings));
    if state.phase == Phase::Dropped {
        state.active = None;
    }
    state.pending = Some(generation.clone());
    state.signature = signature.into();
    state.phase = Phase::Building;
    let tx = session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    save(&tx, settings, &state, "building").await?;
    tx.execute(
        &format!("INSERT INTO {}.pgderive_generations VALUES($1,$2)", settings.catalog_schema),
        &[&generation.query, &settings.query_id],
    )
    .await?;
    tx.commit().await.context("rebuild intent COMMIT uncertain; repeat the original operation")?;
    Ok(state)
}

pub(super) async fn registered(
    sql: &tokio_postgres::Client,
    settings: &Settings,
    query: &str,
) -> Result<bool> {
    let table = format!("{}.pgderive_worker_registration", settings.catalog_schema);
    if !super::exists(sql, &table).await? {
        return Ok(false);
    }
    Ok(sql
        .query_opt(&format!("SELECT query_id FROM {table} WHERE query_id=$1"), &[&query])
        .await?
        .is_some())
}

async fn boundary(opened: &Open, settings: &Settings, state: &State) -> Result<()> {
    let Some(old) = &state.active else {
        return Ok(());
    };
    let row = opened
        .session
        .client
        .query_opt(
            &format!(
                "SELECT end_lsn::text,binding FROM {}.pgderive_progress WHERE query_id=$1",
                settings.catalog_schema
            ),
            &[&old.query],
        )
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let old_end: String = row.try_get(0)?;
    let binding: crate::catalog::Binding = serde_json::from_value(row.try_get(1)?)?;
    binding
        .registered_source()?
        .context("old generation has no source identity")?
        .identity
        .verify_sql(&opened.session.client)
        .await?;
    let new_end: String = opened
        .session
        .client
        .query_one(
            &format!(
                "SELECT end_lsn::text FROM {}.pgderive_progress WHERE query_id=$1",
                settings.catalog_schema
            ),
            &[&state.pending.as_ref().context("missing pending generation")?.query],
        )
        .await?
        .try_get(0)?;
    ensure!(
        opened
            .session
            .client
            .query_one("SELECT $1::text::pg_lsn >= $2::text::pg_lsn", &[&new_end, &old_end])
            .await?
            .try_get::<_, bool>(0)?,
        "replacement snapshot precedes the frozen old generation"
    );
    Ok(())
}

async fn cutover(
    opened: &mut Open,
    settings: &Settings,
    state: &mut State,
    generation: Generation,
) -> Result<()> {
    let tx = opened.session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    let mut generation = generation;
    if let Some(mut old) = state.active.take() {
        super::tables::cutover(&tx, settings, &mut old, &mut generation).await?;
        state.retired.push(old);
    } else {
        super::tables::rename(&tx, settings, &mut generation).await?;
    }
    state.active = Some(generation);
    state.pending = None;
    state.phase = Phase::Active;
    save(&tx, settings, state, "cutover").await?;
    tx.commit().await.context("rebuild cutover COMMIT uncertain; repeat rebuild to reconcile")?;
    Ok(())
}

async fn retire(
    config: &Config,
    settings: &Settings,
    session: &mut Session,
    state: &mut State,
) -> Result<()> {
    while let Some(generation) = state.retired.first().cloned() {
        let (_, native) = generation.apply(config, settings)?;
        super::super::retire::execute(&native, settings, session, state).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
