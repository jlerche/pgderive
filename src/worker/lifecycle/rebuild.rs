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
    Ok(session)
}

async fn signature(config: &Config) -> Result<String> {
    let mut value = admin::validate(config).await?;
    value.as_object_mut().context("invalid validation description")?.remove("query_id");
    // Validation includes normalized IR, compiler/codec plan identity, native source
    // layout and PostgreSQL's output labels/type modifiers. Pending state never
    // uses the generation's randomized slot alias as its semantic signature.
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
    let dropped = super::super::retire::status(&session.client, settings).await? == Some("dropped");
    if prior.is_none() && !dropped {
        super::super::retire::require_active(&session.client, settings).await?;
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
    let mut state = prior.unwrap_or_else(|| State {
        version: 1,
        base: Base::new(config, settings),
        active: (!dropped).then(|| Generation::original(config, settings)),
        pending: None,
        signature: String::new(),
        presentation: format!("presentation:{token}"),
        public_owner: settings.query_id.clone(),
        phase: Phase::Active,
        retired: Vec::new(),
    });
    if state.phase == Phase::Dropped {
        state.active = None;
    }
    state.pending = Some(generation.clone());
    state.signature = signature.into();
    state.phase = Phase::Building;
    let tx = session.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    save(&tx, settings, &state).await?;
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
    let mut old = settings.clone();
    old.query_id = state.public_owner.clone();
    let mut new = settings.clone();
    new.query_id = state.presentation.clone();
    new.sink_table = generation.sink.clone();
    let tx = opened.session.client.transaction().await?;
    tx.batch_execute(
        "SET LOCAL synchronous_commit=on; SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,MDY'",
    )
    .await?;
    super::super::results::replace(&tx, &old, &new, &opened.compiled).await?;
    if let Some(old) = state.active.replace(generation) {
        state.retired.push(old);
    }
    state.pending = None;
    state.public_owner.clone_from(&state.presentation);
    state.phase = Phase::Active;
    save(&tx, settings, state).await?;
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
        let (native_config, native) = generation.apply(config, settings)?;
        super::super::retire::execute(&native_config, &native, session).await?;
        state.retired.remove(0);
        let tx = session.client.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        save(&tx, settings, state).await?;
        tx.commit().await.context("retirement bookkeeping COMMIT uncertain; repeat rebuild")?;
    }
    Ok(())
}
