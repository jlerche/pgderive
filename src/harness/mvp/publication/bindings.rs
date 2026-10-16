//! Low-level mapped sink initialization/reopen; synthetic progress never authorizes ACK.
use crate::{
    Config,
    catalog::{
        Binding, Catalog, Deltas, Lsn, Progress, Publication, Sink, Terminal, terminal::OutputMap,
    },
    engine::{
        Batch,
        plan::{Checkpoint, Plan},
    },
};
use anyhow::{Result, ensure};
use serde_json::Value;
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    schema: &str,
    plan: &Plan,
    empty: &Checkpoint,
    config: &Config,
) -> Result<()> {
    let mut map = Terminal::new(
        vec![OutputMap::TimestampFormat { pattern: "YYYY-MM-DD".into(), utc: false }],
        vec![1114],
    )?;
    map.bind(sql).await?;
    let abbreviations: String =
        sql.query_one("SHOW timezone_abbreviations", &[]).await?.try_get(0)?;
    let catalog = Catalog::new(schema, "mapped_reopen")?;
    catalog.checkpoint(sql, plan, empty, 0).await?;
    let binding = Binding {
        source: "mapped-reopen-synthetic".into(),
        sink: Sink::MappedBag { table: "mapped_reopen_rows".into(), map },
    };
    let initial: Lsn = "0/0".parse()?;
    catalog.claim(sql, plan, binding.clone(), initial).await?;
    let before = boundary(sql, schema).await?;
    let (mut fresh, _connection) = crate::harness::connect(config).await?;
    fresh.batch_execute("SET search_path=public,pg_catalog; SET TimeZone='America/Los_Angeles'; SET DateStyle='SQL,DMY'; SET IntervalStyle='postgres'; SET extra_float_digits=-15; SET timezone_abbreviations='Australia'").await?;
    ensure!(
        catalog.claim(&mut fresh, plan, binding.clone(), initial).await.is_err(),
        "low-level reopen accepted changed terminal context"
    );
    ensure!(boundary(sql, schema).await? == before, "rejected reopen advanced progress/fence");
    fresh
        .query_one(
            "SELECT pg_catalog.set_config('timezone_abbreviations',$1,false)",
            &[&abbreviations],
        )
        .await?;
    let mut writer = catalog.claim(&mut fresh, plan, binding, initial).await?;
    verify_context(&fresh).await?;
    let mut checkpoint = empty.clone();
    super::bags::advance(&mut checkpoint);
    let deltas = Deltas::bag(&Batch::from_updates([(
        (None::<String>, vec!["2026-10-15 12:34:00".to_owned()]),
        1,
    )])?)?;
    writer
        .publish(
            &mut fresh,
            plan,
            Publication {
                checkpoint: &checkpoint,
                progress: &Progress::new(1, "0/1", "0/2")?,
                deltas: &deltas,
            },
        )
        .await?;
    let value: Value = sql
        .query_one(&format!("SELECT tuple FROM {schema}.mapped_reopen_rows"), &[])
        .await?
        .try_get(0)?;
    ensure!(value == serde_json::json!([null, ["2026-10-15"]]), "reopened terminal result differs");
    eprintln!(
        "MVP mapped sink cold claim checked terminal bindings and initialized a fresh session"
    );
    Ok(())
}

async fn boundary(sql: &Client, schema: &str) -> Result<Value> {
    Ok(sql.query_one(&format!("SELECT to_jsonb(p) FROM {schema}.pgderive_progress p WHERE query_id='mapped_reopen'"), &[]).await?.try_get(0)?)
}

async fn verify_context(sql: &Client) -> Result<()> {
    let settings = sql.query_one("SELECT current_setting('search_path'),current_setting('TimeZone'),current_setting('DateStyle'),current_setting('IntervalStyle'),current_setting('extra_float_digits')", &[]).await?;
    for (index, expected) in
        ["pg_catalog", "UTC", "ISO, MDY", "iso_8601", "3"].into_iter().enumerate()
    {
        ensure!(
            settings.try_get::<_, String>(index)? == expected,
            "reopen did not initialize SQL context"
        );
    }
    Ok(())
}
