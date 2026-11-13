use super::{Config, identifier};
use anyhow::Result;
use serde_json::json;

fn fixture() -> Result<Config> {
    Ok(serde_json::from_value(json!({
        "postgres":{"host":"localhost","port":5432,"database":"pgderive_dev","user":"postgres","tls":"disable"},
        "replication":{"slot":"pgderive_slot","publication":"pgderive_pub"}
    }))?)
}

#[test]
fn defaults_and_validation() -> Result<()> {
    let config = fixture()?;
    config.validate()?;
    assert_eq!(config.listener.max_transactions, 0);
    assert_eq!(config.listener.max_transaction_changes, 100_000);
    assert_eq!(config.replication_config().port, 5432);
    drop(config);
    Ok(())
}

#[test]
fn reject_invalid_limits_and_tls() -> Result<()> {
    let mut config = fixture()?;
    config.listener.max_transaction_changes = 0;
    assert!(config.validate().is_err());
    config.listener.max_transaction_changes = 1;
    config.postgres.tls = "unverified".to_owned();
    assert!(config.validate().is_err());
    config.postgres.tls = "verify-full".to_owned();
    config.validate()?;
    drop(config);
    Ok(())
}

#[test]
fn reject_unknown_configuration_fields() {
    let value = json!({
        "postgres":{"host":"localhost","port":5432,"database":"dev","user":"postgres","tls":"disable","typo":true},
        "replication":{"slot":"slot","publication":"pub"}
    });
    assert!(serde_json::from_value::<Config>(value).is_err());
}

#[test]
fn reject_unsafe_sql_identifiers() {
    assert!(identifier("pgderive_dev_slot"));
    for value in ["", "1slot", "slot; DROP TABLE x", "a\"b", "a.b"] {
        assert!(!identifier(value));
    }
}

#[tokio::test]
async fn public_entry_points_validate_before_connecting() -> Result<()> {
    let mut config = fixture()?;
    config.replication.publication = "pgderive_pub; DROP TABLE source".into();
    for result in
        [crate::listen(config.clone()).await, Box::pin(crate::run_harness(config.clone())).await]
    {
        let error = result.err().ok_or_else(|| anyhow::anyhow!("expected validation error"))?;
        assert_eq!(error.to_string(), "invalid replication.publication identifier");
    }
    drop(config);
    Ok(())
}

#[test]
fn replication_pins_canonical_native_text_settings() -> Result<()> {
    let settings = fixture()?.replication_config();
    assert_eq!(settings.max_message_size, crate::replication::MAX_PAYLOAD);
    assert_eq!(
        settings.options.as_deref(),
        Some("-c DateStyle=ISO,MDY -c TimeZone=UTC -c IntervalStyle=iso_8601")
    );
    Ok(())
}

#[test]
fn worker_requires_explicit_independent_tenant_storage_authority() -> Result<()> {
    let mut config = fixture()?;
    config.worker = Some(serde_json::from_value(
        json!({"tenant_id":"fixture","catalog_schema":"demo","query_id":"q","sink_table":"result","object_prefix":"prefix","block_rows":64,"maintenance_ticks":0,"retry_attempts":1,"retry_delay_ms":1,"max_transactions":0,"query":{"sql":"SELECT 1"}}),
    )?);
    assert!(config.validate().is_err());
    config.tenant_storage = Some(serde_json::from_value(
        json!({"postgres":{"host":"localhost","port":5432,"database":"tenant_control","user":"publisher","tls":"disable"},"schema":"tenant_storage","object_prefix":"tenant-packs"}),
    )?);
    config.validate()?;
    config
        .tenant_storage
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("missing tenant storage"))?
        .schema = "public;DROP TABLE source".into();
    assert!(config.validate().is_err());
    drop(config);
    Ok(())
}
