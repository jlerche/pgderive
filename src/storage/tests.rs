use super::Storage;
use anyhow::Result;
#[test]
fn storage_configuration_rejects_shared_buckets_and_remote_endpoints() -> Result<()> {
    let mut storage: Storage = serde_json::from_str(
        r#"{"endpoint":"http://127.0.0.1:8334","bucket":"pgderive-tests","access_key_id":"local","secret_access_key":"local"}"#,
    )?;
    storage.build("fixture")?;
    storage.bucket = "dataflow".into();
    assert!(storage.validate().is_err());
    storage.bucket = "pgderive-tests".into();
    storage.endpoint = "https://remote".into();
    assert!(storage.validate().is_err());
    storage.endpoint = "http://localhost:8334".into();
    storage.access_key_id.clear();
    assert!(storage.validate().is_err());
    Ok(())
}
