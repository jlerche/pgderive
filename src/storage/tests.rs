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

#[test]
fn https_endpoints_require_region_and_reject_credential_bearing_urls() -> Result<()> {
    let mut storage: Storage = serde_json::from_str(
        r#"{"endpoint":"https://objects.example.test:9443","bucket":"application-results","region":"custom-region","access_key_id":"local","secret_access_key":"local"}"#,
    )?;
    storage.build("owned-prefix")?;
    for endpoint in [
        "http://remote:9000",
        "http://localhost:0",
        "https://key:secret@objects.example.test",
        "https://objects.example.test/path",
        "https://objects.example.test?token=secret",
        "https://objects.example.test#fragment",
        "file:///tmp/objects",
    ] {
        storage.endpoint = endpoint.into();
        assert!(storage.validate().is_err());
    }
    storage.endpoint = "https://objects.example.test".into();
    storage.region = None;
    assert!(storage.validate().is_err());
    storage.region = Some(" ".into());
    assert!(storage.validate().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn credential_files_are_private_bounded_and_do_not_expose_secrets_in_errors() -> Result<()> {
    use std::{io::Write as _, os::unix::fs::PermissionsExt as _};
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(br#"{"access_key_id":"local","secret_access_key":"sensitive-value","session_token":"temporary"}"#)?;
    let mut storage: Storage = serde_json::from_value(
        serde_json::json!({"endpoint":"https://objects.example.test","bucket":"application-results","region":"custom-region","credentials_file":file.path()}),
    )?;
    storage.build("owned-prefix")?;
    storage.access_key_id = "conflicting".into();
    assert!(storage.validate().is_err());
    storage.access_key_id.clear();
    std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644))?;
    assert!(storage.build("owned-prefix").is_err());
    std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(file.path(), br#""sensitive-value""#)?;
    let error = storage
        .build("owned-prefix")
        .err()
        .ok_or_else(|| anyhow::anyhow!("invalid credentials accepted"))?;
    assert!(!format!("{error:#}").contains("sensitive-value"));
    std::fs::write(file.path(), vec![b' '; 65_537])?;
    assert!(storage.build("owned-prefix").is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn credential_fifo_is_rejected_without_waiting_for_a_writer() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("credentials.fifo");
    rustix::fs::mkfifoat(rustix::fs::CWD, &path, rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR)?;
    let storage: Storage = serde_json::from_value(serde_json::json!({
        "endpoint":"https://objects.example.test", "bucket":"application-results",
        "region":"custom-region", "credentials_file":path
    }))?;
    let error = storage
        .build("owned-prefix")
        .err()
        .ok_or_else(|| anyhow::anyhow!("FIFO credentials accepted"))?;
    assert!(format!("{error:#}").contains("regular file"));
    Ok(())
}
