use super::Storage;
use anyhow::Result;
#[cfg(unix)]
fn credential_file() -> Result<tempfile::NamedTempFile> {
    use std::io::Write as _;
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(br#"{"access_key_id":"local","secret_access_key":"local"}"#)?;
    Ok(file)
}
#[cfg(unix)]
#[test]
fn storage_configuration_rejects_shared_buckets_and_remote_endpoints() -> Result<()> {
    let file = credential_file()?;
    let mut storage: Storage = serde_json::from_value(serde_json::json!({
        "endpoint":"http://127.0.0.1:8334","bucket":"pgderive-tests","credentials_file":file.path()
    }))?;
    storage.build("fixture", 16)?;
    assert!(storage.build("fixture", 0).is_err());
    assert!(storage.build("fixture", 257).is_err());
    storage.bucket = "dataflow".into();
    assert!(storage.validate().is_err());
    storage.bucket = "pgderive-tests".into();
    storage.endpoint = "https://remote".into();
    assert!(storage.validate().is_err());
    storage.endpoint = "http://localhost:8334".into();
    storage.credentials_file.clear();
    assert!(storage.validate().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn https_endpoints_require_region_and_reject_credential_bearing_urls() -> Result<()> {
    let file = credential_file()?;
    let mut storage: Storage = serde_json::from_value(serde_json::json!({
        "endpoint":"https://objects.example.test:9443","bucket":"application-results","region":"custom-region","credentials_file":file.path()
    }))?;
    storage.build("owned-prefix", 16)?;
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
    let storage: Storage = serde_json::from_value(
        serde_json::json!({"endpoint":"https://objects.example.test","bucket":"application-results","region":"custom-region","credentials_file":file.path()}),
    )?;
    storage.build("owned-prefix", 16)?;
    std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644))?;
    assert!(storage.build("owned-prefix", 16).is_err());
    std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(file.path(), br#""sensitive-value""#)?;
    let error = storage
        .build("owned-prefix", 16)
        .err()
        .ok_or_else(|| anyhow::anyhow!("invalid credentials accepted"))?;
    assert!(!format!("{error:#}").contains("sensitive-value"));
    std::fs::write(file.path(), vec![b' '; 65_537])?;
    assert!(storage.build("owned-prefix", 16).is_err());
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
        .build("owned-prefix", 16)
        .err()
        .ok_or_else(|| anyhow::anyhow!("FIFO credentials accepted"))?;
    assert!(format!("{error:#}").contains("regular file"));
    Ok(())
}

#[test]
fn storage_requires_a_file_and_rejects_inline_credential_fields() {
    let mut config =
        serde_json::json!({"endpoint":"http://127.0.0.1:8334","bucket":"pgderive-tests"});
    assert!(serde_json::from_value::<Storage>(config.clone()).is_err());
    config["credentials_file"] = serde_json::json!("fixture.json");
    for field in ["access_key_id", "secret_access_key", "session_token"] {
        let mut invalid = config.clone();
        invalid[field] = serde_json::json!("removed-inline-value");
        assert!(serde_json::from_value::<Storage>(invalid).is_err());
    }
}

#[tokio::test]
async fn shared_limit_covers_streamed_gets_and_preserves_bulk_delete() -> Result<()> {
    use crate::engine::tests::store::Store;
    use futures_util::{StreamExt as _, TryStreamExt as _};
    use object_store::{ObjectStoreExt as _, path::Path};
    use std::{
        future::{Future as _, poll_fn},
        sync::{Arc, atomic::Ordering},
        task::Poll,
    };
    let inner = Arc::new(Store::unpaced());
    let store = super::limit(inner.clone(), 1);
    let first = Path::from("first");
    store.put(&first, vec![1_u8].into()).await?;
    let body = store.get(&first).await?;
    let second = Path::from("second");
    let mut put = Box::pin(store.put(&second, vec![2_u8].into()));
    poll_fn(|cx| {
        assert!(put.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(inner.puts.load(Ordering::SeqCst), 1);
    // Dropping an unread body must release admission as well as consuming it.
    drop(body);
    put.await?;
    let mut listing = store.list(None);
    assert!(listing.try_next().await?.is_some());
    let third = Path::from("third");
    let mut put = Box::pin(store.put(&third, vec![3_u8].into()));
    poll_fn(|cx| {
        assert!(put.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(listing);
    put.await?;
    let paths = vec![first, second, third];
    let deleted = store
        .delete_stream(futures_util::stream::iter(paths.into_iter().map(Ok)).boxed())
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(deleted.len(), 3);
    assert_eq!(inner.deletes.load(Ordering::SeqCst), 1);
    assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    Ok(())
}
