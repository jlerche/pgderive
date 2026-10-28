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
    use object_store::{ObjectStore as _, ObjectStoreExt as _, path::Path};
    use std::{
        future::{Future as _, poll_fn},
        sync::{Arc, atomic::Ordering},
        task::Poll,
    };
    let inner = Arc::new(Store::unpaced());
    let store = super::scoped::Scoped::new(inner.clone(), inner.clone(), Path::default(), 1)?;
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

#[tokio::test]
async fn pages_share_admission_release_it_and_translate_scoped_paths() -> Result<()> {
    use super::{PageStore as _, scoped::Scoped};
    use futures_util::TryStreamExt as _;
    use object_store::{
        ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path, prefix::PrefixStore,
    };
    use std::{
        future::{Future as _, poll_fn},
        sync::Arc,
        task::Poll,
    };
    let raw = Arc::new(InMemory::new());
    let prefix = Path::from("private/demo");
    let inner = Arc::new(PrefixStore::new(raw.clone(), prefix.clone()));
    let scoped = Scoped::new(inner, raw, prefix, 1)?;
    let first = Path::from("pgderive/first");
    scoped.put(&first, vec![1_u8].into()).await?;
    scoped.put(&Path::from("pgderive/second"), vec![2_u8].into()).await?;
    let body = scoped.get(&first).await?;
    let mut waiting = Box::pin(scoped.page("pgderive/", None, 1));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // Cancellation of a queued page cannot consume the sole request permit.
    drop(waiting);
    drop(body);
    let page = scoped.page("pgderive/", None, 1).await?;
    assert_eq!(page.result.objects.len(), 1);
    assert_eq!(page.result.objects[0].location, first);
    assert!(page.page_token.is_some());
    // Page response owns no request permit; PUT proceeds before using its token.
    scoped.delete(&first).await?;
    let next = scoped.page("pgderive/", page.page_token, 1).await?;
    assert_eq!(next.result.objects[0].location.as_ref(), "pgderive/second");
    assert!(next.page_token.is_none());
    assert_eq!(scoped.list(None).try_collect::<Vec<_>>().await?.len(), 1);
    assert!(scoped.put_multipart(&first).await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn maintenance_requests_leave_headroom_and_account_actual_consumption() -> Result<()> {
    use super::PageStore as _;
    use crate::engine::execution::{Workspaces, service};
    use anyhow::Context as _;
    use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
    use std::sync::Arc;
    let raw = Arc::new(InMemory::new());
    let store = Arc::new(super::scoped::Scoped::new(raw.clone(), raw, Path::default(), 2)?);
    let pool = Arc::new(Workspaces::new(1)?);
    let key = Path::from("physical");
    let (result, report) = service::scope(pool.clone(), async {
        let _workspace = pool.background().await?;
        store.put(&key, vec![7_u8; 1024].into()).await?;
        let body = store.get(&key).await?;
        // A streamed maintenance request owns one of two slots. Foreground must
        // still enter, and dropping an unread stream must not count body bytes.
        assert_eq!(store.service_stats().context("missing request stats")?.admission.active, 1);
        let foreground = store.clone();
        let key = key.clone();
        let bytes =
            tokio::spawn(async move { foreground.get(&key).await?.bytes().await }).await??;
        assert_eq!(bytes.len(), 1024);
        drop(body);
        Ok::<_, anyhow::Error>(())
    })
    .await;
    result?;
    assert_eq!(report.requests, 2);
    assert_eq!(report.write_requested_bytes, 1024);
    assert_eq!(report.written_bytes, 1024);
    assert_eq!(report.read_requested_bytes, 1024);
    assert_eq!(report.read_bytes, 0);
    let bytes = store.get(&key).await?.bytes().await?;
    assert_eq!(bytes.len(), 1024);
    let stats = store.service_stats().context("missing request stats")?;
    assert_eq!(stats.foreground.read_bytes, 2048);
    assert_eq!(stats.maintenance.written_bytes, 1024);
    assert_eq!(stats.admission.active, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_and_capacity_one_requests_keep_attempts_distinct_from_success() -> Result<()> {
    use crate::engine::{
        execution::{Workspaces, service},
        tests::store::Store,
    };
    use object_store::{ObjectStoreExt as _, path::Path};
    use std::sync::Arc;
    let inner = Arc::new(Store::new(Some(0)));
    let store = super::scoped::Scoped::new(inner.clone(), inner, Path::default(), 1)?;
    let pool = Arc::new(Workspaces::new(1)?);
    let (result, report) = service::scope(pool.clone(), async {
        let _phase = pool.background().await?;
        let path = Path::from("failed");
        assert!(store.put(&path, vec![3_u8; 512].into()).await.is_err());
        store.put(&path, vec![4_u8; 512].into()).await?;
        assert!(store.get_range(&path, 0..512).await?.iter().all(|v| *v == 4));
        assert!(store.get_range(&Path::from("missing"), 0..128).await.is_err());
        Ok::<_, anyhow::Error>(())
    })
    .await;
    result?;
    assert_eq!(report.requests, 4);
    assert_eq!(report.write_requested_bytes, 1024);
    assert_eq!(report.written_bytes, 512);
    assert_eq!(report.read_requested_bytes, 640);
    assert_eq!(report.read_bytes, 512);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn continuous_requests_leave_foreground_headroom_and_clear_maintenance_queue() -> Result<()> {
    use super::PageStore as _;
    use crate::engine::{
        execution::{Workspaces, service},
        tests::store::Store,
    };
    use anyhow::Context as _;
    use futures_util::{StreamExt as _, TryStreamExt as _};
    use object_store::{ObjectStoreExt as _, path::Path};
    use std::sync::{Arc, atomic::Ordering};
    let mut raw = Store::new(None);
    raw.read_delay = std::time::Duration::from_millis(10);
    let raw = Arc::new(raw);
    let store = Arc::new(super::scoped::Scoped::new(raw.clone(), raw.clone(), Path::default(), 2)?);
    let key = Path::from("foreground");
    store.put(&key, vec![1_u8; 16].into()).await?;
    let background = store.clone();
    let pool = Arc::new(Workspaces::new(1)?);
    let task = tokio::spawn(async move {
        service::scope(pool.clone(), maintenance_burst(background, pool)).await
    });
    while raw.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let started = tokio::time::Instant::now();
    assert_eq!(store.get_range(&key, 0..16).await?.len(), 16);
    assert_eq!(started.elapsed(), std::time::Duration::from_millis(10));
    let foreground = futures_util::stream::iter(0..512)
        .map(|_| {
            let store = store.clone();
            let key = key.clone();
            async move { store.get_range(&key, 0..16).await }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>();
    let (responses, completed) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(foreground, task)
    })
    .await?;
    assert_eq!(responses?.len(), 512);
    let (result, report) = completed?;
    result?;
    assert_eq!(report.requests, 128);
    assert_eq!(report.written_bytes, 128 * 16);
    let stats = store.service_stats().context("missing request stats")?;
    assert_eq!(stats.admission.active, 0);
    assert_eq!(stats.admission.background_active, 0);
    assert!(stats.admission.background_waits > 0);
    assert_eq!(stats.admission.peak, 2);
    assert_eq!(stats.foreground.read_bytes, 513 * 16);
    Ok(())
}

async fn maintenance_burst(
    store: std::sync::Arc<super::scoped::Scoped>,
    pool: std::sync::Arc<crate::engine::execution::Workspaces>,
) -> Result<()> {
    use futures_util::{StreamExt as _, TryStreamExt as _};
    use object_store::{ObjectStoreExt as _, path::Path};
    let _phase = pool.background().await?;
    futures_util::stream::iter(0..128)
        .map(|id| {
            let store = store.clone();
            async move {
                store.put(&Path::from(format!("maintenance/{id}")), vec![2_u8; 16].into()).await
            }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
