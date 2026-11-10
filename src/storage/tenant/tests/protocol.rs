use super::*;

pub(super) async fn packed_relocation(f: &Fixture) -> Result<()> {
    let a = "pgderive/upload-v1/packed-a/pgderive/pack/a";
    let b = "pgderive/upload-v1/packed-b/pgderive/batch/b";
    let puts = f.physical.puts.load(Ordering::SeqCst);
    let (left, right) = tokio::join!(f.put(0, a, vec![7; 65536]), f.put(1, b, vec![9; 8192]));
    left?;
    right?;
    ensure!(
        f.physical.puts.load(Ordering::SeqCst) == puts + 1,
        "independent catalog uploads were not packed"
    );
    let old =
        f.manifest.lookup(&f.catalogs[1], b).await?.context("packed logical object missing")?;
    ensure!(
        f.store(1).get(&Path::from(b)).await?.bytes().await?.as_ref() == vec![9; 8192],
        "cross-catalog chunk mismatch"
    );
    // Hold the exact physical-reader lock used by the production read path.
    let mut pin = f.manifest.pool.acquire().await?;
    let tx = pin.connection()?.transaction().await?;
    tx.query_one(
        &format!("SELECT id FROM {}.packs WHERE path=$1 FOR SHARE", f.manifest.config.schema),
        &[&old.path],
    )
    .await?;
    f.store(0).delete(&Path::from(a)).await?;
    let group = f.group.clone();
    let collecting = tokio::spawn(async move { group.collect().await });
    wait_for_relocation(f, puts).await?;
    ensure!(!collecting.is_finished(), "collector ignored a pinned physical location");
    tokio::time::timeout(
        Duration::from_secs(1),
        f.put(0, "pgderive/upload-v1/unblocked/pgderive/pack/healthy", vec![13; 128]),
    )
    .await
    .context("pinned collection blocked healthy uploads")??;
    f.physical.head(&Path::from(old.path.clone())).await?;
    tx.commit().await?;
    drop(pin);
    collecting.await??;
    verify_relocation(f, b, &old.path).await
}
async fn verify_relocation(f: &Fixture, name: &str, old: &str) -> Result<()> {
    let new =
        f.manifest.lookup(&f.catalogs[1], name).await?.context("offline catalog chunk lost")?;
    ensure!(new.path != old, "logical chunk was not relocated");
    ensure!(
        f.store(1).get(&Path::from(name)).await?.bytes().await?.as_ref() == vec![9; 8192],
        "relocation changed immutable bytes"
    );
    ensure!(
        matches!(
            f.physical.head(&Path::from(old)).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "obsolete pack not reclaimed"
    );
    ensure!(
        f.store(0).head(&Path::from(name)).await.is_err(),
        "catalog could access another catalog's logical object"
    );
    Ok(())
}

async fn wait_for_relocation(f: &Fixture, puts: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while f.physical.puts.load(Ordering::SeqCst) < puts + 2 {
        ensure!(tokio::time::Instant::now() < deadline, "relocation PUT did not start");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut sql = f.manifest.pool.acquire().await?;
        let waiting:bool=sql.connection()?.query_one("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE $1)",&[&format!("%{}.packs%FOR UPDATE%",f.manifest.config.schema)]).await?.try_get(0)?;
        drop(sql);
        if waiting {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "relocation did not wait on physical reader pin"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(())
}

pub(super) async fn range_and_integrity(f: &Fixture) -> Result<()> {
    use crate::storage::PageStore as _;
    let name = "pgderive/upload-v1/integrity/pgderive/pack/x";
    f.put(0, name, (0_u8..100).collect()).await?;
    let store = f.store(0);
    let path = Path::from(name);
    ensure!(
        store.get_range(&path, 20..30).await?.as_ref() == (20_u8..30).collect::<Vec<_>>(),
        "range addressing changed bytes"
    );
    let head = store.head(&path).await?;
    let result = store
        .get_opts(
            &path,
            object_store::GetOptions {
                if_match: Some("wrong".into()),
                ..object_store::GetOptions::default()
            },
        )
        .await;
    ensure!(
        matches!(result, Err(object_store::Error::Precondition { .. })),
        "conditional GET ignored immutable identity"
    );
    let page = store.page("pgderive/upload-v1/integrity/", None, 1).await?;
    ensure!(page.result.objects == vec![head], "catalog listing metadata mismatch");
    let location =
        f.manifest.lookup(&f.catalogs[0], name).await?.context("integrity mapping absent")?;
    let physical = Path::from(location.path);
    let saved = f.physical.get(&physical).await?.bytes().await?;
    let mut corrupted = saved.to_vec();
    let index = usize::try_from(location.offset)?;
    *corrupted.get_mut(index).context("corruption offset absent")? ^= 1;
    f.physical.put(&physical, Bytes::from(corrupted).into()).await?;
    ensure!(store.get(&path).await.is_err(), "corrupt logical bytes passed checksum validation");
    f.physical.put(&physical, saved.into()).await?;
    drop(store);
    Ok(())
}

pub(super) async fn cancelled_shutdown(f: &Fixture) -> Result<()> {
    let group = f.group.clone();
    let catalog = f.catalogs[0].clone();
    let name = "pgderive/upload-v1/shutdown/pgderive/pack/x";
    let putting = tokio::spawn(async move {
        group
            .put(catalog, name.into(), Bytes::from_static(b"drain after cancellation").into())
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while f.group.stats()["buffer_used"].as_u64().unwrap_or(0) == 0 {
        ensure!(tokio::time::Instant::now() < deadline, "shutdown request not admitted");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    ensure!(
        tokio::time::timeout(Duration::from_millis(1), f.group.close()).await.is_err(),
        "shutdown did not encounter pending upload"
    );
    f.group.close().await?;
    putting.await??;
    ensure!(
        f.manifest.lookup(&f.catalogs[0], name).await?.is_some(),
        "retried shutdown detached accepted upload"
    );
    ensure!(!f.group.open(), "closed uploader still accepts requests");
    Ok(())
}
pub(super) async fn cancelled_contributor(f: &Fixture) -> Result<()> {
    let name = "pgderive/upload-v1/cancelled/pgderive/pack/a";
    let group = f.group.clone();
    let catalog = f.catalogs[0].clone();
    let cancelled = tokio::spawn(async move {
        group.put(catalog, name.into(), Bytes::from_static(b"cancelled-but-owned").into()).await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while f.group.stats()["buffer_used"].as_u64().unwrap_or(0) == 0 {
        ensure!(tokio::time::Instant::now() < deadline, "contributor was not admitted");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let healthy = f.put(1, "pgderive/upload-v1/healthy/pgderive/pack/b", vec![42; 128]);
    cancelled.abort();
    healthy.await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while f.manifest.lookup(&f.catalogs[0], name).await?.is_none() {
        ensure!(tokio::time::Instant::now() < deadline, "accepted cancelled request was abandoned");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    ensure!(f.group.open(), "cancelled contributor stopped tenant uploader");
    Ok(())
}
