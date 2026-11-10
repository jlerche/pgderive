use super::super::upload::{Chunk, Owner};
use super::*;

pub(super) async fn terminal_delete(f: &Fixture) -> Result<()> {
    let path = "pgderive/upload-v1/deleted/pgderive/pack/a";
    f.put(0, path, vec![1; 100]).await?;
    f.store(0).delete(&Path::from(path)).await?;
    ensure!(f.put(0, path, vec![1; 100]).await.is_err(), "deleted logical object resurrected");
    ensure!(
        f.store(0).head(&Path::from(path)).await.is_err(),
        "deleted logical object remained visible"
    );
    let unknown = "pgderive/upload-v1/never-created/pgderive/pack/x";
    f.store(0).delete(&Path::from(unknown)).await?;
    ensure!(
        f.put(0, unknown, vec![2; 100]).await.is_err(),
        "late first PUT bypassed namespace sealing"
    );
    Ok(())
}
pub(super) async fn late_put(f: &Fixture) -> Result<()> {
    let mut owner = Owner::connect(f.manifest.clone(), "alice").await?;
    let bytes = b"provider completion after uploader fencing";
    let pack = owner.reserve("alice", bytes).await?;
    // Closing the SQL owner fences mapping installation; no timer proves I/O finished.
    owner.close().await?;
    f.group.collect().await?;
    f.physical
        .put_opts(
            &Path::from(pack.path.clone()),
            Bytes::from_static(bytes).into(),
            PutOptions::from(PutMode::Create),
        )
        .await?;
    let chunk = Chunk {
        catalog: f.catalogs[0].clone(),
        name: "pgderive/upload-v1/late/pgderive/pack/x".into(),
        bytes: Bytes::from_static(bytes),
        checksum: super::super::checksum(bytes),
    };
    ensure!(
        owner.install(&pack, &[chunk]).await.is_err(),
        "fenced uploader installed late mapping"
    );
    let mut sweeper = Owner::connect(f.manifest.clone(), "alice").await?;
    let physical: Arc<dyn crate::storage::PageStore> = f.physical.clone();
    sweeper.collect("alice", &physical).await?;
    sweeper.close().await?;
    ensure!(
        matches!(
            f.physical.head(&Path::from(pack.path)).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "late physical PUT escaped inventory cleanup"
    );
    Ok(())
}
pub(super) async fn ambiguous_mapping_commit(f: &Fixture) -> Result<()> {
    let mut owner = Owner::connect(f.manifest.clone(), "alice").await?;
    let bytes = Bytes::from_static(b"mapping confirmed independently of source PostgreSQL");
    let pack = owner.reserve("alice", &bytes).await?;
    f.physical
        .put_opts(
            &Path::from(pack.path.clone()),
            bytes.clone().into(),
            PutOptions::from(PutMode::Create),
        )
        .await?;
    let name = "pgderive/upload-v1/uncertain/pgderive/batch/root";
    let chunk = Chunk {
        catalog: f.catalogs[1].clone(),
        name: name.into(),
        checksum: super::super::checksum(&bytes),
        bytes: bytes.clone(),
    };
    owner.install(&pack, &[chunk]).await?;
    // Model losing the successful reply: no source checkpoint is required to
    // keep this mapping live, and no source connection is available to GC.
    drop(owner);
    f.group.collect().await?;
    ensure!(
        f.store(1).get(&Path::from(name)).await?.bytes().await? == bytes,
        "uncertain source publication lost protection"
    );
    Ok(())
}
pub(super) async fn tenant_isolation(f: &Fixture) -> Result<()> {
    let foreign = Catalog { tenant: "bob".into(), catalog: f.catalogs[0].catalog.clone() };
    let name = "pgderive/upload-v1/healthy/pgderive/pack/b";
    ensure!(
        f.group
            .put(foreign.clone(), name.into(), Bytes::from_static(b"wrong tenant").into())
            .await
            .is_err(),
        "cross-tenant write accepted"
    );
    let store = Virtual { group: f.group.clone(), catalog: foreign };
    ensure!(store.head(&Path::from(name)).await.is_err(), "cross-tenant read accepted");
    let rejected = store
        .put_opts(
            &Path::from(name),
            Bytes::from_static(b"foreign").into(),
            PutOptions::from(PutMode::Create),
        )
        .await
        .err()
        .context("foreign PUT accepted")?;
    ensure!(
        rejected.to_string().contains("cross-tenant store binding rejected"),
        "foreign PUT reached logical existence lookup before binding validation"
    );
    drop(store);
    let different = f.uuid().await?;
    ensure!(
        f.manifest
            .catalog(
                &Catalog { tenant: "alice".into(), catalog: different },
                &serde_json::json!({"unavailable_source_database":0}),
                "same-logical-alias"
            )
            .await
            .is_err(),
        "catalog logical ownership alias was adopted"
    );
    Ok(())
}
