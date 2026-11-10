use super::super::upload::Owner;
use super::*;

const LIVE: &str = "pgderive/upload-v1/sweep-b/pgderive/pack/b";

pub(super) async fn pinned_relocation_does_not_starve_sweep(f: &Fixture) -> Result<()> {
    let fragmented = fragment(f).await?;
    let pinned = dead(f, "pinned-empty").await?;
    let free = dead(f, "unrelated-empty").await?;
    let mut pin = f.manifest.pool.acquire().await?;
    let tx = pin.connection()?.transaction().await?;
    tx.query(
        &format!(
            "SELECT id FROM {}.packs WHERE path=ANY($1::text[]) FOR SHARE",
            f.manifest.config.schema
        ),
        &[&vec![fragmented, pinned.clone()]],
    )
    .await?;
    let mut owner = Owner::connect(f.manifest.clone(), "alice").await?;
    owner.sql.client.batch_execute("SET lock_timeout='20ms'").await?;
    let physical: Arc<dyn crate::storage::PageStore> = f.physical.clone();
    ensure!(
        owner.collect("alice", &physical).await.is_err(),
        "pinned relocation did not encounter the injected lock timeout"
    );
    owner.close().await?;
    drop(owner);
    verify_independent_sweep(f, &pinned, free).await?;
    tx.commit().await?;
    drop(pin);
    f.group.collect().await?;
    absent(f, &pinned).await?;
    Ok(())
}

async fn absent(f: &Fixture, path: &str) -> Result<()> {
    ensure!(
        matches!(
            f.physical.head(&Path::from(path)).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "released empty pack was not reclaimed"
    );
    Ok(())
}

async fn verify_independent_sweep(f: &Fixture, pinned: &str, free: String) -> Result<()> {
    ensure!(
        matches!(
            f.physical.head(&Path::from(free)).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "failed relocation starved an unrelated dead pack"
    );
    f.physical.head(&Path::from(pinned)).await?;
    ensure!(
        f.store(1).get(&Path::from(LIVE)).await?.bytes().await?.as_ref() == vec![9; 8192],
        "failed relocation lost live offline bytes"
    );
    Ok(())
}

async fn fragment(f: &Fixture) -> Result<String> {
    let a = "pgderive/upload-v1/sweep-a/pgderive/pack/a";
    let (left, right) = tokio::join!(f.put(0, a, vec![7; 65536]), f.put(1, LIVE, vec![9; 8192]));
    left?;
    right?;
    let fragmented =
        f.manifest.lookup(&f.catalogs[1], LIVE).await?.context("fragmented mapping absent")?;
    f.store(0).delete(&Path::from(a)).await?;
    Ok(fragmented.path)
}

async fn dead(f: &Fixture, label: &str) -> Result<String> {
    let name = format!("pgderive/upload-v1/{label}/pgderive/pack/a");
    f.put(0, &name, vec![3; 128]).await?;
    let chunk = f.manifest.lookup(&f.catalogs[0], &name).await?.context("dead fixture absent")?;
    f.store(0).delete(&Path::from(name)).await?;
    Ok(chunk.path)
}
