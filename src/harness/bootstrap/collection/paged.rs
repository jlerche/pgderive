//! Bounded production GC across cursor rollback, fresh caches and multiple passes.
use super::{Catalog, GcLimits, Query, Recovery, Writer};
use anyhow::{Context as _, Result, ensure};
use object_store::{ObjectStoreExt as _, path::Path};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    graph: &Query,
    writer: &Writer,
    recovery: &Recovery,
) -> Result<()> {
    let protection = writer.protect_upload(sql, "gc-paged").await?;
    let namespace = protection.namespace()?.to_owned();
    for index in 0..513_u64 {
        let path = Path::from(format!("{namespace}/pgderive/pack/{index:064x}"));
        recovery.store.put(&path, vec![0_u8].into()).await?;
    }
    let unknown = Path::from(format!("{namespace}/unknown-format"));
    recovery.store.put(&unknown, vec![1_u8].into()).await?;
    protection.published(sql, writer, graph.plan(), &graph.checkpoint()?).await?;
    rollback(sql, catalog, recovery).await?;
    let turns = finish(sql, catalog, recovery).await?;
    ensure!(turns > 1, "GC did not exercise durable continuation");
    recovery.store.head(&unknown).await?;
    let exists = ledger(sql, catalog, &namespace).await?;
    ensure!(exists, "unknown namespace object lost retirement ledger");
    recovery.store.delete(&unknown).await?;
    finish(sql, catalog, recovery).await?;
    ensure!(
        !ledger(sql, catalog, &namespace).await?,
        "empty quiescent namespace ledger survived full pass"
    );
    graph.snapshot().trace("output")?.materialize().await?;
    eprintln!(
        "MVP incremental GC bounded pages, cursor rollback, fresh-cache resume and unknown retention verified"
    );
    Ok(())
}
async fn rollback(sql: &mut Client, catalog: &Catalog, recovery: &Recovery) -> Result<()> {
    let schema = catalog.schema();
    let before = cursor(sql, catalog).await?;
    sql.batch_execute(&format!("CREATE FUNCTION {schema}.reject_gc_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected GC cursor failure'; END $$; CREATE TRIGGER reject_gc_cursor BEFORE UPDATE ON {schema}.pgderive_gc FOR EACH ROW EXECUTE FUNCTION {schema}.reject_gc_cursor()" )).await?;
    let result = turn(sql, catalog, recovery).await;
    sql.batch_execute(&format!("DROP TRIGGER reject_gc_cursor ON {schema}.pgderive_gc; DROP FUNCTION {schema}.reject_gc_cursor()" )).await?;
    ensure!(result.is_err(), "GC cursor failure was not injected");
    ensure!(
        before == cursor(sql, catalog).await?,
        "failed cursor publication advanced GC progress"
    );
    Ok(())
}
pub(super) async fn finish(
    sql: &mut Client,
    catalog: &Catalog,
    recovery: &Recovery,
) -> Result<usize> {
    for count in 1..=64 {
        turn(sql, catalog, recovery).await?;
        if !sql
            .query_one(&format!("SELECT in_progress FROM {}.pgderive_gc", catalog.schema()), &[])
            .await?
            .try_get::<_, bool>(0)?
        {
            return Ok(count);
        }
    }
    anyhow::bail!("bounded fixture GC failed to finish a full pass")
}
pub(super) async fn turn(sql: &mut Client, catalog: &Catalog, recovery: &Recovery) -> Result<()> {
    let result = catalog
        .collect(
            sql,
            recovery.store.clone(),
            GcLimits { page_objects: 64, ..GcLimits::default() },
            &mut crate::catalog::GcCache::default(),
        )
        .await?;
    let crate::catalog::Collection::Collected { scanned, deleted, pages, .. } = result else {
        anyhow::bail!("incremental GC unexpectedly blocked");
    };
    ensure!(scanned <= 64 && deleted <= scanned && pages == 1, "GC turn exceeded page budget");
    Ok(())
}
async fn cursor(sql: &Client, catalog: &Catalog) -> Result<serde_json::Value> {
    Ok(sql
        .query_one(&format!("SELECT to_jsonb(g) FROM {}.pgderive_gc g", catalog.schema()), &[])
        .await?
        .try_get(0)?)
}
pub(super) async fn ledger(sql: &Client, catalog: &Catalog, namespace: &str) -> Result<bool> {
    sql.query_one(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM {}.pgderive_protections WHERE namespace=$1)",
            catalog.schema()
        ),
        &[&namespace],
    )
    .await?
    .try_get(0)
    .context("GC fixture ledger decode")
}
