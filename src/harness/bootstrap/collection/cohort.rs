//! A late writes-finished proof cannot join an already started retirement pass.
use super::{Catalog, Query, Recovery, Writer};
use anyhow::{Result, ensure};
use object_store::{ObjectStoreExt as _, path::Path};

pub(super) async fn check(
    sql: &mut crate::catalog::Connection,
    catalog: &Catalog,
    graph: &Query,
    writer: &Writer,
    recovery: &Recovery,
) -> Result<()> {
    let namespace: String = sql.query_one(&format!("SELECT namespace FROM {}.pgderive_protections WHERE token='gc-abandoned' AND NOT quiescent",catalog.schema()), &[]).await?.try_get(0)?;
    let sentinel = Path::from(format!("{namespace}/zzz-sentinel"));
    recovery.store.put(&sentinel, vec![1_u8].into()).await?;
    // Keep a suffix after the sentinel in the real S3 service's lexical listing.
    let suffix =
        (0..128).map(|n| Path::from(format!("pgderive/zz-cohort/{n:03}"))).collect::<Vec<_>>();
    for path in &suffix {
        recovery.store.put(path, vec![1_u8].into()).await?;
    }
    seen(sql, catalog, recovery, &namespace).await?;
    let late = Path::from(format!("{namespace}/pgderive/pack/{}", "0".repeat(64)));
    recovery.store.put(&late, vec![1_u8].into()).await?;
    recovery.store.delete(&sentinel).await?;
    // Fixture owns the fenced uploader and all of its PUTs have now completed.
    // Production has no automatic age-based writes-finished inference.
    sql.execute(
        &format!(
            "UPDATE {}.pgderive_protections SET quiescent=true WHERE namespace=$1 AND NOT active",
            catalog.schema()
        ),
        &[&namespace],
    )
    .await?;
    let fresh = writer.protect_upload(sql, "gc-midpass-empty").await?;
    let fresh_namespace = fresh.namespace()?.to_owned();
    fresh.published(sql, writer, graph.plan(), &graph.checkpoint()?).await?;
    verify(sql, catalog, recovery, (&namespace, &fresh_namespace), &late).await?;
    for path in suffix {
        recovery.store.delete(&path).await?;
    }
    eprintln!("MVP GC retirement excludes late quiescence and mid-pass upload closure");
    Ok(())
}
async fn seen(
    sql: &mut crate::catalog::Connection,
    catalog: &Catalog,
    recovery: &Recovery,
    namespace: &str,
) -> Result<()> {
    for _ in 0..64 {
        super::paged::turn(sql, catalog, recovery).await?;
        let row = sql.query_one(&format!("SELECT p.gc_seen=g.generation,g.in_progress FROM {}.pgderive_protections p CROSS JOIN {}.pgderive_gc g WHERE namespace=$1",catalog.schema(),catalog.schema()), &[&namespace]).await?;
        if row.try_get::<_, bool>(0)? {
            ensure!(row.try_get::<_, bool>(1)?, "cohort fixture lacked a post-sentinel suffix");
            return Ok(());
        }
    }
    anyhow::bail!("cohort fixture did not pass its sentinel")
}

async fn verify(
    sql: &mut crate::catalog::Connection,
    catalog: &Catalog,
    recovery: &Recovery,
    namespaces: (&str, &str),
    late: &Path,
) -> Result<()> {
    super::paged::finish(sql, catalog, recovery).await?;
    recovery.store.head(late).await?;
    ensure!(
        super::paged::ledger(sql, catalog, namespaces.0).await?
            && super::paged::ledger(sql, catalog, namespaces.1).await?,
        "mid-pass closure joined an older retirement cohort"
    );
    super::paged::finish(sql, catalog, recovery).await?;
    ensure!(
        recovery.store.head(late).await.is_err()
            && !super::paged::ledger(sql, catalog, namespaces.0).await?
            && !super::paged::ledger(sql, catalog, namespaces.1).await?,
        "next pass did not reclaim late closed namespace"
    );
    Ok(())
}
