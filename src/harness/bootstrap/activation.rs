use crate::{
    Config,
    catalog::{Catalog, Snapshot},
    engine::plan::Plan,
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    catalog: &Catalog,
    plan: &Plan,
    snapshot: Snapshot<'_>,
    config: &Config,
) -> Result<()> {
    for mode in ["BEFORE", "AFTER"] {
        let Ok(port) = std::env::var(format!("PGDERIVE_SQL_COMMIT_{mode}_PORT")) else {
            catalog.activate_snapshot(sql, plan, snapshot.clone()).await?;
            break;
        };
        let mut config = config.clone();
        config.postgres.port = port.parse()?;
        let (mut fault, task) = crate::harness::connect(&config).await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            catalog.activate_snapshot(&mut fault, plan, snapshot.clone()),
        )
        .await
        .context("snapshot COMMIT fault timed out")?;
        ensure!(result.is_err(), "snapshot COMMIT fault did not disconnect");
        drop(fault);
        let _disconnected = task.await.context("snapshot fault SQL driver panicked")?;
        let durable = catalog.load(sql, plan).await?;
        if mode == "BEFORE" {
            ensure!(durable.is_none(), "snapshot before COMMIT failure activated partial state");
        } else {
            ensure!(
                durable.context("snapshot after COMMIT missing state")?.checkpoint
                    == *snapshot.checkpoint,
                "snapshot after COMMIT changed membership"
            );
        }
        eprintln!("MVP snapshot uncertain {mode} COMMIT resolved from authoritative registration");
    }
    let durable = catalog
        .load_durable(sql, plan)
        .await?
        .context("snapshot activation missing durable progress")?;
    ensure!(
        durable.binding.source == snapshot.source.encode()?
            && durable.end == snapshot.boundary
            && durable.xid.is_none(),
        "snapshot source boundary mismatch"
    );
    ensure!(
        catalog.activate_snapshot(sql, plan, snapshot).await.is_err(),
        "snapshot activation replayed destination DML"
    );
    Ok(())
}
