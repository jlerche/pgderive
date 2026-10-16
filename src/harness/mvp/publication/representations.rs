//! Native representation identity must survive occurrence retraction and rollback.
use crate::{
    catalog::{Binding, Catalog, Deltas, Progress, Publication, ResultColumn, Sink},
    engine::{
        Batch,
        plan::{Checkpoint, Plan},
    },
};
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(
    sql: &mut Client,
    schema: &str,
    plan: &Plan,
    empty: &Checkpoint,
) -> Result<()> {
    let catalog = Catalog::new(schema, "native_representations")?;
    catalog.checkpoint(sql, plan, empty, 0).await?;
    let columns =
        vec![ResultColumn { name: "number".into(), oid: 1700, modifier: -1, collation: None }];
    let sink = Sink::Rows { table: "native_representations".into(), columns };
    let mut writer = catalog
        .claim(
            sql,
            plan,
            Binding { source: "synthetic representation test; never ACK".into(), sink },
            "0/0".parse()?,
        )
        .await?;
    let mut checkpoint = empty.clone();
    super::bags::advance(&mut checkpoint);
    let deltas = Deltas::bag(&Batch::from_updates([
        (((), vec!["1.0".to_owned()]), 2),
        (((), vec!["1.00".to_owned()]), 1),
    ])?)?;
    writer
        .publish(
            sql,
            plan,
            Publication {
                checkpoint: &checkpoint,
                progress: &Progress::new(1, "0/1", "0/2")?,
                deltas: &deltas,
            },
        )
        .await?;
    super::bags::advance(&mut checkpoint);
    let deltas = Deltas::bag(&Batch::from_updates([(((), vec!["1.00".to_owned()]), -1)])?)?;
    writer
        .publish(
            sql,
            plan,
            Publication {
                checkpoint: &checkpoint,
                progress: &Progress::new(2, "0/3", "0/4")?,
                deltas: &deltas,
            },
        )
        .await?;
    let query = format!(
        "SELECT count(*),min(pg_catalog.scale(number)),max(pg_catalog.scale(number)) FROM {schema}.native_representations"
    );
    let row = sql.query_one(&query, &[]).await?;
    ensure!(
        row.try_get::<_, i64>(0)? == 2
            && row.try_get::<_, i32>(1)? == 1
            && row.try_get::<_, i32>(2)? == 1,
        "numeric retraction erased a different display representation"
    );
    super::bags::advance(&mut checkpoint);
    reject_invalid(sql, &mut writer, plan, &checkpoint, &query).await?;
    eprintln!(
        "MVP numeric representation identity, missing occurrence and expansion limit verified"
    );
    Ok(())
}

async fn reject_invalid(
    sql: &mut Client,
    writer: &mut crate::catalog::Writer,
    plan: &Plan,
    checkpoint: &Checkpoint,
    query: &str,
) -> Result<()> {
    for (value, weight) in [("1.00", -1), ("2.0", 1_000_001)] {
        let deltas = Deltas::bag(&Batch::from_updates([(((), vec![value.to_owned()]), weight)])?)?;
        ensure!(
            writer
                .publish(
                    sql,
                    plan,
                    Publication {
                        checkpoint,
                        progress: &Progress::new(3, "0/5", "0/6")?,
                        deltas: &deltas
                    }
                )
                .await
                .is_err(),
            "invalid occurrence publication committed"
        );
        ensure!(
            writer.end().to_string() == "0/4",
            "failed native occurrence publication advanced source progress"
        );
        ensure!(
            sql.query_one(query, &[]).await?.try_get::<_, i64>(0)? == 2,
            "failed native publication changed result rows"
        );
    }
    Ok(())
}
