//! Isolated destination arithmetic checks; these synthetic positions never authorize ACK.
use crate::{
    catalog::{Catalog, Progress, Publication, Writer},
    engine::{
        Batch,
        plan::{Checkpoint, Plan},
    },
};
use anyhow::{Context, Result, ensure};

pub(super) async fn check(
    sql: &mut crate::catalog::Connection,
    schema: &str,
    initial: &super::initial::Initial<'_>,
) -> Result<()> {
    let catalog = Catalog::new(schema, "bag_coefficients")?;
    let plan = initial.plan;
    let (mut writer, mut checkpoint) = initial
        .synthetic(sql, &catalog, crate::harness::tuple_sink::sink("coefficient_bag"))
        .await?;
    advance(&mut checkpoint);
    let batch = Batch::from_updates([
        ((1, "a".to_owned()), 2),
        ((1, "b".to_owned()), 3),
        ((2, "c".to_owned()), 4),
    ])?;
    let deltas = crate::harness::tuple_sink::deltas(&batch)?;
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
    advance(&mut checkpoint);
    underflow(sql, &mut writer, &checkpoint, Contract { catalog: &catalog, plan, schema }).await?;
    let delta = crate::harness::tuple_sink::deltas(&Batch::from_updates([
        ((1, "a".to_owned()), -1),
        ((1, "b".to_owned()), -1),
        ((2, "c".to_owned()), -4),
    ])?)?;
    writer
        .publish(
            sql,
            plan,
            Publication {
                checkpoint: &checkpoint,
                progress: &Progress::new(2, "0/3", "0/4")?,
                deltas: &delta,
            },
        )
        .await?;
    let rows = sql
        .query(&format!("SELECT tuple,count(*)::bigint weight FROM {schema}.coefficient_bag GROUP BY tuple ORDER BY tuple"), &[])
        .await?;
    ensure!(
        rows.len() == 2 && rows[0].try_get::<_, i64>(1)? == 1 && rows[1].try_get::<_, i64>(1)? == 2,
        "bag sink lost same-key full identity/multiplicity"
    );
    advance(&mut checkpoint);
    let delta = crate::harness::tuple_sink::deltas(&Batch::from_updates([
        ((1, "a".to_owned()), -1),
        ((1, "b".to_owned()), -2),
    ])?)?;
    writer
        .publish(
            sql,
            plan,
            Publication {
                checkpoint: &checkpoint,
                progress: &Progress::new(3, "0/5", "0/6")?,
                deltas: &delta,
            },
        )
        .await?;
    ensure!(
        sql.query_one(&format!("SELECT COUNT(*) FROM {schema}.coefficient_bag"), &[])
            .await?
            .try_get::<_, i64>(0)?
            == 0,
        "bag zero coefficients were retained"
    );
    eprintln!(
        "MVP full-tuple result occurrences, cancellation and negative multiplicity rollback passed"
    );
    Ok(())
}
pub(super) fn advance(checkpoint: &mut Checkpoint) {
    checkpoint.time += 1;
    for member in &mut checkpoint.arrangements {
        member.trace.time = checkpoint.time;
        member.trace.generation = checkpoint.time;
    }
}

struct Contract<'a> {
    catalog: &'a Catalog,
    plan: &'a Plan,
    schema: &'a str,
}
async fn underflow(
    sql: &mut crate::catalog::Connection,
    writer: &mut Writer,
    checkpoint: &Checkpoint,
    contract: Contract<'_>,
) -> Result<()> {
    let underflow = crate::harness::tuple_sink::deltas(&Batch::from_updates([
        ((1, "a".to_owned()), 1),
        ((2, "c".to_owned()), -5),
    ])?)?;
    ensure!(
        writer
            .publish(
                sql,
                contract.plan,
                Publication {
                    checkpoint,
                    progress: &Progress::new(2, "0/3", "0/4")?,
                    deltas: &underflow
                }
            )
            .await
            .is_err(),
        "result sink accepted negative final multiplicity"
    );
    let unchanged = contract
        .catalog
        .load_durable(sql, contract.plan)
        .await?
        .context("missing coefficient catalog")?;
    ensure!(
        unchanged.stored.epoch == 2
            && unchanged.stored.checkpoint.time == 2
            && writer.end().to_string() == "0/2",
        "underflow changed durable bag boundary"
    );
    let row = sql
        .query_one(
            &format!(
                "SELECT count(*) FROM {}.coefficient_bag WHERE tuple='[2,\"c\"]'::jsonb",
                contract.schema
            ),
            &[],
        )
        .await?;
    ensure!(row.try_get::<_, i64>(0)? == 4, "underflow changed sink coefficient");
    Ok(())
}
