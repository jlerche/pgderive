//! Regression checks for the batched source catalog snapshot and DDL locks.
use super::Fixture;
use crate::source::{Contract, Identity};
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(sql: &mut Client, fixture: &Fixture, identity: &Identity) -> Result<()> {
    let schema = &fixture.schema;
    let publication = format!("{}_layout", fixture.publication);
    let first = format!("{schema}.\"layout\"\"one\"");
    let second = format!("{schema}.layout_two");
    sql.batch_execute(&format!(
        "CREATE TABLE {first}(id bigint PRIMARY KEY, note varchar(12));
         CREATE TABLE {second}(id bigint PRIMARY KEY, flag boolean);
         ALTER TABLE {first} REPLICA IDENTITY FULL;
         ALTER TABLE {second} REPLICA IDENTITY FULL;
         CREATE PUBLICATION {publication} FOR TABLE {first},{second}"
    ))
    .await?;
    let contract = Contract::inspect(sql, identity.clone(), &publication, &fixture.slot).await?;
    ensure!(contract.relations.len() == 2, "batched layout lost a relation");
    let tx = sql.transaction().await?;
    contract.lock_and_verify_layout(&tx).await?;
    tx.rollback().await?;
    for change in [
        format!("ALTER PUBLICATION {publication} SET (publish='insert,delete,truncate')"),
        format!("ALTER PUBLICATION {publication} DROP TABLE {second}"),
        format!("ALTER PUBLICATION {publication} SET TABLE {first}(id),{second}"),
        format!("ALTER PUBLICATION {publication} SET TABLE {first} WHERE (id>0),{second}"),
        format!("ALTER TABLE {first} REPLICA IDENTITY DEFAULT"),
        format!("ALTER TABLE {first} DROP CONSTRAINT \"layout\"\"one_pkey\""),
        format!("ALTER TABLE {first} ALTER COLUMN note SET NOT NULL"),
        format!("ALTER TABLE {first} ALTER COLUMN note TYPE varchar(15)"),
        format!("ALTER TABLE {first} RENAME COLUMN note TO renamed"),
        format!(
            "ALTER TABLE {first} ADD COLUMN generated bigint GENERATED ALWAYS AS (id+1) STORED"
        ),
        format!("ALTER TABLE {first} ADD COLUMN unsupported bytea"),
    ] {
        let tx = sql.transaction().await?;
        tx.batch_execute(&change).await?;
        ensure!(
            contract.lock_and_verify_layout(&tx).await.is_err(),
            "layout drift accepted: {change}"
        );
        tx.rollback().await?;
    }
    identity_check(sql, &contract).await?;
    sql.batch_execute(&format!("DROP PUBLICATION {publication}; DROP TABLE {first},{second}"))
        .await?;
    eprintln!("MVP batched source layouts reject publication, identity and column drift");
    Ok(())
}

async fn identity_check(sql: &mut Client, contract: &Contract) -> Result<()> {
    let mut wrong = contract.clone();
    wrong.identity.timeline = wrong
        .identity
        .timeline
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("timeline overflow"))?;
    let tx = sql.transaction().await?;
    ensure!(wrong.lock_and_verify_layout(&tx).await.is_err(), "changed timeline accepted");
    tx.rollback().await?;
    let tx = sql.transaction().await?;
    contract.lock_and_verify_layout(&tx).await?;
    tx.rollback().await?;
    Ok(())
}
