use super::{Fixture, connect};
use crate::{
    Config,
    source::{Contract, Export},
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub(super) async fn check(sql: &Client, fixture: &Fixture, config: &Config) -> Result<()> {
    let mut config = config.clone();
    config.replication.slot = format!("{}_codec", fixture.slot);
    config.replication.publication = format!("{}_codec", fixture.publication);
    let role = format!("{}_reader", fixture.schema);
    let schema = &fixture.schema;
    sql.batch_execute(&format!("CREATE ROLE {role} NOLOGIN;
        CREATE TABLE {schema}.source_codec(id bigint PRIMARY KEY,\"a\"\"bool\" boolean,u uuid,s smallint,i integer,v varchar(30));
        ALTER TABLE {schema}.source_codec REPLICA IDENTITY FULL;
        INSERT INTO {schema}.source_codec VALUES(1,true,'00000000-0000-0000-0000-000000000001',-2,3,'text'),(2,false,NULL,NULL,NULL,NULL),(3,NULL,NULL,NULL,NULL,NULL);
        ALTER TABLE {schema}.source_codec ENABLE ROW LEVEL SECURITY;
        CREATE POLICY restricted_copy ON {schema}.source_codec USING(id=1);
        GRANT EXECUTE ON FUNCTION pg_control_system(),pg_control_checkpoint() TO {role}; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT ON {schema}.source_codec TO {role};
        CREATE PUBLICATION {} FOR TABLE {schema}.source_codec", config.replication.publication)).await?;
    let export = Export::create(&config, &config.replication.slot).await?;
    let (mut reader, task) = connect(&config).await?;
    copy_checks(&mut reader, &export, &config, &role).await?;
    export.close().await?;
    drop(reader);
    task.await.context("codec snapshot SQL task failed")??;
    sql.query_one("SELECT pg_drop_replication_slot($1)", &[&config.replication.slot]).await?;
    sql.batch_execute(&format!("DROP PUBLICATION {}; DROP TABLE {schema}.source_codec; REVOKE USAGE ON SCHEMA {schema} FROM {role}; REVOKE EXECUTE ON FUNCTION pg_control_system(),pg_control_checkpoint() FROM {role}; DROP ROLE {role}",config.replication.publication)).await?;
    eprintln!(
        "MVP snapshot quoted-name/boolean/null/native codecs and bounded-copy/RLS rejection verified"
    );
    Ok(())
}

async fn copy_checks(
    reader: &mut Client,
    export: &Export,
    config: &Config,
    role: &str,
) -> Result<()> {
    let snapshot = export.import(reader).await?;
    let contract = Contract::inspect(
        &snapshot,
        export.identity().clone(),
        &config.replication.publication,
        &config.replication.slot,
    )
    .await?;
    snapshot.batch_execute(&format!("SET LOCAL ROLE {role}")).await?;
    let error = crate::source::copy(&snapshot, &contract, config.execution)
        .await
        .err()
        .context("RLS silently filtered bootstrap rows")?;
    ensure!(
        error
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(tokio_postgres::Error::as_db_error)
            .is_some_and(|error| error.message().contains("row-level security")),
        "RLS rejection failed for an unrelated reason: {error:#}"
    );
    snapshot.rollback().await?;
    let snapshot = export.import(reader).await?;
    let limits = crate::engine::execution::Limits { record_bytes: 8, ..config.execution };
    ensure!(
        crate::source::copy(&snapshot, &contract, limits).await.is_err(),
        "snapshot accepted oversize encoded row"
    );
    snapshot.rollback().await?;
    let snapshot = export.import(reader).await?;
    let batch = crate::source::copy(&snapshot, &contract, config.execution).await?;
    ensure!(batch.updates.len() == 3, "privileged RLS bootstrap lost rows");
    for update in &batch.updates {
        let id =
            update.tuple.row.get("id").and_then(Option::as_deref).context("missing codec id")?;
        let expected = match id {
            "1" => Some("t"),
            "2" => Some("f"),
            "3" => None,
            _ => anyhow::bail!("unknown codec fixture row"),
        };
        ensure!(
            update.tuple.row.get("a\"bool").context("quoted boolean column missing")?.as_deref()
                == expected,
            "snapshot boolean codec differs from pgoutput"
        );
    }
    snapshot.commit().await?;
    Ok(())
}
