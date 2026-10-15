//! `PostgreSQL` presentation of committed internal bags as typed SQL rows.
mod switch;
use super::{
    admin::{query_sql, quote},
    spec::Settings,
};
use crate::compiler::Compiled;
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
pub(super) use switch::replace;
use tokio_postgres::{Client, Column, GenericClient, Statement};

pub(super) async fn validate(sql: &(impl GenericClient + Sync), settings: &Settings) -> Result<()> {
    if settings.result_view.is_some() {
        let statement = sql.prepare(&query_sql(&settings.query)).await?;
        validate_columns(&statement)?;
    }
    Ok(())
}

fn validate_columns(statement: &Statement) -> Result<()> {
    let mut names = BTreeSet::new();
    for column in statement.columns() {
        ensure!(
            column.name().len() <= 63 && names.insert(column.name()),
            "result columns require unique names of at most 63 bytes; add SQL aliases"
        );
        native_type(column.type_().oid())?;
    }
    Ok(())
}

pub(super) async fn install(
    sql: &mut Client,
    settings: &Settings,
    compiled: &Compiled,
) -> Result<()> {
    let Some(view) = &settings.result_view else {
        return Ok(());
    };
    let tx = sql.transaction().await?;
    tx.batch_execute(
        "SET LOCAL synchronous_commit=on; SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,MDY'",
    )
    .await?;
    compiled.validate_bound()?;
    let definition = definition(&tx, settings).await?;
    tx.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {}.pgderive_result_views(query_id text PRIMARY KEY,view_name text NOT NULL UNIQUE,definition text NOT NULL,catalog_definition text NOT NULL)", settings.catalog_schema,
    )).await?;
    let existing = tx
        .query_opt(
            &format!(
                "SELECT view_name,definition,catalog_definition FROM {}.pgderive_result_views WHERE query_id=$1",
                settings.catalog_schema,
            ),
            &[&settings.query_id],
        )
        .await?;
    if let Some(row) = existing {
        ensure!(
            row.try_get::<_, String>(0)? == *view && row.try_get::<_, String>(1)? == definition,
            "registered result view layout changed; rebuild required"
        );
        let actual = catalog_definition(&tx, &settings.catalog_schema, view).await?;
        ensure!(actual == row.try_get::<_, String>(2)?, "registered result view was altered");
    } else {
        tx.batch_execute(&format!(
            "CREATE VIEW {}.{view} AS {definition}",
            settings.catalog_schema
        ))
        .await?;
        let catalog_definition = catalog_definition(&tx, &settings.catalog_schema, view).await?;
        tx.execute(
            &format!(
                "INSERT INTO {}.pgderive_result_views VALUES($1,$2,$3,$4)",
                settings.catalog_schema
            ),
            &[&settings.query_id, &view, &definition, &catalog_definition],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn definition(
    sql: &(impl GenericClient + Sync),
    settings: &Settings,
) -> Result<String> {
    let statement = sql.prepare(&query_sql(&settings.query)).await?;
    validate_columns(&statement)?;
    let mut columns = Vec::new();
    for (index, column) in statement.columns().iter().enumerate() {
        let value = format!("tuple->1->>{index}");
        let native = native_type(column.type_().oid())?;
        let native = type_spec(column, native)?;
        let expression = if column.type_().oid() == 1009 {
            format!(
                "CASE WHEN tuple->1->{index}='null'::pg_catalog.jsonb THEN NULL ELSE ARRAY(SELECT pg_catalog.jsonb_array_elements_text(tuple->1->{index})) END"
            )
        } else {
            format!("({value})::pg_catalog.{native}")
        };
        let collation = collation(sql, &query_sql(&settings.query), column).await?;
        columns.push(format!("{expression}{collation} AS {}", quote(column.name())));
    }
    let table = format!("{}.{}", settings.catalog_schema, settings.sink_table);
    let rows = format!(
        "{table} CROSS JOIN LATERAL pg_catalog.generate_series(1::bigint,weight) AS multiplicity(occurrence)"
    );
    Ok(format!("SELECT {} FROM {rows}", columns.join(",")))
}

fn type_spec(column: &Column, native: &str) -> Result<String> {
    let modifier = column.type_modifier();
    if modifier < 0 {
        return Ok(native.into());
    }
    match column.type_().oid() {
        1043 => {
            ensure!(modifier > 4, "invalid varchar result modifier");
            Ok(format!("{native}({})", modifier - 4))
        }
        1114 | 1184 => {
            ensure!(modifier <= 6, "invalid timestamp result precision");
            Ok(format!("{native}({modifier})"))
        }
        1700 => {
            let encoded = modifier.checked_sub(4).context("invalid numeric result modifier")?;
            let precision = (encoded >> 16) & 65_535;
            let scale = ((encoded & 2047) ^ 1024) - 1024;
            Ok(format!("{native}({precision},{scale})"))
        }
        _ => Ok(native.into()),
    }
}

pub(super) async fn collation(
    sql: &(impl GenericClient + Sync),
    query: &str,
    column: &Column,
) -> Result<String> {
    if !matches!(column.type_().oid(), 25 | 1043 | 1009) {
        return Ok(String::new());
    }
    // LIMIT 0 resolves the PostgreSQL expression's collation without evaluating
    // its source rows. Deparsing removes statement terminators and comments.
    let query = pg_query::parse(query)?.deparse()?;
    let selected = format!(
        "SELECT pg_catalog.pg_collation_for((SELECT q.{} FROM ({query}) q LIMIT 0))",
        quote(column.name())
    );
    let name: String = sql.query_one(&selected, &[]).await?.try_get(0)?;
    let row = sql.query_one("SELECT n.nspname,c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=$1::text::pg_catalog.regcollation",
        &[&name]).await?;
    Ok(format!(
        " COLLATE {}.{}",
        quote(&row.try_get::<_, String>(0)?),
        quote(&row.try_get::<_, String>(1)?)
    ))
}

pub(super) async fn catalog_definition(
    sql: &(impl GenericClient + Sync),
    schema: &str,
    view: &str,
) -> Result<String> {
    sql.query_opt("SELECT pg_catalog.pg_get_viewdef(c.oid,true) FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass($1) AND c.relkind='v'",
        &[&format!("{schema}.{view}")]).await?.context("registered result view is missing or replaced")?.try_get(0).map_err(Into::into)
}

fn native_type(oid: u32) -> Result<&'static str> {
    match oid {
        16 => Ok("bool"),
        20 => Ok("int8"),
        21 => Ok("int2"),
        23 => Ok("int4"),
        25 => Ok("text"),
        1043 => Ok("varchar"),
        2950 => Ok("uuid"),
        1114 => Ok("timestamp"),
        1184 => Ok("timestamptz"),
        1700 => Ok("numeric"),
        1009 => Ok("_text"),
        _ => anyhow::bail!("unsupported public result type OID {oid}"),
    }
}

pub(super) async fn remove(
    sql: &tokio_postgres::Transaction<'_>,
    settings: &Settings,
    view: &str,
) -> Result<()> {
    let row = sql.query_one(&format!(
        "SELECT view_name,catalog_definition FROM {}.pgderive_result_views WHERE query_id=$1", settings.catalog_schema,
    ), &[&settings.query_id]).await?;
    ensure!(row.try_get::<_, String>(0)? == view, "removal result view changed");
    sql.batch_execute(&format!(
        "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
        settings.catalog_schema,
        quote(view)
    ))
    .await?;
    let actual = catalog_definition(sql, &settings.catalog_schema, view).await?;
    ensure!(actual == row.try_get::<_, String>(1)?, "removal result view was altered");
    sql.batch_execute(&format!("DROP VIEW {}.{} RESTRICT", settings.catalog_schema, quote(view)))
        .await?;
    sql.execute(
        &format!("DELETE FROM {}.pgderive_result_views WHERE query_id=$1", settings.catalog_schema),
        &[&settings.query_id],
    )
    .await?;
    Ok(())
}
