//! Native result metadata resolved at initialization; no `PostgreSQL` presentation view.
use super::{
    admin::{query_sql, quote},
    spec::{QueryDefinition, Settings},
};
use crate::catalog::ResultColumn;
use anyhow::{Result, ensure};
use tokio_postgres::{Column, GenericClient};

pub(super) async fn validate(sql: &(impl GenericClient + Sync), settings: &Settings) -> Result<()> {
    let columns = columns(sql, &settings.query).await?;
    crate::catalog::Sink::Rows { table: settings.sink_table.clone(), columns }.validate()
}

pub(super) async fn columns(
    sql: &(impl GenericClient + Sync),
    query: &QueryDefinition,
) -> Result<Vec<ResultColumn>> {
    let query = query_sql(query);
    let statement = sql.prepare(&query).await?;
    let mut columns = Vec::new();
    for column in statement.columns() {
        columns.push(ResultColumn {
            name: column.name().into(),
            oid: column.type_().oid(),
            modifier: column.type_modifier(),
            collation: collation(sql, &query, column).await?,
        });
    }
    // Validate before creating registrations, slots or destinations.
    ensure!(!columns.is_empty(), "result has no columns");
    Ok(columns)
}

pub(super) async fn collation(
    sql: &(impl GenericClient + Sync),
    query: &str,
    column: &Column,
) -> Result<Option<(String, String)>> {
    if !matches!(column.type_().oid(), 25 | 1043 | 1009) {
        return Ok(None);
    }
    // LIMIT 0 resolves the `PostgreSQL` expression's collation without evaluating
    // its source rows. Deparsing removes statement terminators and comments.
    let query = pg_query::parse(query)?.deparse()?;
    let selected = format!(
        "SELECT pg_catalog.pg_collation_for((SELECT q.{} FROM ({query}) q LIMIT 0))",
        quote(column.name())
    );
    let name: String = sql.query_one(&selected, &[]).await?.try_get(0)?;
    let row = sql.query_one("SELECT n.nspname,c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=$1::text::pg_catalog.regcollation",
        &[&name]).await?;
    Ok(Some((row.try_get(0)?, row.try_get(1)?)))
}
