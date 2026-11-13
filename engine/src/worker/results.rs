//! Native result metadata resolved at initialization; no `PostgreSQL` presentation view.
use super::{admin::quote, spec::QueryDefinition};
use crate::catalog::ResultColumn;
use anyhow::{Result, ensure};
use tokio_postgres::{Column, GenericClient};

pub(super) async fn columns(
    sql: &(impl GenericClient + Sync),
    query: &QueryDefinition,
) -> Result<(Vec<ResultColumn>, Vec<serde_json::Value>)> {
    let statement = sql.prepare(&query.sql).await?;
    let query = pg_query::parse(&query.sql)?.deparse()?;
    let mut description = Vec::new();
    let mut columns = Vec::new();
    for column in statement.columns() {
        let collation = collation(sql, &query, column).await?;
        description.push(serde_json::json!({"name":column.name(),"type_oid":column.type_().oid(),"type":column.type_().name(),"type_modifier":column.type_modifier(),"collation":collation}));
        columns.push(ResultColumn {
            name: column.name().into(),
            oid: column.type_().oid(),
            modifier: column.type_modifier(),
            collation,
        });
    }
    // Validate before creating registrations, slots or destinations.
    ensure!(!columns.is_empty(), "result has no columns");
    Ok((columns, description))
}

async fn collation(
    sql: &(impl GenericClient + Sync),
    query: &str,
    column: &Column,
) -> Result<Option<(String, String)>> {
    if !matches!(column.type_().oid(), 25 | 1043 | 1009) {
        return Ok(None);
    }
    // LIMIT 0 resolves the `PostgreSQL` expression's collation without evaluating
    // its source rows. Deparsing removes statement terminators and comments.
    let selected = format!(
        "SELECT pg_catalog.pg_collation_for((SELECT q.{} FROM ({query}) q LIMIT 0))",
        quote(column.name())
    );
    let name: String = sql.query_one(&selected, &[]).await?.try_get(0)?;
    let row = sql.query_one("SELECT n.nspname,c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=$1::text::pg_catalog.regcollation",
        &[&name]).await?;
    Ok(Some((row.try_get(0)?, row.try_get(1)?)))
}
