//! Physical primary-key contract, independent of full-tuple weighted identity.
use super::{Sink, SinkOutput, rows::quote};
use anyhow::{Context as _, Result, ensure};
use std::collections::BTreeSet;

pub(super) fn validate(sink: &Sink) -> Result<()> {
    ensure!(
        sink.primary_key.is_empty() || sink.output == SinkOutput::Postgres,
        "keyed sinks require PostgreSQL output"
    );
    ensure!(sink.primary_key.len() <= 32, "sink primary key exceeds PostgreSQL index column limit");
    let mut seen = BTreeSet::new();
    for name in &sink.primary_key {
        ensure!(
            seen.insert(name) && sink.columns.iter().any(|c| &c.name == name),
            "sink primary key requires distinct existing output columns"
        );
    }
    Ok(())
}
pub(super) fn declaration(sink: &Sink) -> String {
    if sink.primary_key.is_empty() {
        return String::new();
    }
    format!(
        ", PRIMARY KEY ({})",
        sink.primary_key.iter().map(|n| quote(n)).collect::<Vec<_>>().join(",")
    )
}
pub(super) async fn verify(
    sql: &(impl tokio_postgres::GenericClient + Sync),
    sink: &Sink,
    oid: u32,
) -> Result<()> {
    let rows = sql.query("SELECT c.contype::text,c.condeferrable,c.condeferred,c.convalidated,ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(attnum,position) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.position),coalesce(i.indisvalid AND i.indisready AND i.indisunique AND i.indimmediate,false) FROM pg_catalog.pg_constraint c LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=c.conindid WHERE c.conrelid=$1 AND c.contype<>'n'", &[&oid]).await?;
    if sink.primary_key.is_empty() {
        ensure!(rows.is_empty(), "unregistered result table constraints");
        return Ok(());
    }
    ensure!(rows.len() == 1, "result primary key constraints changed");
    let row = &rows[0];
    ensure!(
        row.try_get::<_, String>(0)? == "p"
            && !row.try_get::<_, bool>(1)?
            && !row.try_get::<_, bool>(2)?
            && row.try_get::<_, bool>(3)?
            && row.try_get::<_, Vec<String>>(4)? == sink.primary_key
            && row.try_get::<_, bool>(5)?,
        "result primary key changed or is invalid"
    );
    Ok(())
}

pub(super) fn positions(sink: &Sink) -> Result<Vec<usize>> {
    sink.primary_key
        .iter()
        .map(|name| {
            sink.columns
                .iter()
                .position(|column| &column.name == name)
                .context("sink key column missing")
        })
        .collect()
}
