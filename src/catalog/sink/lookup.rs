//! Initialize ordinary fixed-width candidate lookup; exact comparisons stay in DML.
use super::{Catalog, Sink, layout, rows::quote};
use anyhow::{Result, ensure};
use tokio_postgres::Transaction;
pub(super) async fn ensure(
    tx: &Transaction<'_>,
    catalog: &Catalog,
    sink: &Sink,
    create: bool,
) -> Result<()> {
    let table = format!("{}.{}", catalog.schema, sink.table.as_str());
    let oid: u32 =
        tx.query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&table]).await?.try_get(0)?;
    let name = format!("pgderive_sink_lookup_{oid}");
    let fields = layout::names(sink);
    if create {
        let expression = layout::hash(&fields.iter().map(|field| quote(field)).collect::<Vec<_>>());
        tx.batch_execute(&format!("CREATE INDEX {} ON {table} ({expression})", quote(&name)))
            .await?;
    }
    let qualified = format!("{}.{}", catalog.schema, name);
    let valid:bool=tx.query_one("SELECT coalesce((SELECT i.indrelid=$2 AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique AND i.indpred IS NULL AND i.indnkeyatts=1 AND i.indnatts=1 AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='btree') AND pg_catalog.pg_get_indexdef(i.indexrelid,1,false)=(CASE WHEN pg_catalog.pg_function_is_visible('pg_catalog.hash_record_extended(record,bigint)'::regprocedure) THEN 'hash_record_extended' ELSE 'pg_catalog.hash_record_extended' END)||'(ROW('||(SELECT pg_catalog.string_agg(pg_catalog.quote_ident(n),', ' ORDER BY ordinal) FROM pg_catalog.unnest($3::text[]) WITH ORDINALITY AS names(n,ordinal))||'), (0)::bigint)' FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE i.indexrelid=pg_catalog.to_regclass($1)),false)", &[&qualified,&oid,&fields]).await?.try_get(0)?;
    ensure!(valid, "result lookup index definition changed; restore the index before restart");
    Ok(())
}
