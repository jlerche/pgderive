//! One catalog snapshot for source identity, publication scope and row layouts.
use super::{Identity, Relation};
use anyhow::{Result, ensure};
use serde::Deserialize;
use tokio_postgres::GenericClient;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inspected {
    layout: Relation,
    supported: bool,
}

pub(super) async fn inspect(
    sql: &(impl GenericClient + Sync),
    identity: &Identity,
    publication: &str,
) -> Result<Vec<Relation>> {
    let row =
        sql.query_typed_one(LAYOUT, &[(&publication, tokio_postgres::types::Type::TEXT)]).await?;
    ensure!(
        row.try_get::<_, String>(0)? == identity.system
            && row.try_get::<_, i64>(1)? == i64::from(identity.timeline)
            && row.try_get::<_, String>(2)? == identity.database,
        "SQL source cluster/timeline/database identity mismatch"
    );
    ensure!(row.try_get::<_, bool>(3)?, "unsupported source database/publication scope");
    let layouts: Vec<Inspected> = serde_json::from_value(row.try_get(4)?)?;
    ensure!(!layouts.is_empty(), "source publication is empty");
    layouts
        .into_iter()
        .map(|item| {
            ensure!(item.supported, "unsupported source table/column or replica identity");
            super::contract::validate_columns(&item.layout.columns)?;
            Ok(item.layout)
        })
        .collect()
}

const LAYOUT: &str = r"
SELECT s.system_identifier::text, ck.timeline_id::bigint, current_database(),
       COALESCE(NOT p.puballtables AND p.pubinsert AND p.pubupdate AND p.pubdelete
           AND p.pubtruncate AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_publication_namespace pn WHERE pn.pnpubid=p.oid
           ), false),
       COALESCE((
           SELECT jsonb_agg(jsonb_build_object(
               'layout', jsonb_build_object('oid', c.oid::bigint, 'schema', n.nspname,
                   'table', c.relname, 'columns', attrs.columns),
               'supported', c.relkind='r' AND c.relpersistence='p' AND c.relreplident='f'
                   AND pr.prattrs IS NULL AND pr.prqual IS NULL AND attrs.supported
                   AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
                       WHERE i.inhrelid=c.oid OR i.inhparent=c.oid)
           ) ORDER BY c.oid)
           FROM pg_catalog.pg_publication_rel pr
           JOIN pg_catalog.pg_class c ON c.oid=pr.prrelid
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           CROSS JOIN LATERAL (
               SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'position', a.attnum, 'name', a.attname, 'oid', a.atttypid::bigint,
                   'modifier', a.atttypmod, 'nullable', NOT a.attnotnull,
                   'collation', a.attcollation::bigint, 'primary', EXISTS (
                       SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid=a.attrelid
                           AND i.indisprimary AND a.attnum=ANY(i.indkey)
                   )) ORDER BY a.attnum), '[]'::jsonb) AS columns,
                   COALESCE(bool_and(a.attgenerated='' AND COALESCE(co.collisdeterministic,true)),true) AS supported
               FROM pg_catalog.pg_attribute a
               LEFT JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation
               WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
           ) attrs WHERE pr.prpubid=p.oid
       ), '[]'::jsonb)
FROM pg_catalog.pg_control_system() s
CROSS JOIN pg_catalog.pg_control_checkpoint() ck
LEFT JOIN pg_catalog.pg_publication p ON p.pubname=$1
";
