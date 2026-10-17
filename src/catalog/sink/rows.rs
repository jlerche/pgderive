//! Ordinary result rows; no coefficients or private occurrence columns live in PG.
use super::Sink;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// Native output column identity, fixed during initialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultColumn {
    /// `PostgreSQL` result label.
    pub name: String,
    /// Qualified built-in type OID.
    pub oid: u32,
    /// `PostgreSQL` type modifier.
    pub modifier: i32,
    /// Namespace and name of the resolved collation, if applicable.
    pub collation: Option<(String, String)>,
}

pub(super) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn validate(columns: &[ResultColumn]) -> Result<()> {
    ensure!((1..=64).contains(&columns.len()), "invalid native result column count");
    let mut names = BTreeSet::new();
    for column in columns {
        ensure!(
            !column.name.is_empty()
                && column.name.len() <= 63
                && !column.name.contains('\0')
                && names.insert(&column.name),
            "result columns require unique nonempty names of at most 63 bytes"
        );
        native(column.oid)?;
        column.type_spec()?;
        if let Some((schema, name)) = &column.collation {
            ensure!(
                !schema.is_empty()
                    && !name.is_empty()
                    && !schema.contains('\0')
                    && !name.contains('\0'),
                "invalid result collation"
            );
        }
    }
    Ok(())
}

impl ResultColumn {
    pub(super) fn type_spec(&self) -> Result<String> {
        let (name, _) = native(self.oid)?;
        let modifier = self.modifier;
        if modifier == -1 {
            return Ok(name.into());
        }
        let parameters = match self.oid {
            1043 => {
                ensure!(modifier > 4, "invalid varchar result modifier");
                (modifier - 4).to_string()
            }
            1114 | 1184 => {
                ensure!((0..=6).contains(&modifier), "invalid timestamp result precision");
                modifier.to_string()
            }
            1700 => {
                ensure!(modifier >= 4, "invalid numeric result modifier");
                let encoded = modifier - 4;
                let precision = (encoded >> 16) & 65_535;
                let scale = ((encoded & 2047) ^ 1024) - 1024;
                ensure!(
                    (1..=1000).contains(&precision) && (-1000..=1000).contains(&scale),
                    "invalid numeric precision/scale"
                );
                format!("{precision},{scale}")
            }
            _ => anyhow::bail!("unsupported result type modifier"),
        };
        Ok(format!("{name}({parameters})"))
    }
    fn declaration(&self) -> Result<String> {
        let collation = self.collation.as_ref().map_or_else(String::new, |(schema, name)| {
            format!(" COLLATE {}.{}", quote(schema), quote(name))
        });
        Ok(format!("{} pg_catalog.{}{collation}", quote(&self.name), self.type_spec()?))
    }
    pub(super) fn value(&self, tuple: &str, index: usize) -> Result<String> {
        Ok(if self.oid == 1009 {
            format!(
                "CASE WHEN {tuple}->1->{index}='null'::jsonb THEN NULL ELSE ARRAY(SELECT pg_catalog.jsonb_array_elements_text({tuple}->1->{index})) END"
            )
        } else {
            format!("({tuple}->1->>{index})::pg_catalog.{}", self.type_spec()?)
        })
    }
}

pub(super) fn declaration(columns: &[ResultColumn]) -> Result<String> {
    validate(columns)?;
    Ok(columns.iter().map(ResultColumn::declaration).collect::<Result<Vec<_>>>()?.join(","))
}

pub(super) fn native(oid: u32) -> Result<(&'static str, &'static str)> {
    Ok(match oid {
        16 => ("bool", "boolsend"),
        20 => ("int8", "int8send"),
        21 => ("int2", "int2send"),
        23 => ("int4", "int4send"),
        25 | 1043 => (if oid == 25 { "text" } else { "varchar" }, "textsend"),
        2950 => ("uuid", "uuid_send"),
        1114 => ("timestamp", "timestamp_send"),
        1184 => ("timestamptz", "timestamptz_send"),
        1700 => ("numeric", "numeric_send"),
        1009 => ("_text", "array_send"),
        _ => anyhow::bail!("unsupported public result type OID {oid}"),
    })
}

pub(super) async fn verify(
    sql: &(impl tokio_postgres::GenericClient + Sync),
    sink: &Sink,
    oid: u32,
) -> Result<()> {
    let row = sql.query_one("SELECT c.relkind::text,c.relreplident::text,c.relrowsecurity,c.relforcerowsecurity,c.relhasrules,EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal),c.relpersistence::text FROM pg_catalog.pg_class c WHERE c.oid=$1", &[&oid]).await?;
    ensure!(
        row.try_get::<_, String>(0)? == "r"
            && row.try_get::<_, String>(1)? == "f"
            && !row.try_get::<_, bool>(2)?
            && !row.try_get::<_, bool>(3)?
            && !row.try_get::<_, bool>(4)?
            && !row.try_get::<_, bool>(5)?
            && row.try_get::<_, String>(6)? == "p",
        "result table kind, persistence, replica identity, RLS, rules or triggers changed"
    );
    let actual: Value = sql.query_one("SELECT coalesce(jsonb_agg(jsonb_build_array(a.attname,a.atttypid::bigint,a.atttypmod,CASE WHEN a.attcollation=0 THEN NULL ELSE jsonb_build_array(n.nspname,c.collname) END,a.attnotnull,a.attgenerated,a.attidentity,a.atthasdef) ORDER BY a.attnum),'[]'::jsonb) FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_collation c ON c.oid=a.attcollation LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped", &[&oid]).await?.try_get(0)?;
    let expected = if let Sink::Rows { columns, .. } = sink {
        Value::Array(
            columns
                .iter()
                .map(|column| {
                    serde_json::json!([
                        column.name,
                        column.oid,
                        column.modifier,
                        column.collation,
                        false,
                        "",
                        "",
                        false
                    ])
                })
                .collect(),
        )
    } else {
        serde_json::json!([["tuple", 3802, -1, null, true, "", "", false]])
    };
    ensure!(
        actual == expected,
        "registered result table layout changed; rebuild or restore required"
    );
    Ok(())
}
