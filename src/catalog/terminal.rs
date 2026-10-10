//! Restricted replay-safe terminal scalar maps evaluated inside PG publication.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Transaction};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transform {
    Identity,
    Abs,
    Length,
    Numeric,
    Average,
    NumericMultiply { factor: String },
    ArrayElement { delimiter: String, index: u16 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Environment {
    server: String,
    encoding: String,
    functions: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    collations: Vec<Value>,
}
/// Bound pure tuple-local scalar map; its raw input bag remains object-backed.
/// Only concrete built-in signatures are permitted, never arbitrary SQL callbacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Terminal {
    transforms: Vec<Transform>,
    types: Vec<u32>,
    // Keep attestation metadata behind an allocation; serde identity is unchanged.
    environment: Option<Box<Environment>>,
}
impl Terminal {
    pub(crate) fn new(transforms: Vec<Transform>, types: Vec<u32>) -> Result<Self> {
        let result = Self { transforms, types, environment: None };
        result.signatures()?;
        Ok(result)
    }
    fn signatures(&self) -> Result<Vec<(&'static str, &'static str)>> {
        ensure!(
            !self.types.is_empty()
                && self.types.len() == self.transforms.len()
                && self.types.len() <= 64,
            "invalid terminal layout"
        );
        let mut signatures = Vec::new();
        for (transform, oid) in self.transforms.iter().zip(&self.types) {
            signatures.extend(signatures_for(transform, *oid)?);
        }
        signatures.sort_unstable();
        signatures.dedup();
        ensure!(!signatures.is_empty(), "terminal map has no functions");
        Ok(signatures)
    }
    async fn inspect(&self, sql: &(impl GenericClient + Sync)) -> Result<Environment> {
        let row = sql
            .query_one(
                "SELECT current_setting('server_version_num'),current_setting('server_encoding')",
                &[],
            )
            .await?;
        let server: String = row.try_get(0)?;
        let encoding: String = row.try_get(1)?;
        ensure!(encoding == "UTF8", "terminal codec requires UTF8");
        let mut functions = Vec::new();
        for (signature, implementation) in self.signatures()? {
            let row = sql.query_one("SELECT jsonb_build_object('oid',p.oid,'source',p.prosrc,'binary',p.probin,'config',p.proconfig,'args',p.proargtypes::text,'result',p.prorettype,'kind',p.prokind,'volatile',p.provolatile,'strict',p.proisstrict,'language',l.lanname,'schema',n.nspname) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=pg_catalog.to_regprocedure($1)", &[&signature]).await?;
            let value: Value = row.try_get(0)?;
            let (argument, result, strict) = implementation_types(implementation)?;
            ensure!(
                value["args"] == argument && value["result"] == result,
                "terminal built-in types changed"
            );
            ensure!(
                value["source"] == implementation
                    && value["binary"].is_null()
                    && value["config"].is_null()
                    && value["kind"] == "f"
                    && value["volatile"] == "i"
                    && value["strict"] == strict
                    && value["language"] == "internal"
                    && value["schema"] == "pg_catalog",
                "terminal built-in signature has incompatible implementation"
            );
            functions.push((signature.into(), value));
        }
        let collations =
            if self.has_array() { vec![array_collation(sql).await?] } else { Vec::new() };
        Ok(Environment { server, encoding, functions, collations })
    }
    pub(crate) async fn bind(&mut self, sql: &(impl GenericClient + Sync)) -> Result<()> {
        let environment = self.inspect(sql).await?;
        if let Some(prior) = &self.environment {
            ensure!(prior.as_ref() == &environment, "terminal expression environment changed");
        }
        self.environment = Some(Box::new(environment));
        Ok(())
    }
    pub(crate) fn validate(&self) -> Result<()> {
        self.signatures()?;
        ensure!(self.environment.is_some(), "terminal expression must bind before registration");
        Ok(())
    }
    pub(crate) async fn verify(&self, sql: &(impl GenericClient + Sync)) -> Result<()> {
        self.validate()?;
        ensure!(
            self.environment.as_deref() == Some(&self.inspect(sql).await?),
            "terminal expression environment changed"
        );
        Ok(())
    }
    pub(crate) fn has_array(&self) -> bool {
        self.transforms.iter().any(|transform| matches!(transform, Transform::ArrayElement { .. }))
    }
    pub(crate) fn has_numeric_multiply(&self) -> bool {
        self.transforms
            .iter()
            .any(|transform| matches!(transform, Transform::NumericMultiply { .. }))
    }
    fn expression(&self, index: usize) -> Result<String> {
        let value = format!("tuple->1->{index}");
        let text = format!("tuple->1->>{index}");
        match (&self.transforms[index], self.types[index]) {
            (Transform::Identity, _) => Ok(value),
            (Transform::Abs, oid) => {
                let native = match oid {
                    20 => "int8",
                    21 => "int2",
                    23 => "int4",
                    _ => anyhow::bail!("invalid ABS type"),
                };
                Ok(format!("pg_catalog.to_jsonb(pg_catalog.abs(({text})::pg_catalog.{native}))"))
            }
            (Transform::Numeric, _) => {
                Ok(format!("pg_catalog.to_jsonb(({text})::pg_catalog.numeric)"))
            }
            (Transform::Average, _) => Ok(format!(
                "pg_catalog.to_jsonb(pg_catalog.numeric_div(pg_catalog.split_part({text},'/',1)::pg_catalog.numeric,pg_catalog.split_part({text},'/',2)::pg_catalog.numeric))"
            )),
            (Transform::ArrayElement { index: subscript, .. }, _) => Ok(format!(
                "pg_catalog.to_jsonb((pg_catalog.string_to_array(({text})::pg_catalog.text COLLATE pg_catalog.\"C\",($2::pg_catalog.jsonb)->{index}->'ArrayElement'->>'delimiter'))[{subscript}])"
            )),
            (Transform::NumericMultiply { .. }, _) => Ok(format!(
                "pg_catalog.to_jsonb(pg_catalog.numeric_mul(({text})::pg_catalog.numeric,(($2::pg_catalog.jsonb)->{index}->'NumericMultiply'->>'factor')::pg_catalog.numeric))"
            )),
            (Transform::Length, _) => {
                Ok(format!("pg_catalog.to_jsonb(pg_catalog.length(({text})::pg_catalog.text))"))
            }
        }
    }
    pub(crate) async fn evaluate(
        &self,
        tx: &Transaction<'_>,
        rows: &[(Value, i64)],
    ) -> Result<Vec<(Value, i64)>> {
        self.verify(tx).await?;
        // Context-sensitive/volatile functions, relation reads, SRFs and relational
        // operators cannot be rendered by this closed expression algebra.
        let columns = (0..self.types.len())
            .map(|index| self.expression(index))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        let query = format!(
            "WITH raw AS (SELECT value->0 tuple,(value->>1)::bigint weight FROM pg_catalog.jsonb_array_elements($1)), mapped AS (SELECT pg_catalog.jsonb_build_array(NULL,pg_catalog.jsonb_build_array({columns})) tuple,weight FROM raw) SELECT tuple,pg_catalog.sum(weight)::text FROM mapped GROUP BY tuple HAVING pg_catalog.sum(weight)<>0"
        );
        let input = json!(rows);
        let rows = if self.has_array() || self.has_numeric_multiply() {
            let parameters = json!(&self.transforms);
            tx.query(&query, &[&input, &parameters]).await?
        } else {
            tx.query(&query, &[&input]).await?
        };
        rows.into_iter()
            .map(|row| {
                let weight: String = row.try_get(1)?;
                Ok((
                    row.try_get(0)?,
                    weight.parse().context("terminal finalized delta coefficient exceeds i64")?,
                ))
            })
            .collect()
    }
}

fn signatures_for(transform: &Transform, oid: u32) -> Result<Vec<(&'static str, &'static str)>> {
    let mut signatures = match (transform, oid) {
        (Transform::Identity, 16 | 20 | 21 | 23 | 25 | 1043 | 2950 | 1114 | 1184) => Vec::new(),
        (Transform::Abs, 20) => vec![("pg_catalog.abs(bigint)", "int8abs")],
        (Transform::Abs, 21) => vec![("pg_catalog.abs(smallint)", "int2abs")],
        (Transform::Abs, 23) => vec![("pg_catalog.abs(integer)", "int4abs")],
        (Transform::ArrayElement { delimiter, index }, 25 | 1043) => {
            ensure!(
                !delimiter.is_empty()
                    && !delimiter.contains('\0')
                    && delimiter.len() <= 64
                    && (1..=1024).contains(index),
                "invalid terminal array selection"
            );
            vec![("pg_catalog.string_to_array(text,text)", "text_to_array")]
        }
        (Transform::NumericMultiply { factor }, 20 | 21 | 23) => {
            validate_factor(factor)?;
            vec![
                ("pg_catalog.numeric_in(cstring,oid,integer)", "numeric_in"),
                ("pg_catalog.numeric_mul(numeric,numeric)", "numeric_mul"),
            ]
        }
        (Transform::Length, 25 | 1043) => vec![("pg_catalog.length(text)", "textlen")],
        (Transform::Numeric | Transform::Average, 1700) => {
            vec![("pg_catalog.numeric_in(cstring,oid,integer)", "numeric_in")]
        }
        _ => anyhow::bail!("SQL type: unsupported terminal function signature"),
    };
    if *transform == Transform::Average {
        signatures.extend([
            ("pg_catalog.numeric_div(numeric,numeric)", "numeric_div"),
            ("pg_catalog.split_part(text,text,integer)", "split_part"),
        ]);
    }
    Ok(signatures)
}
pub fn validate_factor(factor: &str) -> Result<()> {
    let (whole, fraction) = factor.split_once('.').context("decimal factor requires a point")?;
    ensure!(
        !whole.is_empty()
            && !fraction.is_empty()
            && whole.len() <= 32
            && fraction.len() <= 32
            && whole.bytes().chain(fraction.bytes()).all(|byte| byte.is_ascii_digit()),
        "decimal factor requires 1..32 whole and fractional digits"
    );
    Ok(())
}
fn implementation_types(implementation: &str) -> Result<(&'static str, &'static str, bool)> {
    match implementation {
        "int8abs" => Ok(("20", "20", true)),
        "int2abs" => Ok(("21", "21", true)),
        "int4abs" => Ok(("23", "23", true)),
        "textlen" => Ok(("25", "23", true)),
        "numeric_in" => Ok(("2275 26 23", "1700", true)),
        "numeric_mul" | "numeric_div" => Ok(("1700 1700", "1700", true)),
        "split_part" => Ok(("25 25 23", "25", true)),
        "text_to_array" => Ok(("25 25", "1009", false)),
        _ => anyhow::bail!("unknown terminal implementation"),
    }
}

async fn array_collation(sql: &(impl GenericClient + Sync)) -> Result<Value> {
    let row = sql.query_one("SELECT jsonb_build_object('oid',c.oid,'provider',c.collprovider,'deterministic',c.collisdeterministic,'encoding',c.collencoding,'collate',c.collcollate,'ctype',c.collctype,'version',c.collversion,'schema',n.nspname) FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=pg_catalog.to_regcollation('pg_catalog.\"C\"')", &[]).await?;
    let value: Value = row.try_get(0)?;
    ensure!(
        value["provider"] == "c"
            && value["deterministic"] == true
            && value["encoding"] == -1
            && value["collate"] == "C"
            && value["ctype"] == "C"
            && value["version"].is_null()
            && value["schema"] == "pg_catalog",
        "terminal C collation changed"
    );
    Ok(value)
}

#[cfg(test)]
#[path = "terminal/tests.rs"]
mod tests;
