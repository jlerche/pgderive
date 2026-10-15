//! Restricted replay-safe terminal scalar maps evaluated inside PG publication.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Transaction};

pub mod program;

pub use program::OutputMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Environment {
    semantics: crate::source::Environment,
    functions: Vec<(String, Value)>,
    collations: Vec<Value>,
    calendar: Option<Value>,
}
/// Bound pure tuple-local scalar map; its raw input bag remains object-backed.
/// Only concrete built-in signatures are permitted, never arbitrary SQL callbacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Terminal {
    types: Vec<u32>,
    environment: Option<Box<Environment>>,
    program: program::Program,
}
impl Terminal {
    pub(crate) fn native_rendering(&self) -> bool {
        self.program.predicate.is_none()
            && self.program.outputs.iter().all(|output| {
                matches!(
                    self.program.expressions.get(output.expression),
                    Some(program::Expression::Input(_))
                )
            })
    }
    pub(crate) fn new(transforms: Vec<OutputMap>, types: Vec<u32>) -> Result<Self> {
        Self::from_program(program::Program::outputs(transforms, &types)?, types)
    }
    pub(crate) fn from_program(program: program::Program, types: Vec<u32>) -> Result<Self> {
        let result = Self { types, environment: None, program };
        result.signatures()?;
        Ok(result)
    }
    fn signatures(&self) -> Result<Vec<(&'static str, &'static str)>> {
        ensure!((1..=64).contains(&self.types.len()), "invalid terminal input layout");
        self.program.signatures(&self.types)
    }
    async fn inspect(&self, sql: &(impl GenericClient + Sync)) -> Result<Environment> {
        let semantics = crate::source::Environment::inspect(sql, &[]).await?;
        let mut functions = Vec::new();
        for (signature, implementation) in self.signatures()? {
            let row = sql.query_one("SELECT jsonb_build_object('oid',p.oid,'source',p.prosrc,'binary',p.probin,'config',p.proconfig,'args',p.proargtypes::text,'result',p.prorettype,'kind',p.prokind,'volatile',p.provolatile,'strict',p.proisstrict,'language',l.lanname,'schema',n.nspname,'definition',to_jsonb(p),'language_definition',to_jsonb(l)) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=pg_catalog.to_regprocedure($1)", &[&signature]).await?;
            let value: Value = row.try_get(0)?;
            let (argument, result, strict) = implementation_types(implementation)?;
            ensure!(
                value["args"] == argument && value["result"] == result,
                "terminal built-in {signature} types changed"
            );
            // General to_char is stable; the closed templates use only numeric
            // calendar fields, with UTC attested separately when formatting.
            ensure!(
                value["source"] == implementation
                    && value["binary"].is_null()
                    && value["config"].is_null()
                    && value["kind"] == "f"
                    && value["definition"]["proretset"] == false
                    && value["definition"]["provariadic"] == "0"
                    && value["definition"]["prosecdef"] == false
                    && value["definition"]["pronargdefaults"] == 0
                    && value["volatile"]
                        == if implementation == "timestamp_to_char" { "s" } else { "i" }
                    && value["strict"] == strict
                    && value["language"] == "internal"
                    && value["schema"] == "pg_catalog",
                "terminal built-in {signature} has incompatible implementation"
            );
            functions.push((signature.into(), value));
        }
        let collations = if self.program.has_collation() {
            vec![array_collation(sql).await?]
        } else {
            Vec::new()
        };
        let calendar =
            if self.program.has_calendar() { Some(calendar_context(sql).await?) } else { None };
        Ok(Environment { semantics, functions, collations, calendar })
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
    pub(crate) async fn evaluate(
        &self,
        tx: &Transaction<'_>,
        rows: &[(Value, i64)],
    ) -> Result<Vec<(Value, i64)>> {
        tx.batch_execute("SET LOCAL search_path=pg_catalog; SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,MDY'; SET LOCAL IntervalStyle='iso_8601'; SET LOCAL extra_float_digits=3").await?;
        self.verify(tx).await?;
        // Only closed tuple-local forms render here; calendar formats have fixed
        // fields and an attested UTC context. Relation reads, clocks and SRFs do not.
        let (columns, predicate) = self.program.render(&self.types)?;
        let query = format!(
            "WITH raw AS (SELECT value->0 tuple,(value->>1)::bigint weight FROM pg_catalog.jsonb_array_elements($1)), mapped AS (SELECT pg_catalog.jsonb_build_array(NULL,pg_catalog.jsonb_build_array({columns})) tuple,weight FROM raw WHERE {predicate}) SELECT tuple,pg_catalog.sum(weight)::text FROM mapped GROUP BY tuple HAVING pg_catalog.sum(weight)<>0"
        );
        let input = json!(rows);
        let parameters = json!(&self.program);
        let rows = tx.query(&query, &[&input, &parameters]).await?;
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
        "timestamp_to_char" => Ok(("1114 25", "25", true)),
        "timestamptz_zone" => Ok(("25 1184", "1114", true)),
        "textlen" => Ok(("25", "23", true)),
        "numeric_in" => Ok(("2275 26 23", "1700", true)),
        "numeric_mul" | "numeric_div" => Ok(("1700 1700", "1700", true)),
        "split_part" => Ok(("25 25 23", "25", true)),
        "text_to_array" => Ok(("25 25", "1009", false)),
        _ => program::implementation_types(implementation),
    }
}

async fn calendar_context(sql: &(impl GenericClient + Sync)) -> Result<Value> {
    let row = sql.query_one("SELECT jsonb_build_object('abbreviations',current_setting('timezone_abbreviations'),'utc_offset',extract(epoch FROM utc_offset)::bigint,'dst',is_dst) FROM pg_catalog.pg_timezone_abbrevs WHERE abbrev='UTC'", &[]).await?;
    let value: Value = row.try_get(0)?;
    ensure!(
        value["utc_offset"] == 0 && value["dst"] == false,
        "terminal UTC abbreviation must retain zero offset without daylight saving"
    );
    Ok(value)
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
