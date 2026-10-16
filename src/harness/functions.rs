//! Connected `PostgreSQL` is a reference oracle only, never a runtime evaluator.
use crate::{
    compiler::{compile, relational::Node},
    source::Contract,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

const EXPRESSIONS: &[&str] = &[
    "abs(id)",
    "abs(id::integer)",
    "abs(id::smallint)",
    "length(name)",
    "char_length(name)",
    "character_length(name)",
    "octet_length(name)",
    "bit_length(name)",
    "ascii(name)",
    "chr(id::integer)",
    "reverse(name)",
    "left(name,id::integer)",
    "right(name,id::integer)",
    "btrim(name)",
    "ltrim(name,'é🙂 ')",
    "rtrim(name,'é🙂 ')",
    "replace(name,'é','🙂x')",
    "replace(name,'','x')",
    "translate(name,'é🙂é','abz')",
    "translate(name,'é🙂','a')",
    "strpos(name,'🙂')",
    "strpos(name,'')",
    "starts_with(name,'é')",
    "repeat(name,id::integer)",
    "split_part(name,'🙂',id::integer)",
    "split_part(name,'',id::integer)",
    "substr(name,id::integer)",
    "substr(name,id::integer,2)",
    "substring(name,1,id::integer)",
    "to_hex(id)",
    "to_hex(id::integer)",
    "length(reverse(name))",
    "length(name::varchar)",
    "COALESCE(name::varchar,'empty')",
    "CASE WHEN id>0 THEN name::varchar ELSE 'other' END",
    "CASE WHEN id>0 THEN reverse(name) ELSE name END",
    "COALESCE(reverse(name),'empty')",
];
pub(super) async fn check(sql: &Client, schema: &str, contract: &Contract) -> Result<()> {
    let cases = [
        (Some("é🙂a\u{301} é"), Some(2_i64)),
        (Some(""), Some(0)),
        (Some("é🙂🙂x🙂"), Some(-1)),
        (Some("é🙂x"), Some(-2)),
        (Some("\t\n x \t"), Some(1)),
        (None, Some(2)),
        (Some("é"), None),
        (Some("x"), Some(i64::MIN)),
        (Some("x"), Some(-2_147_483_648)),
        (Some("x"), Some(55_296)),
        (Some("x"), Some(1_114_112)),
    ];
    for expression in EXPRESSIONS {
        let compiled =
            compile(&format!("SELECT {expression} AS value FROM {schema}.person"), contract)?;
        let native =
            compiled
                .relational()
                .nodes
                .iter()
                .find_map(|node| {
                    if let Node::Map { computed, .. } = node { computed.last() } else { None }
                })
                .context("function fixture missing local computation")?;
        let query = format!("SELECT {expression} FROM (VALUES ($1::text,$2::bigint)) q(name,id)");
        let statement = sql.prepare(&query).await?;
        ensure!(
            statement.columns()[0].type_().oid() == native.column.oid,
            "function result type differs: {expression}"
        );
        for (name, id) in cases {
            let row = Row::from([
                ("0:name".into(), name.map(str::to_owned)),
                ("0:id".into(), id.map(|value| value.to_string())),
            ]);
            let local = native.expression.evaluate(&row);
            let postgres = sql.query_one(&statement, &[&name, &id]).await;
            if *expression == "right(name,id::integer)" && id == Some(i64::from(i32::MIN)) {
                ensure!(
                    local.is_err() && postgres.is_ok(),
                    "version-dependent right boundary must reject explicitly"
                );
                continue;
            }
            compare(local, postgres, expression)
                .with_context(|| format!("function {expression}; name={name:?}, id={id:?}"))?;
        }
    }
    constant_null(sql, schema, contract).await?;
    eprintln!(
        "MVP typed functions matched PostgreSQL values, result types, NULLs and error domains"
    );
    Ok(())
}
fn compare(
    local: Result<Option<String>>,
    postgres: Result<tokio_postgres::Row, tokio_postgres::Error>,
    expression: &str,
) -> Result<()> {
    match (local, postgres) {
        (Ok(local), Ok(row)) => ensure!(
            local == native_text(&row)?,
            "function value differs: {expression}: {local:?} vs {:?}",
            native_text(&row)?
        ),
        (Err(_), Err(error)) => ensure!(
            error.code().is_some_and(|code| code.code().starts_with("22")
                || (expression == "chr(id::integer)"
                    && code == &tokio_postgres::error::SqlState::PROGRAM_LIMIT_EXCEEDED)),
            "unexpected PostgreSQL function error: {error:?}"
        ),
        (local, postgres) => anyhow::bail!(
            "function error domain differs: {expression}: local={local:?}; PostgreSQL={postgres:?}"
        ),
    }
    Ok(())
}
fn native_text(row: &tokio_postgres::Row) -> Result<Option<String>> {
    Ok(match row.columns()[0].type_().oid() {
        16 => row.try_get::<_, Option<bool>>(0)?.map(|value| if value { "t" } else { "f" }.into()),
        21 => row.try_get::<_, Option<i16>>(0)?.map(|value| value.to_string()),
        23 => row.try_get::<_, Option<i32>>(0)?.map(|value| value.to_string()),
        20 => row.try_get::<_, Option<i64>>(0)?.map(|value| value.to_string()),
        _ => row.try_get(0)?,
    })
}

async fn constant_null(sql: &Client, schema: &str, contract: &Contract) -> Result<()> {
    // Physical source prevents VALUES/parameter folding from hiding a row error.
    for expression in [
        "left(NULL,id::smallint)",
        "repeat(NULL,id::smallint)",
        "substr(NULL,id::smallint)",
        "replace(NULL,chr(id::smallint),'x')",
    ] {
        let query = format!("SELECT {expression} AS value FROM {schema}.person");
        let compiled = compile(&query, contract)?;
        let native =
            compiled
                .relational()
                .nodes
                .iter()
                .find_map(|node| {
                    if let Node::Map { computed, .. } = node { computed.last() } else { None }
                })
                .context("constant NULL fixture missing map")?;
        let row = Row::from([("0:id".into(), Some(i64::MAX.to_string()))]);
        ensure!(
            native.expression.evaluate(&row)?.is_none(),
            "constant NULL evaluated row-dependent error"
        );
        // Force an overflowing source expression without mutating fixture rows.
        let query = format!(
            "SELECT {expression} AS value FROM (SELECT id+2147483648 AS id FROM {schema}.person) q"
        );
        for row in sql.query(&query, &[]).await? {
            ensure!(
                row.try_get::<_, Option<String>>(0)?.is_none(),
                "PostgreSQL did not fold strict NULL call"
            );
        }
    }
    Ok(())
}
