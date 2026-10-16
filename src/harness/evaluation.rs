//! Live `PostgreSQL` evidence for local scalar error domains; no placement is enabled.
use crate::{
    compiler::{compile, relational::Node},
    source::Contract,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, error::SqlState};

pub(super) async fn check(sql: &Client, schema: &str, contract: &Contract) -> Result<()> {
    for (expression, bidder) in [
        ("CASE WHEN bidder > 0 THEN bidder ELSE price + 1 END", Some(1_i64)),
        ("CASE WHEN bidder > 0 THEN bidder ELSE price + 1 END", Some(0)),
        ("CASE WHEN price + 1 > 0 THEN bidder ELSE 0 END", Some(1)),
        ("COALESCE(bidder, price + 1)", Some(1)),
        ("COALESCE(bidder, price + 1)", None),
        ("CASE WHEN bidder > 0 THEN bidder ELSE price::smallint END", Some(1)),
        ("CASE WHEN bidder > 0 THEN bidder ELSE price::smallint END", None),
    ] {
        compare(sql, schema, contract, expression, bidder).await?;
    }
    super::functions::check(sql, schema, contract).await?;
    average(sql).await?;
    filter_boundary(sql, schema).await?;
    eprintln!(
        "MVP placement error domains: local CASE/COALESCE/casts matched PostgreSQL; filter-crossing counterexample retained"
    );
    Ok(())
}
async fn compare(
    sql: &Client,
    schema: &str,
    contract: &Contract,
    expression: &str,
    bidder: Option<i64>,
) -> Result<()> {
    let compiled = compile(
        &format!(
            "SELECT q.value,COUNT(*) AS n FROM (SELECT {expression} AS value FROM {schema}.bid) q GROUP BY q.value"
        ),
        contract,
    )?;
    let native = compiled
        .relational()
        .nodes
        .iter()
        .find_map(
            |node| {
                if let Node::Map { computed, .. } = node { computed.first() } else { None }
            },
        )
        .with_context(|| format!("error-domain fixture requires local scalar: {expression}"))?;
    let row = Row::from([
        ("0:price".into(), Some(i64::MAX.to_string())),
        ("0:bidder".into(), bidder.map(|value| value.to_string())),
    ]);
    let local = native.expression.evaluate(&row);
    let query = format!(
        "SELECT ({expression})::text FROM (VALUES ($1::bigint,$2::bigint)) AS q(price,bidder)"
    );
    let postgres = sql.query_one(&query, &[&i64::MAX, &bidder]).await;
    match (local, postgres) {
        (Ok(local), Ok(postgres)) => ensure!(
            local == postgres.try_get::<_, Option<String>>(0)?,
            "scalar value differs: {expression}"
        ),
        (Err(_), Err(postgres)) => ensure!(
            postgres.code() == Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE),
            "unexpected PostgreSQL error: {postgres}"
        ),
        (local, postgres) => anyhow::bail!(
            "scalar error domain differs: {expression}: local={local:?}; PostgreSQL={postgres:?}"
        ),
    }
    Ok(())
}
async fn filter_boundary(sql: &Client, schema: &str) -> Result<()> {
    let table = format!("{schema}.placement_error_domain");
    sql.batch_execute(&format!("CREATE TABLE {table}(price bigint,keep integer)")).await?;
    sql.execute(&format!("INSERT INTO {table} VALUES($1,0)"), &[&i64::MAX]).await?;
    // A physical row prevents bound parameters/VALUES from becoming planner constants.
    let guarded = format!("SELECT (price+1)::text FROM {table} WHERE keep>0");
    ensure!(sql.query(&guarded, &[]).await?.is_empty(), "filtered scalar evaluated eagerly");
    let eager = format!(
        "WITH q AS MATERIALIZED (SELECT price+1 AS value,keep FROM {table}) SELECT value FROM q WHERE keep>0"
    );
    let error = sql
        .query(&eager, &[])
        .await
        .err()
        .context("filter-crossing counterexample did not overflow")?;
    ensure!(
        error.code() == Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE),
        "unexpected filter counterexample error: {error}"
    );
    Ok(())
}

async fn average(sql: &Client) -> Result<()> {
    for values in [
        vec![0],
        vec![1, 2, 2],
        vec![-1, -2, -2],
        vec![i64::MAX, i64::MAX, -1],
        vec![i64::MIN, i64::MAX],
        vec![1, 0, 0],
        vec![10000, 1],
        vec![1; 10001],
    ] {
        let sum: num_bigint::BigInt =
            values.iter().map(|value| num_bigint::BigInt::from(*value)).sum();
        let ratio = format!("{sum}/{}", values.len());
        let local = crate::numeric::Numeric::average(&ratio)?;
        let expected: String = sql
            .query_one("SELECT avg(value)::text FROM unnest($1::bigint[]) AS value", &[&values])
            .await?
            .try_get(0)?;
        ensure!(local == expected, "engine AVG differs for {ratio}: {local} vs {expected}");
    }
    eprintln!("MVP engine AVG finalization matched PostgreSQL scale and rounding");
    Ok(())
}
