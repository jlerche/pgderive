use super::projection_oracle::{Joined, number};
use crate::engine::ZSet;
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) type CountRow = (Option<String>, i64);

pub(super) fn counts(rows: &ZSet<Joined>) -> Result<ZSet<CountRow>> {
    let mut counts = std::collections::BTreeMap::<Option<String>, num_bigint::BigInt>::new();
    for ((_, auction, bid), weight) in rows.iter() {
        if number(bid, "price")?.is_some_and(|price| price >= 205) {
            let category = auction
                .get("category")
                .ok_or_else(|| anyhow::anyhow!("missing fixture category"))?;
            *counts.entry(category.clone()).or_default() += *weight;
        }
    }
    ZSet::from_updates(
        counts
            .into_iter()
            .filter(|(_, count)| *count != num_bigint::BigInt::default())
            .map(|(key, count)| Ok(((key, i64::try_from(count)?), 1)))
            .collect::<Result<Vec<_>>>()?,
    )
}

pub(super) async fn verify(sql: &Client, schema: &str, output: &ZSet<CountRow>) -> Result<()> {
    let query = format!(
        "SELECT a.category::text,count(*) FROM {schema}.auction a
        JOIN {schema}.bid b ON a.id=b.auction WHERE b.price>=205 GROUP BY a.category"
    );
    let rows = sql
        .query(&query, &[])
        .await?
        .iter()
        .map(|row| ((row.get(0), row.get(1)), 1))
        .collect::<Vec<_>>();
    ensure!(*output == ZSet::from_updates(rows)?, "incremental category counts differ from SQL");
    Ok(())
}
