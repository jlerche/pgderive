use super::projection_oracle::{Joined, number};
use crate::engine::ZSet;
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) type CountRow = (Option<String>, i64);
pub(super) type CountInput = (Option<String>, Joined);

pub(super) fn input(delta: &ZSet<Joined>) -> Result<ZSet<CountInput>> {
    delta
        .try_filter(|(_, _, bid)| Ok(number(bid, "price")?.is_some_and(|price| price >= 205)))?
        .try_map(|tuple| {
            let category = tuple
                .1
                .get("category")
                .ok_or_else(|| anyhow::anyhow!("missing fixture category"))?;
            Ok((category.clone(), tuple.clone()))
        })
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
