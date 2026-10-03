use crate::{engine::ZSet, transaction::Row};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

pub(super) type Joined = (String, Row, Row);
pub(super) type Projected = (String, Option<String>);

pub(super) fn project(delta: &ZSet<Joined>) -> Result<ZSet<Projected>> {
    delta
        .try_filter(|(_, auction, bid)| {
            Ok(number(auction, "category")? == Some(20)
                && number(bid, "price")?.is_some_and(|price| price >= 205))
        })?
        .try_map(|(_, auction, bid)| {
            let category = auction
                .get("category")
                .and_then(Option::as_ref)
                .context("missing qualifying category")?;
            let bidder = bid.get("bidder").context("missing fixture bidder column")?;
            Ok((category.clone(), bidder.clone()))
        })
}

pub(super) async fn verify(sql: &Client, schema: &str, output: &ZSet<Projected>) -> Result<()> {
    let query = format!(
        "SELECT a.category::text,b.bidder::text,count(*)
        FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction
        WHERE a.category=20 AND b.price>=205 GROUP BY a.category,b.bidder"
    );
    let rows = sql
        .query(&query, &[])
        .await?
        .iter()
        .map(|row| ((row.get(0), row.get(1)), row.get(2)))
        .collect::<Vec<_>>();
    ensure!(*output == ZSet::from_updates(rows)?, "filtered/projected join differs from SQL");
    Ok(())
}

fn number(row: &Row, column: &str) -> Result<Option<i64>> {
    row.get(column)
        .context("missing fixture numeric column")?
        .as_deref()
        .map(str::parse)
        .transpose()
        .context("invalid fixture number")
}
