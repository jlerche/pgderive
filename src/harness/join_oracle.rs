use super::projection_oracle;
use crate::{
    engine::{IncrementalJoin, ZSet},
    transaction::Row,
    weighted::Batch,
};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

type Joined = (String, Row, Row);

#[derive(Default)]
pub(super) struct JoinFixture {
    engine: IncrementalJoin<String, Row, Row>,
    output: ZSet<Joined>,
    projected: ZSet<projection_oracle::Projected>,
}

impl JoinFixture {
    pub(super) async fn verify(&mut self, sql: &Client, schema: &str, batch: &Batch) -> Result<()> {
        let left = input(batch, "auction", "id")?;
        let right = input(batch, "bid", "auction")?;
        let step = self.engine.step(&left, &right)?;
        self.output.apply(&step.delta)?;
        self.projected.apply(&projection_oracle::project(&step.delta)?)?;
        projection_oracle::verify(sql, schema, &self.projected).await?;
        let query = format!("SELECT a.id::text,
            jsonb_build_object('id',a.id::text,'seller',a.seller::text,'category',a.category::text),
            jsonb_build_object('id',b.id::text,'auction',b.auction::text,'bidder',b.bidder::text,'price',b.price::text)
            FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction");
        let mut rows = Vec::new();
        for row in sql.query(&query, &[]).await? {
            let auction: serde_json::Value = row.get(1);
            let bid: serde_json::Value = row.get(2);
            let tuple =
                (row.get(0), serde_json::from_value(auction)?, serde_json::from_value(bid)?);
            rows.push((tuple, 1));
        }
        ensure!(
            self.output == ZSet::from_updates(rows)?,
            "incremental auction/bid join differs from SQL at logical tick {}",
            step.time
        );
        eprintln!(
            "join oracle passed at logical tick {} ({} result deltas)",
            step.time,
            step.delta.iter().count()
        );
        Ok(())
    }
}

fn input(batch: &Batch, table: &str, column: &str) -> Result<ZSet<(String, Row)>> {
    let mut rows = Vec::new();
    for update in batch.updates.iter().filter(|update| update.tuple.table == table) {
        let value = update.tuple.row.get(column).context("missing fixture join column")?;
        // SQL equality does not match NULL join keys.
        if let Some(key) = value {
            rows.push(((key.clone(), update.tuple.row.clone()), update.weight));
        }
    }
    ZSet::from_updates(rows)
}
