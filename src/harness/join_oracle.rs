use super::{count_oracle, projection_oracle};
use crate::{
    engine::{ZSet, dataflow::TimedBatch},
    transaction::Row,
    weighted::Batch,
};
use anyhow::{Context, Result, ensure};
use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::Client;

type ObjectQuery = crate::harness::object_oracle::Counts<String, Row, Row, Option<String>>;

type Joined = (String, Row, Row);

#[derive(Default)]
pub(super) struct JoinFixture {
    left: ZSet<(String, Row)>,
    right: ZSet<(String, Row)>,
    time: u64,
    trace: Option<ObjectQuery>,
}
type Inputs = (ZSet<(String, Row)>, ZSet<(String, Row)>);
// Independent full recomputation from integrated raw source bags, never a production operator.
fn recompute(left: &ZSet<(String, Row)>, right: &ZSet<(String, Row)>) -> Result<ZSet<Joined>> {
    let mut rows = Vec::new();
    for ((key, left), weight) in left.iter() {
        for ((other, right), other_weight) in right.iter() {
            if key == other {
                rows.push((
                    (key.clone(), left.clone(), right.clone()),
                    weight.checked_mul(*other_weight).context("oracle weight overflow")?,
                ));
            }
        }
    }
    ZSet::from_updates(rows)
}

impl JoinFixture {
    pub(super) async fn verify(&mut self, sql: &Client, schema: &str, batch: &Batch) -> Result<()> {
        let left = input(batch, "auction", "id")?;
        let right = input(batch, "bid", "auction")?;
        let inputs = (left, right);
        let mut left = self.left.clone();
        let mut right = self.right.clone();
        left.apply(&inputs.0)?;
        right.apply(&inputs.1)?;
        let output = recompute(&left, &right)?;
        let counts = count_oracle::counts(&output)?;
        let projected = projection_oracle::project(&output)?;
        self.time = self.time.checked_add(1).context("oracle logical time overflow")?;
        self.left = left;
        self.right = right;
        self.verify_trace(sql, schema, &inputs, &counts).await?;
        projection_oracle::verify(sql, schema, &projected).await?;
        count_oracle::verify(sql, schema, &counts).await?;
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
            output == ZSet::from_updates(rows)?,
            "incremental auction/bid join differs from SQL at logical tick {}",
            self.time
        );
        eprintln!(
            "join oracle passed at logical tick {} ({} result deltas)",
            self.time,
            output.iter().count()
        );
        Ok(())
    }
    async fn verify_trace(
        &mut self,
        sql: &Client,
        schema: &str,
        inputs: &Inputs,
        memory: &ZSet<count_oracle::CountRow>,
    ) -> Result<()> {
        let time = self.time;
        if self.trace.is_none() {
            self.trace = Some(crate::harness::object_oracle::counts(
                Arc::new(InMemory::new()),
                "nexmark-rows-v1",
                3,
                |_, auction: &Row, bid| {
                    if projection_oracle::number(bid, "price")?.is_none_or(|price| price < 205) {
                        return Ok(None);
                    }
                    Ok(Some(auction.get("category").context("missing fixture category")?.clone()))
                },
            )?);
        }
        let graph = self.trace.as_mut().context("missing trace graph")?;
        let left = TimedBatch {
            time,
            batch: crate::engine::Batch::from_updates(
                inputs.0.iter().map(|(row, w)| (row.clone(), *w)),
            )?,
        };
        let right = TimedBatch {
            time,
            batch: crate::engine::Batch::from_updates(
                inputs.1.iter().map(|(row, w)| (row.clone(), *w)),
            )?,
        };
        let prepared = graph.prepare(crate::harness::object_oracle::inputs(&left, &right)?).await?;
        graph.commit(prepared)?;
        let counts = graph.snapshot().counts.materialize().await?;
        let output = ZSet::from_updates(counts.iter().map(|(row, w)| (row.clone(), *w)))?;
        count_oracle::verify(sql, schema, &output).await?;
        ensure!(&output == memory, "trace counts differ from memory graph");
        eprintln!("object trace count oracle passed at logical tick {time}");
        Ok(())
    }
}

pub(super) fn input(batch: &Batch, table: &str, column: &str) -> Result<ZSet<(String, Row)>> {
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
