use super::{count_oracle, projection_oracle};
use crate::{
    engine::{
        Circuit, GroupedCount, IncrementalJoin, ZSet,
        dataflow::{Stream, TraceQuery},
    },
    transaction::Row,
    weighted::Batch,
};
use anyhow::{Context, Result, ensure};
use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::Client;

type ObjectQuery = TraceQuery<String, Row, Row, Option<String>>;

type Joined = (String, Row, Row);

#[derive(Clone, Default)]
struct QueryState {
    engine: IncrementalJoin<String, Row, Row>,
    output: ZSet<Joined>,
    projected: ZSet<projection_oracle::Projected>,
    counts: GroupedCount<Option<String>>,
    count_output: ZSet<count_oracle::CountRow>,
}

#[derive(Default)]
pub(super) struct JoinFixture {
    circuit: Circuit<QueryState>,
    trace: Option<ObjectQuery>,
}

type Inputs = (ZSet<(String, Row)>, ZSet<(String, Row)>);

// Fixed acyclic graph: source deltas -> join -> filter/map -> integrated bags.
fn evaluate(state: &mut QueryState, input: &Inputs) -> Result<usize> {
    let step = state.engine.step(&input.0, &input.1)?;
    state.output.apply(&step.delta)?;
    state.projected.apply(&projection_oracle::project(&step.delta)?)?;
    let count_delta = state.counts.step(&count_oracle::input(&step.delta)?)?;
    state.count_output.apply(&count_delta)?;
    Ok(step.delta.iter().count())
}

impl JoinFixture {
    pub(super) async fn verify(&mut self, sql: &Client, schema: &str, batch: &Batch) -> Result<()> {
        let left = input(batch, "auction", "id")?;
        let right = input(batch, "bid", "auction")?;
        let inputs = (left, right);
        let step = self.circuit.step(&inputs, evaluate)?;
        self.verify_trace(sql, schema, step.time, &inputs).await?;
        let state = self.circuit.state();
        projection_oracle::verify(sql, schema, &state.projected).await?;
        count_oracle::verify(sql, schema, &state.count_output).await?;
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
            state.output == ZSet::from_updates(rows)?,
            "incremental auction/bid join differs from SQL at logical tick {}",
            step.time
        );
        eprintln!(
            "join oracle passed at logical tick {} ({} result deltas)",
            step.time, step.output
        );
        Ok(())
    }
    async fn verify_trace(
        &mut self,
        sql: &Client,
        schema: &str,
        time: u64,
        inputs: &Inputs,
    ) -> Result<()> {
        if self.trace.is_none() {
            self.trace = Some(TraceQuery::new(
                Arc::new(InMemory::new()),
                "nexmark-rows-v1".into(),
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
        let left = Stream {
            time,
            batch: crate::engine::Batch::from_updates(
                inputs.0.iter().map(|(row, w)| (row.clone(), *w)),
            )?,
        };
        let right = Stream {
            time,
            batch: crate::engine::Batch::from_updates(
                inputs.1.iter().map(|(row, w)| (row.clone(), *w)),
            )?,
        };
        let prepared = graph.prepare(&left, &right).await?;
        graph.commit(prepared)?;
        let counts = graph.snapshot().counts.materialize().await?;
        let output = ZSet::from_updates(counts.iter().map(|(row, w)| (row.clone(), *w)))?;
        count_oracle::verify(sql, schema, &output).await?;
        ensure!(
            output == self.circuit.state().count_output,
            "trace counts differ from memory graph"
        );
        eprintln!("object trace count oracle passed at logical tick {time}");
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
