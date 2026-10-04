use super::{Model, join_oracle, projection_oracle};
use crate::{
    engine::{
        Batch,
        dataflow::{Arrangement, Graph, GroupSum, Join, Project, Stream, SumState},
        trace::TraceSnapshot,
    },
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::Client;
type Key = String;
type Group = Option<String>;
type Bid = (String, Option<i64>);
type Inputs = (Batch<Key, Row>, Batch<Key, Row>);
type Output = Batch<Group, Option<i64>>;
struct State {
    left: TraceSnapshot<Key, Group>,
    right: TraceSnapshot<Key, Bid>,
    sums: TraceSnapshot<Group, SumState>,
    output: TraceSnapshot<Group, Option<i64>>,
}
struct Plan {
    left: Arrangement<Key, Group>,
    right: Arrangement<Key, Bid>,
    sums: Arrangement<Group, SumState>,
    output: Arrangement<Group, Option<i64>>,
    project_left: Project<Key, Row, Key, Group>,
    project_right: Project<Key, Row, Key, Bid>,
    group: Project<Key, (Group, Bid), Group, Bid>,
    sum: GroupSum<Group, Bid>,
}
impl Plan {
    fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        Ok(Self {
            left: Arrangement::new(store.clone(), "mvp-auction-category-v1".into(), 3)?,
            right: Arrangement::new(store.clone(), "mvp-bid-price-v1".into(), 3)?,
            sums: Arrangement::new(store.clone(), "mvp-sum-statistics-v1".into(), 3)?,
            output: Arrangement::new(store, "mvp-visible-sums-v1".into(), 3)?,
            project_left: Project::new(|key: &Key, row: &Row| {
                Ok(Some((key.clone(), row.get("category").context("missing category")?.clone())))
            }),
            project_right: Project::new(|key: &Key, row: &Row| {
                Ok(Some((
                    key.clone(),
                    (
                        row.get("id").and_then(Option::as_ref).context("missing bid id")?.clone(),
                        projection_oracle::number(row, "price")?,
                    ),
                )))
            }),
            group: Project::new(|_: &Key, (category, bid): &(Group, Bid)| {
                Ok(Some((category.clone(), bid.clone())))
            }),
            sum: GroupSum::new(|_: &Group, bid: &Bid| Ok(bid.1)),
        })
    }
    const fn empty(&self) -> State {
        State {
            left: self.left.empty(),
            right: self.right.empty(),
            sums: self.sums.empty(),
            output: self.output.empty(),
        }
    }
    async fn evaluate(&self, state: Arc<State>, input: Stream<Inputs>) -> Result<(State, Output)> {
        let left =
            self.project_left.evaluate(&Stream { time: input.time, batch: input.batch.0 })?;
        let right =
            self.project_right.evaluate(&Stream { time: input.time, batch: input.batch.1 })?;
        let joined = Join.evaluate(&left, &right, &state.left, &state.right).await?;
        let grouped = self.group.evaluate(&joined)?;
        let delta = self.sum.evaluate(&grouped, &state.sums).await?;
        let next = State {
            left: self.left.stage(&state.left, &left).await?,
            right: self.right.stage(&state.right, &right).await?,
            sums: self.sums.stage(&state.sums, &delta.state).await?,
            output: self.output.stage(&state.output, &delta.output).await?,
        };
        Ok((next, delta.output.batch))
    }
    async fn compact(&self, state: Arc<State>) -> Result<State> {
        Ok(State {
            left: self.left.compact(&state.left).await?,
            right: self.right.compact(&state.right).await?,
            sums: self.sums.compact(&state.sums).await?,
            output: self.output.compact(&state.output).await?,
        })
    }
}
pub(super) struct MvpFixture {
    graph: Graph<State, Inputs, Output>,
    plan: Arc<Plan>,
}
impl MvpFixture {
    pub(super) fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        let plan = Arc::new(Plan::new(store)?);
        let evaluator = plan.clone();
        let graph = Graph::new(plan.empty(), move |state, input| {
            let plan = evaluator.clone();
            async move { plan.evaluate(state, input).await }
        });
        Ok(Self { graph, plan })
    }
    pub(super) async fn verify(
        &mut self,
        sql: &Client,
        schema: &str,
        batch: &weighted::Batch,
        model: &Model,
    ) -> Result<()> {
        let time = self.graph.time() + 1;
        let input = Stream {
            time,
            batch: (input(batch, "auction", "id")?, input(batch, "bid", "auction")?),
        };
        let pinned = self.graph.snapshot();
        let prepared = self.graph.prepare(input.clone()).await;
        let prepared = if time == 1 && std::env::var("PGDERIVE_EXPECT_STORAGE_FAILURE").is_ok() {
            ensure!(prepared.is_err(), "expected injected S3 failure was not observed");
            ensure!(
                self.graph.time() == 0 && Arc::ptr_eq(&pinned, &self.graph.snapshot()),
                "storage failure published partial graph state"
            );
            eprintln!("MVP injected storage failure preserved root; retrying same transaction");
            self.graph.prepare(input).await?
        } else {
            prepared?
        };
        self.graph.commit(prepared)?;
        let state = self.graph.snapshot();
        let expected = memory(model)?;
        ensure!(
            state.sums.materialize().await? == expected,
            "MVP SUM statistics differ from independent memory oracle"
        );
        let visible = state.output.materialize().await?;
        let query = format!(
            "SELECT a.category::text,SUM(b.price)::bigint FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction GROUP BY a.category"
        );
        let rows = sql
            .query(&query, &[])
            .await?
            .into_iter()
            .map(|row| ((row.get::<_, Group>(0), row.get::<_, Option<i64>>(1)), 1));
        ensure!(visible == Batch::from_updates(rows)?, "MVP SUM output differs from SQL");
        if time == 5 {
            self.compact().await?;
        }
        ensure!(pinned.output.time() == time - 1, "old reader moved to new tick");
        eprintln!("MVP project/join/group/sum passed SQL+memory at tick {time}");
        Ok(())
    }
    async fn compact(&mut self) -> Result<()> {
        let pinned = self.graph.snapshot();
        let expected = pinned.output.materialize().await?;
        let time = self.graph.time();
        let plan = self.plan.clone();
        let prepared = self
            .graph
            .prepare_maintenance(move |state| async move { plan.compact(state).await })
            .await?;
        self.graph.commit_maintenance(prepared)?;
        ensure!(
            self.graph.time() == time
                && pinned.output.materialize().await? == expected
                && self.graph.snapshot().output.materialize().await? == expected,
            "MVP compaction changed pinned state or logical tick"
        );
        eprintln!("MVP physical compaction preserved tick {time} and pinned reader");
        Ok(())
    }
}
fn input(batch: &weighted::Batch, table: &str, key: &str) -> Result<Batch<Key, Row>> {
    Batch::from_updates(
        join_oracle::input(batch, table, key)?.iter().map(|(row, w)| (row.clone(), *w)),
    )
}
fn memory(model: &Model) -> Result<Batch<Group, SumState>> {
    let mut totals = BTreeMap::<Group, SumState>::new();
    for ((table, _), bid) in model {
        if table != "bid" {
            continue;
        }
        let Some(auction) = bid.get("auction").and_then(Option::as_ref) else {
            continue;
        };
        let Some(auction) = model.get(&("auction".into(), auction.clone())) else {
            continue;
        };
        let category = auction.get("category").context("missing model category")?.clone();
        let total = totals.entry(category).or_insert(SumState { rows: 0, non_null: 0, sum: 0 });
        total.rows += 1;
        if let Some(price) = projection_oracle::number(bid, "price")? {
            total.non_null += 1;
            total.sum = total.sum.checked_add(price).context("memory oracle sum overflow")?;
        }
    }
    Batch::from_updates(totals.into_iter().map(|row| (row, 1)))
}
