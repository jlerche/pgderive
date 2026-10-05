use super::{Model, join_oracle, projection_oracle};
mod catalog_checks;
mod publication;
mod recovery;
use crate::{
    engine::{
        Batch,
        dataflow::{GroupSum, Project, SumState, TimedBatch},
    },
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
pub(super) use recovery::recover_plan;
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::Client;
type Key = String;
type Group = Option<String>;
type Bid = (String, Option<i64>);
pub(super) type Query =
    crate::engine::plan::query::GroupedJoin<Key, Row, Row, Group, Bid, Group, Bid>;
pub(super) struct MvpFixture {
    graph: Query,
    epoch: u64,
    publication: Option<publication::PublicationFixture>,
}
impl MvpFixture {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        limits: crate::engine::execution::Limits,
    ) -> Result<Self> {
        let plan = registered_plan()?;
        let operators = operators();
        let graph = Query::new_with_limits(plan, operators, store, 3, limits)?;
        eprintln!("registered engine query: {}", graph.plan().identity());
        Ok(Self { graph, epoch: 0, publication: None })
    }
    pub(super) fn time(&self) -> u64 {
        self.graph.time()
    }
    pub(super) async fn verify(
        &mut self,
        sql: &mut Client,
        schema: &str,
        transaction: &crate::transaction::Transaction,
        model: &Model,
    ) -> Result<()> {
        let time = self.graph.time() + 1;
        let batch = &transaction.batch;
        let input = TimedBatch {
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
        self.publication
            .as_mut()
            .context("publication fixture not initialized")?
            .publish(sql, &mut self.graph, prepared, transaction)
            .await?;
        let state = self.graph.snapshot();
        let expected = memory(model)?;
        ensure!(
            state.sums.materialize().await? == expected,
            "MVP SUM statistics differ from independent memory oracle"
        );
        let visible = state.output.materialize().await?;
        let query = format!(
            "SELECT a.category::text,COUNT(*),SUM(b.price)::bigint FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction GROUP BY a.category"
        );
        let rows = sql.query(&query, &[]).await?.into_iter().map(|row| {
            ((row.get::<_, Group>(0), (row.get::<_, i64>(1), row.get::<_, Option<i64>>(2))), 1)
        });
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
        self.graph.compact().await?;
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
type QueryOperators = crate::engine::plan::query::Operators<Key, Row, Row, Group, Bid, Group, Bid>;
pub(super) fn operators() -> QueryOperators {
    use crate::engine::plan::query::Operators;
    Operators {
        left: Project::new(|key: &Key, row: &Row| {
            Ok(Some((key.clone(), row.get("category").context("missing category")?.clone())))
        }),
        right: Project::new(|key: &Key, row: &Row| {
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
    }
}
pub(super) fn input(batch: &weighted::Batch, table: &str, key: &str) -> Result<Batch<Key, Row>> {
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

pub(super) fn registered_plan() -> Result<crate::engine::plan::Plan> {
    use crate::engine::plan::{Arrangement as Registration, Definition, Kind, Node, Plan, Source};
    let nodes = vec![
        Node {
            id: "auction".into(),
            kind: Kind::Source,
            inputs: vec!["auctions".into()],
            schema: "auction-full-row-v1".into(),
        },
        Node {
            id: "bid".into(),
            kind: Kind::Source,
            inputs: vec!["bids".into()],
            schema: "bid-full-row-v1".into(),
        },
        Node {
            id: "left".into(),
            kind: Kind::Project,
            inputs: vec!["auction".into()],
            schema: "mvp-auction-category-v1".into(),
        },
        Node {
            id: "right".into(),
            kind: Kind::Project,
            inputs: vec!["bid".into()],
            schema: "mvp-bid-price-v1".into(),
        },
        Node {
            id: "join".into(),
            kind: Kind::Join,
            inputs: vec!["left".into(), "right".into()],
            schema: "category-bid-v1".into(),
        },
        Node {
            id: "group".into(),
            kind: Kind::Project,
            inputs: vec!["join".into()],
            schema: "group-bid-v1".into(),
        },
        Node {
            id: "aggregate".into(),
            kind: Kind::Aggregate,
            inputs: vec!["group".into()],
            schema: "count-sum-row-v1".into(),
        },
    ];
    let arrangements = [
        ("left", "left", "mvp-auction-category-v1"),
        ("right", "right", "mvp-bid-price-v1"),
        ("sums", "aggregate", "mvp-sum-statistics-v1"),
        ("output", "aggregate", "mvp-visible-count-sums-v1"),
    ]
    .into_iter()
    .map(|(id, node, schema)| Registration {
        id: id.into(),
        node: node.into(),
        schema: schema.into(),
    })
    .collect();
    Plan::new(Definition {
        revision: "auction-bid-count-sum-v1".into(),
        sources: vec![
            Source { id: "auctions".into(), schema: "auction-full-row-v1".into() },
            Source { id: "bids".into(), schema: "bid-full-row-v1".into() },
        ],
        nodes,
        arrangements,
        outputs: vec!["aggregate".into()],
    })
}
