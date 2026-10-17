use super::{Model, join_oracle, projection_oracle};
mod catalog_checks;
mod publication;
mod recovery;
use crate::{
    engine::{
        Batch,
        dataflow::{Project, Statistics, TimedBatch},
        plan::relational::{Operator, Operators, Relational},
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
pub(super) type Query = Relational<Key, Row>;
pub(super) struct MvpFixture {
    graph: Query,
    publication: Option<publication::PublicationFixture>,
}
impl MvpFixture {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        limits: crate::engine::execution::Limits,
    ) -> Result<Self> {
        let plan = registered_plan()?;
        let operators = operators();
        let graph = Query::new(
            plan,
            operators,
            &crate::engine::plan::Settings { store, block_rows: 3, limits, encoded_cache: None },
        )?;
        eprintln!("registered engine query: {}", graph.plan().identity());
        Ok(Self { graph, publication: None })
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
            batch: vec![input(batch, "auction", "id")?, input(batch, "bid", "auction")?],
        };
        let pinned = self.graph.snapshot();
        let prepared = self.graph.prepare(input.clone()).await;
        let prepared = if time == 2 && std::env::var("PGDERIVE_EXPECT_STORAGE_FAILURE").is_ok() {
            ensure!(prepared.is_err(), "expected injected S3 failure was not observed");
            ensure!(
                self.graph.time() == 1 && Arc::ptr_eq(&pinned, &self.graph.snapshot()),
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
            state.trace("sums")?.materialize().await? == expected,
            "MVP SUM statistics differ from independent memory oracle"
        );
        let visible = state.trace("output")?.materialize().await?;
        let query = format!(
            "SELECT a.category::text,COUNT(*),SUM(b.price)::bigint FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction GROUP BY a.category"
        );
        let rows = sql
            .query(&query, &[])
            .await?
            .into_iter()
            .map(|row| ((String::new(), visible_row(row.get(0), row.get(1), row.get(2))), 1));
        ensure!(visible == Batch::from_updates(rows)?, "MVP SUM output differs from SQL");
        if time == 6 {
            self.compact(sql).await?;
        }
        ensure!(pinned.trace("output")?.time() == time - 1, "old reader moved to new tick");
        eprintln!("MVP project/join/group/sum passed SQL+memory at tick {time}");
        Ok(())
    }
    async fn compact(&mut self, sql: &mut Client) -> Result<()> {
        let pinned = self.graph.snapshot();
        let expected = pinned.trace("output")?.materialize().await?;
        let time = self.graph.time();
        let work = self.graph.prepare_compaction().await?;
        let publication =
            self.publication.as_mut().context("publication fixture not initialized")?;
        self.graph.publish_compaction(sql, &mut publication.grouped, work).await?;
        ensure!(
            self.graph.time() == time
                && pinned.trace("output")?.materialize().await? == expected
                && self.graph.snapshot().trace("output")?.materialize().await? == expected,
            "MVP compaction changed pinned state or logical tick"
        );
        eprintln!("MVP physical compaction preserved tick {time} and pinned reader");
        Ok(())
    }
}
pub(super) fn operators() -> Operators<Key, Row> {
    let mut projects = BTreeMap::new();
    projects.insert(
        "left".into(),
        Project::new(|key: &Key, row: &Row| {
            Ok(Some((
                key.clone(),
                Row::from([(
                    "category".into(),
                    row.get("category").context("missing category")?.clone(),
                )]),
            )))
        }),
    );
    projects.insert(
        "right".into(),
        Project::new(|key: &Key, row: &Row| {
            Ok(Some((
                key.clone(),
                Row::from([
                    ("bid_id".into(), row.get("id").context("missing bid id")?.clone()),
                    ("price".into(), row.get("price").context("missing price")?.clone()),
                ]),
            )))
        }),
    );
    projects.insert(
        "group".into(),
        Project::new(|_: &Key, row: &Row| {
            Ok(Some((
                serde_json::to_string(row.get("category").context("missing grouping key")?)?,
                row.clone(),
            )))
        }),
    );
    projects.insert(
        "output".into(),
        Project::new(|key: &Key, row: &Row| {
            let rows = integer(row, "rows")?;
            let non_null = integer(row, "non_null")?;
            let sum = integer(row, "sum")?;
            Ok(Some((
                String::new(),
                visible_row(serde_json::from_str(key)?, rows, (non_null != 0).then_some(sum)),
            )))
        }),
    );
    let join = Project::new(|key: &Key, pair: &(Row, Row)| {
        let mut row = pair.0.clone();
        row.extend(pair.1.clone());
        Ok(Some((key.clone(), row)))
    });
    let mut operators: Operators<Key, Row> =
        projects.into_iter().map(|(id, value)| (id, Operator::Project(value))).collect();
    operators.insert("join".into(), Operator::Join(join));
    operators.insert("sums".into(), Operator::Statistics(statistics()));
    operators
}
fn statistics() -> Statistics<Key, Row> {
    Statistics::new(
        3,
        |row: &Row| {
            let price = projection_oracle::number(row, "price")?;
            Ok(vec![1.into(), i64::from(price.is_some()).into(), price.unwrap_or_default().into()])
        },
        |row: &Row| {
            ["rows", "non_null", "sum"]
                .into_iter()
                .map(|name| {
                    row.get(name)
                        .and_then(Option::as_deref)
                        .context("missing statistic")?
                        .parse()
                        .map_err(Into::into)
                })
                .collect()
        },
        |_: &Key, values| {
            let rows = i64::try_from(&values[0])?;
            let non_null = i64::try_from(&values[1])?;
            let sum = i64::try_from(&values[2])?;
            ensure!(
                rows >= 0 && non_null >= 0 && non_null <= rows && (non_null != 0 || sum == 0),
                "invalid group statistics"
            );
            Ok((rows != 0).then(|| {
                Row::from([
                    ("rows".into(), Some(rows.to_string())),
                    ("non_null".into(), Some(non_null.to_string())),
                    ("sum".into(), Some(sum.to_string())),
                ])
            }))
        },
    )
}
fn integer(row: &Row, name: &str) -> Result<i64> {
    row.get(name)
        .and_then(Option::as_deref)
        .context("missing statistic")?
        .parse()
        .map_err(Into::into)
}
pub(super) fn visible_row(category: Group, count: i64, sum: Option<i64>) -> Row {
    Row::from([
        ("category".into(), category),
        ("count".into(), Some(count.to_string())),
        ("sum".into(), sum.map(|value| value.to_string())),
    ])
}
pub(super) fn input(batch: &weighted::Batch, table: &str, key: &str) -> Result<Batch<Key, Row>> {
    Batch::from_updates(
        join_oracle::input(batch, table, key)?.iter().map(|(row, weight)| (row.clone(), *weight)),
    )
}
fn memory(model: &Model) -> Result<Batch<Key, Row>> {
    let mut totals = BTreeMap::<Group, (i64, i64, i64)>::new();
    for ((table, _), bid) in model {
        if table != "bid" {
            continue;
        }
        let Some(auction) = bid
            .get("auction")
            .and_then(Option::as_ref)
            .and_then(|id| model.get(&("auction".into(), id.clone())))
        else {
            continue;
        };
        let category = auction.get("category").context("missing model category")?.clone();
        let total = totals.entry(category).or_default();
        total.0 += 1;
        if let Some(price) = projection_oracle::number(bid, "price")? {
            total.1 += 1;
            total.2 = total.2.checked_add(price).context("memory sum overflow")?;
        }
    }
    Batch::from_updates(
        totals
            .into_iter()
            .map(|(key, (rows, non_null, sum))| {
                Ok((
                    (
                        serde_json::to_string(&key)?,
                        Row::from([
                            ("rows".into(), Some(rows.to_string())),
                            ("non_null".into(), Some(non_null.to_string())),
                            ("sum".into(), Some(sum.to_string())),
                        ]),
                    ),
                    1,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )
}
pub(super) fn registered_plan() -> Result<crate::engine::plan::Plan> {
    use crate::engine::plan::{Arrangement, Definition, Kind, Node, Plan, Source};
    let declarations = [
        ("auction", Kind::Source, vec!["auctions"]),
        ("bid", Kind::Source, vec!["bids"]),
        ("left", Kind::Project, vec!["auction"]),
        ("right", Kind::Project, vec!["bid"]),
        ("join", Kind::Join, vec!["left", "right"]),
        ("group", Kind::Project, vec!["join"]),
        ("sums", Kind::Statistics, vec!["group"]),
        ("output", Kind::Project, vec!["sums"]),
    ];
    Plan::new(Definition {
        revision: "mvp-relational-count-sum-v1".into(),
        sources: vec![
            Source { id: "auctions".into(), schema: "auction".into() },
            Source { id: "bids".into(), schema: "bid".into() },
        ],
        nodes: declarations
            .into_iter()
            .map(|(id, kind, inputs)| Node {
                id: id.into(),
                kind,
                inputs: inputs.into_iter().map(Into::into).collect(),
                schema: id.into(),
            })
            .collect(),
        arrangements: [
            ("left", "left"),
            ("right", "right"),
            ("sums", "sums"),
            ("output", "output"),
            ("group", "group"),
        ]
        .into_iter()
        .map(|(id, node)| Arrangement { id: id.into(), node: node.into(), schema: node.into() })
        .collect(),
        outputs: vec!["output".into()],
    })
}

#[cfg(test)]
mod tests;
