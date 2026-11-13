mod bags;
mod bulk;
mod cancellation;
mod checks;
mod initial;
mod recovery;
mod representations;
use super::{MvpFixture, Query};
use crate::{
    catalog::{Binding, Catalog, Progress, Protection, Publication, Snapshot, Writer},
    engine::plan::{Plan, relational::Prepared},
    transaction::Transaction,
};
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) struct Staged {
    pub(super) prepared: Prepared<super::Key, crate::transaction::Row>,
    pub(super) upload: Protection,
}
pub(super) struct PublicationFixture {
    pub(super) grouped: Writer,
    pub(super) bag: Writer,
    stale: Writer,
    catalog: Catalog,
    schema: String,
    config: crate::Config,
}
impl MvpFixture {
    pub(in crate::harness) async fn initialize_publication(
        &mut self,
        sql: &mut crate::catalog::Connection,
        schema: &str,
        config: &crate::Config,
        initial: &str,
    ) -> Result<()> {
        let source = initial::source(sql, config).await?;
        ensure!(
            super::super::source_state(&**sql, schema).await?.is_empty(),
            "MVP initial snapshot requires empty source tables"
        );
        let empty = self.graph.checkpoint()?;
        let catalog = Catalog::new(schema, "atomic_grouped")?;
        catalog.install(sql).await?;
        let setup = initial::Initial { plan: self.graph.plan(), empty: &empty, source: &source };
        bags::check(sql, schema, &setup).await?;
        representations::check(sql, schema, &setup).await?;
        bulk::check(sql, schema, &setup).await?;
        let bag_catalog = Catalog::new(schema, "atomic_bag")?;
        let (bag, bag_checkpoint) = setup
            .synthetic(sql, &bag_catalog, crate::harness::tuple_sink::sink("derived_bag"))
            .await?;
        self.bag_graph.restore_checkpoint(bag_checkpoint).await?;
        let binding = Binding { source, sink: crate::harness::tuple_sink::sink("derived_groups") };
        self.initial_snapshot(sql, &catalog, &binding, initial).await?;
        let stale = catalog.claim(sql, self.graph.plan(), binding.clone()).await?;
        let grouped = catalog.claim(sql, self.graph.plan(), binding).await?;
        checks::require_synchronous_commit(sql, schema).await?;
        self.publication = Some(PublicationFixture {
            grouped,
            bag,
            stale,
            catalog,
            schema: schema.into(),
            config: config.clone(),
        });
        Ok(())
    }
    async fn initial_snapshot(
        &mut self,
        sql: &mut crate::catalog::Connection,
        catalog: &Catalog,
        binding: &Binding,
        initial: &str,
    ) -> Result<()> {
        let mut checkpoint = self.graph.checkpoint()?;
        bags::advance(&mut checkpoint);
        let deltas = crate::harness::tuple_sink::deltas(&super::Batch::<
            super::Key,
            crate::transaction::Row,
        >::default())?;
        catalog
            .activate_snapshot(
                sql,
                self.graph.plan(),
                Snapshot {
                    checkpoint: &checkpoint,
                    deltas: &deltas,
                    source: &binding.source,
                    boundary: initial.parse()?,
                    sink: binding.sink.clone(),
                },
            )
            .await?;
        self.graph.restore_checkpoint(checkpoint).await
    }
}
impl PublicationFixture {
    pub(super) async fn retry_upload(
        &mut self,
        sql: &mut crate::catalog::Connection,
        plan: &Plan,
        token: &str,
    ) -> Result<Protection> {
        let binding = self.grouped.confirmed(sql, plan).await?.binding;
        self.grouped = self.catalog.claim(sql, plan, binding).await?;
        self.grouped.seal_fenced_uploads(sql, plan).await?;
        self.grouped.protect_upload(sql, token).await
    }
    pub(super) async fn publish(
        &mut self,
        sql: &mut crate::catalog::Connection,
        graphs: (&mut Query, &mut Query),
        staged: (Staged, Staged),
        transaction: &Transaction,
    ) -> Result<()> {
        let (graph, bag_graph) = graphs;
        let (main, bag) = staged;
        let checkpoint = graph.prepared_checkpoint(&main.prepared)?;
        let bag_checkpoint = bag_graph.prepared_checkpoint(&bag.prepared)?;
        ensure!(
            main.prepared.output().batch == bag.prepared.output().batch,
            "independent publication graphs disagree"
        );
        let progress =
            Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        let deltas = crate::harness::tuple_sink::deltas(&main.prepared.output().batch)?;
        let publication =
            || Publication { checkpoint: &checkpoint, progress: &progress, deltas: &deltas };
        if checkpoint.time == 2 {
            self.failure_checks(sql, graph, publication()).await?;
        }
        if !self.uncertain_commit(sql, graph, publication()).await? {
            self.grouped.publish(sql, graph.plan(), publication()).await?;
            graph.commit(main.prepared)?;
        }

        self.bag
            .publish(
                sql,
                graph.plan(),
                Publication { checkpoint: &bag_checkpoint, progress: &progress, deltas: &deltas },
            )
            .await?;
        bag_graph.commit(bag.prepared)?;
        main.upload.published(sql, &self.grouped, graph.plan(), &checkpoint).await?;
        bag.upload.published(sql, &self.bag, bag_graph.plan(), &bag_checkpoint).await?;
        ensure!(
            self.grouped.end() == progress.end && self.bag.end() == progress.end,
            "publisher returned wrong durable source position"
        );
        let stored = self
            .catalog
            .load_durable(sql, graph.plan())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing durable publication"))?;
        ensure!(
            stored.stored.checkpoint == checkpoint && stored.stored.epoch == self.grouped.epoch(),
            "published membership/epoch differs from candidate"
        );
        let matches = sql.query_one(&format!("SELECT COUNT(*) FROM {}.pgderive_progress p JOIN {}.pgderive_queries q USING(query_id) WHERE p.query_id IN ('atomic_grouped','atomic_bag') AND p.end_lsn=$1::text::pg_lsn AND p.commit_lsn=$2::text::pg_lsn AND p.xid=$3 AND q.logical_time=$4", self.schema, self.schema), &[&progress.end.to_string(), &progress.commit.to_string(), &i64::from(progress.xid), &i64::try_from(checkpoint.time)?]).await?.try_get::<_, i64>(0)?;
        ensure!(matches == 2, "sink publication did not persist exact source transaction progress");
        self.verify(sql).await?;
        eprintln!(
            "MVP atomic sink/membership/source publication passed at tick {}",
            checkpoint.time
        );
        Ok(())
    }
    async fn verify(&self, sql: &Client) -> Result<()> {
        let expected = format!(
            "SELECT jsonb_build_array('',jsonb_build_object('category',a.category::text,'count',COUNT(*)::text,'sum',(SUM(b.price)::bigint)::text)) AS tuple,1::bigint AS weight FROM {0}.auction a JOIN {0}.bid b ON a.id=b.auction GROUP BY a.category",
            self.schema
        );
        for table in ["derived_groups", "derived_bag"] {
            let different = sql.query_one(&format!("SELECT EXISTS((SELECT tuple,count(*)::bigint weight FROM {0}.{table} GROUP BY tuple EXCEPT {expected}) UNION ALL ({expected} EXCEPT SELECT tuple,count(*)::bigint weight FROM {0}.{table} GROUP BY tuple))",self.schema),&[]).await?.try_get::<_,bool>(0)?;
            ensure!(!different, "durable bag destination differs from SQL recomputation");
        }
        Ok(())
    }
}
