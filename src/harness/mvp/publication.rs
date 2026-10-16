mod bags;
mod bindings;
mod cancellation;
mod checks;
mod recovery;
mod representations;
use super::{MvpFixture, Query};
use crate::{
    catalog::{Binding, Catalog, Deltas, Lsn, Progress, Publication, Sink, Writer},
    engine::plan::relational::Prepared,
    transaction::Transaction,
};
use anyhow::{Result, ensure};
use tokio_postgres::Client;

pub(super) struct PublicationFixture {
    grouped: Writer,
    bag: Writer,
    stale: Writer,
    catalog: Catalog,
    schema: String,
    config: crate::Config,
}
impl MvpFixture {
    pub(in crate::harness) async fn initialize_publication(
        &mut self,
        sql: &mut Client,
        schema: &str,
        config: &crate::Config,
        initial: &str,
    ) -> Result<()> {
        let source = format!(
            "{}:{}:{}",
            config.postgres.database, config.replication.publication, config.replication.slot
        );
        let checkpoint = self.graph.checkpoint()?;
        let catalog = Catalog::new(schema, "atomic_grouped")?;
        catalog.install(sql).await?;
        bags::check(sql, schema, self.graph.plan(), &checkpoint).await?;
        representations::check(sql, schema, self.graph.plan(), &checkpoint).await?;
        bindings::check(sql, schema, self.graph.plan(), &checkpoint, config).await?;
        catalog.checkpoint(sql, self.graph.plan(), &checkpoint, 0).await?;
        let binding = Binding { source: source.clone(), sink: Sink::Json("derived_groups".into()) };
        let initial: Lsn = initial.parse()?;
        let stale = catalog.claim(sql, self.graph.plan(), binding.clone(), initial).await?;
        let grouped = catalog.claim(sql, self.graph.plan(), binding, initial).await?;
        checks::require_synchronous_commit(sql, schema).await?;

        let bag_catalog = Catalog::new(schema, "atomic_bag")?;
        bag_catalog.checkpoint(sql, self.graph.plan(), &checkpoint, 0).await?;
        let bag = bag_catalog
            .claim(
                sql,
                self.graph.plan(),
                Binding { source, sink: Sink::Json("derived_bag".into()) },
                initial,
            )
            .await?;
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
}
impl PublicationFixture {
    pub(super) async fn publish(
        &mut self,
        sql: &mut Client,
        graph: &mut Query,
        prepared: Prepared<super::Key, crate::transaction::Row>,
        transaction: &Transaction,
    ) -> Result<()> {
        let checkpoint = graph.prepared_checkpoint(&prepared)?;
        let progress =
            Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        let deltas = Deltas::bag(&prepared.output().batch)?;
        let publication =
            || Publication { checkpoint: &checkpoint, progress: &progress, deltas: &deltas };
        if checkpoint.time == 1 {
            self.failure_checks(sql, graph, publication()).await?;
        }
        if !self.uncertain_commit(sql, graph, publication()).await? {
            self.grouped.publish(sql, graph.plan(), publication()).await?;
            graph.commit(prepared)?;
        }

        self.bag
            .publish(
                sql,
                graph.plan(),
                Publication { checkpoint: &checkpoint, progress: &progress, deltas: &deltas },
            )
            .await?;
        ensure!(
            self.grouped.end() == progress.end && self.bag.end() == progress.end,
            "publisher returned wrong durable source position"
        );
        let stored = self
            .catalog
            .load(sql, graph.plan())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing durable publication"))?;
        ensure!(
            stored.checkpoint == checkpoint && stored.epoch == self.grouped.epoch(),
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
