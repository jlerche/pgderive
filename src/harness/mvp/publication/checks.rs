use super::{PublicationFixture, Query};
use crate::catalog::{Publication, Stored};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio_postgres::Client;

struct Boundary {
    stored: Stored,
    progress: Value,
}
impl PublicationFixture {
    pub(super) async fn failure_checks(
        &mut self,
        sql: &mut Client,
        graph: &Query,
        publication: Publication<'_>,
    ) -> Result<()> {
        let before = self.boundary(sql, graph).await?;
        ensure!(
            self.stale.publish(sql, graph.plan(), publication).await.is_err(),
            "replaced worker published"
        );
        self.unchanged(sql, graph, &before).await?;
        ensure!(
            self.catalog
                .checkpoint(sql, graph.plan(), publication.checkpoint, self.grouped.epoch())
                .await
                .is_err(),
            "metadata API overwrote publication query"
        );
        self.unchanged(sql, graph, &before).await?;
        let schema = &self.schema;
        sql.batch_execute(&format!("CREATE FUNCTION {schema}.reject_sink_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected destination failure'; END $$;
            CREATE TRIGGER reject_sink_write BEFORE INSERT OR UPDATE OR DELETE ON {schema}.derived_groups FOR EACH ROW EXECUTE FUNCTION {schema}.reject_sink_write()" )).await?;
        let failure = self.grouped.publish(sql, graph.plan(), publication).await;
        ensure!(failure.is_err(), "injected destination failure was not observed");
        eprintln!(
            "expected atomic publication failure: {}",
            failure.err().context("missing publication failure")?
        );
        self.unchanged(sql, graph, &before).await?;
        ensure!(
            sql.query_one(&format!("SELECT COUNT(*) FROM {}.derived_groups", self.schema), &[])
                .await?
                .try_get::<_, i64>(0)?
                == 0,
            "failed publication changed destination"
        );
        sql.batch_execute(&format!("CREATE OR REPLACE FUNCTION {}.reject_sink_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$", self.schema)).await?;
        ensure!(
            self.grouped.publish(sql, graph.plan(), publication).await.is_err(),
            "suppressed destination write was committed"
        );
        self.unchanged(sql, graph, &before).await?;
        eprintln!("MVP suppressed destination DML rejected without progress change");
        sql.batch_execute(&format!("DROP TRIGGER reject_sink_write ON {0}.derived_groups; DROP FUNCTION {0}.reject_sink_write()", self.schema)).await?;
        eprintln!(
            "MVP failed destination DML and fenced writers preserved sink/membership/progress"
        );
        Ok(())
    }
    async fn boundary(&self, sql: &mut Client, graph: &Query) -> Result<Boundary> {
        let stored =
            self.catalog.load(sql, graph.plan()).await?.context("missing publication boundary")?;
        let progress = sql.query_one(&format!("SELECT to_jsonb(p) FROM {}.pgderive_progress p WHERE query_id='atomic_grouped'", self.schema), &[]).await?.try_get(0)?;
        Ok(Boundary { stored, progress })
    }
    async fn unchanged(&self, sql: &mut Client, graph: &Query, before: &Boundary) -> Result<()> {
        let after = self.boundary(sql, graph).await?;
        ensure!(
            before.stored.epoch == after.stored.epoch
                && before.stored.checkpoint == after.stored.checkpoint
                && before.progress == after.progress,
            "failed publication changed membership/epoch/fence/source progress"
        );
        ensure!(
            self.grouped.epoch() == before.stored.epoch,
            "failed publication moved writer epoch"
        );
        Ok(())
    }
}

pub(super) async fn require_synchronous_commit(sql: &Client, schema: &str) -> Result<()> {
    sql.batch_execute(&format!("CREATE FUNCTION {schema}.assert_sync_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') <> 'on' THEN RAISE EXCEPTION 'publication did not enable synchronous commit'; END IF; RETURN NULL; END $$;
        CREATE TRIGGER assert_sync_commit BEFORE INSERT OR UPDATE OR DELETE ON {schema}.derived_groups FOR EACH STATEMENT EXECUTE FUNCTION {schema}.assert_sync_commit();
        SET synchronous_commit=off")).await?;
    Ok(())
}
