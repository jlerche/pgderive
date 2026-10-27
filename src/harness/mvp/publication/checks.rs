use super::{PublicationFixture, Query};
use crate::catalog::{Durable, Publication};
use anyhow::{Context, Result, ensure};
use tokio_postgres::Client;

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
        self.boundary_checks(sql, graph, publication, &before).await?;
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
    async fn boundary_checks(
        &mut self,
        sql: &mut Client,
        graph: &Query,
        publication: Publication<'_>,
        before: &Durable,
    ) -> Result<()> {
        sql.execute(
            &format!(
                "UPDATE {}.pgderive_queries SET epoch=epoch+1 WHERE query_id='atomic_grouped'",
                self.schema
            ),
            &[],
        )
        .await?;
        ensure!(
            self.grouped.publish(sql, graph.plan(), publication).await.is_err(),
            "stale epoch published"
        );
        sql.execute(
            &format!(
                "UPDATE {}.pgderive_queries SET epoch=epoch-1 WHERE query_id='atomic_grouped'",
                self.schema
            ),
            &[],
        )
        .await?;
        self.unchanged(sql, graph, before).await?;
        let mut unrepresentable = publication.checkpoint.clone();
        unrepresentable.arrangements[0].trace.generation = u64::MAX;
        ensure!(
            self.grouped
                .publish(
                    sql,
                    graph.plan(),
                    Publication { checkpoint: &unrepresentable, ..publication }
                )
                .await
                .is_err(),
            "unrepresentable generation was committed"
        );
        self.unchanged(sql, graph, before).await?;
        let mut unmanaged = publication.checkpoint.clone();
        let root = unmanaged
            .arrangements
            .iter_mut()
            .flat_map(|member| &mut member.trace.objects)
            .next()
            .context("namespace fixture requires an object")?;
        let mut encoded = serde_json::to_value(&*root)?;
        encoded["namespace"] = serde_json::Value::Null;
        let hash = encoded["hash"].as_str().context("missing root hash")?;
        encoded["path"] = serde_json::json!(format!("pgderive/batch/{hash}"));
        *root = serde_json::from_value(encoded)?;
        let failure = self
            .grouped
            .publish(sql, graph.plan(), Publication { checkpoint: &unmanaged, ..publication })
            .await
            .err()
            .context("unmanaged object was published")?;
        ensure!(
            format!("{failure:#}").contains("managed upload namespaces"),
            "unexpected unmanaged publication failure: {failure:#}"
        );
        self.unchanged(sql, graph, before).await?;
        eprintln!("MVP unmanaged publication roots rejected without progress change");
        Ok(())
    }
    async fn boundary(&self, sql: &mut Client, graph: &Query) -> Result<Durable> {
        self.catalog.load_durable(sql, graph.plan()).await?.context("missing publication boundary")
    }
    async fn unchanged(&self, sql: &mut Client, graph: &Query, before: &Durable) -> Result<()> {
        let after = self.boundary(sql, graph).await?;
        ensure!(
            before == &after,
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
        CREATE TRIGGER assert_sync_commit BEFORE INSERT OR UPDATE OR DELETE ON {schema}.pgderive_progress FOR EACH STATEMENT EXECUTE FUNCTION {schema}.assert_sync_commit();
        SET synchronous_commit=off")).await?;
    Ok(())
}
