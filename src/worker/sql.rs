use crate::Config;
use anyhow::{Context, Result, ensure};
use std::ops::{Deref, DerefMut};
use tokio::time::{Duration, timeout};

pub(super) struct Session(crate::postgres::Session);
impl Deref for Session {
    type Target = crate::postgres::Session;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Session {
    pub(super) async fn connect(config: &Config) -> Result<Self> {
        Ok(Self(crate::postgres::Session::connect(&config.postgres).await?))
    }
    pub(super) async fn own(&self, schema: &str, query: &str, tenant: &str) -> Result<()> {
        ensure!(self.try_own(schema, query, tenant).await?, "another worker owns this query");
        Ok(())
    }
    pub(super) async fn try_own(&self, schema: &str, query: &str, tenant: &str) -> Result<bool> {
        crate::semantics::lease(&*self.client).await?;
        super::tenant::claim(&*self.client, schema, tenant).await?;
        let scope = format!("pgderive-worker:{schema}:{query}");
        let row = timeout(
            Duration::from_secs(10),
            self.client.query_one(
                "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended($1,0))",
                &[&scope],
            ),
        )
        .await
        .context("worker ownership acquisition timed out")??;
        Ok(row.try_get(0)?)
    }
}
