//! Prepared publication programs belong to exactly one physical connection.
use super::{Catalog, Sink, prepared::Prepared};
use anyhow::Result;
use std::ops::Deref;
use tokio_postgres::{Client, Transaction};

/// Catalog SQL connection and its bounded, connection-local publication cache.
/// Reconnecting constructs a new connection; statements never cross sessions.
pub struct Connection {
    client: Client,
    publication: Option<(String, Box<Prepared>)>,
}
impl From<Client> for Connection {
    fn from(client: Client) -> Self {
        Self { client, publication: None }
    }
}
impl Deref for Connection {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}
impl Connection {
    /// Borrow the raw client, discarding statements before it can be replaced.
    pub fn client_mut(&mut self) -> &mut Client {
        self.publication = None;
        &mut self.client
    }
    /// Configure a transaction on this physical connection.
    pub fn build_transaction(&mut self) -> tokio_postgres::TransactionBuilder<'_> {
        self.client.build_transaction()
    }

    /// Begin a transaction without exposing mutable access to the underlying client.
    ///
    /// # Errors
    /// Returns `PostgreSQL` transaction errors.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, tokio_postgres::Error> {
        self.client.transaction().await
    }

    pub(super) async fn prepare_publication(
        &mut self,
        catalog: &Catalog,
        sink: &Sink,
    ) -> Result<&Prepared> {
        let table = format!("{}.{}", catalog.schema, sink.table);
        if !self
            .publication
            .as_ref()
            .is_some_and(|(name, program)| name == &table && &program.sink.sink == sink)
        {
            let program = Box::pin(Prepared::new(&self.client, catalog, sink)).await?;
            self.publication = Some((table, Box::new(program)));
        }
        let (_, program) = self
            .publication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing publication program"))?;
        Ok(program)
    }
    pub(super) async fn publication(
        &mut self,
        catalog: &Catalog,
        sink: &Sink,
    ) -> Result<(Transaction<'_>, &Prepared)> {
        self.prepare_publication(catalog, sink).await?;
        let (_, program) = self
            .publication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing publication program"))?;
        Ok((self.client.transaction().await?, program))
    }
}
