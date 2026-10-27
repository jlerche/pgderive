//! Immutable sink SQL/codec metadata compiled once per connection and binding.
use super::{Sink, SinkOutput, batching, key, keyed, layout::Layout};
use anyhow::Result;
use tokio_postgres::{GenericClient, Statement, types::Type};

pub struct Command {
    pub(super) sql: String,
    pub(super) prepared: Option<Statement>,
}
pub struct Program {
    pub sink: Sink,
    pub(super) insert: Command,
    pub(super) delete: Command,
    pub(super) update: Option<Command>,
    pub(super) positions: Vec<usize>,
}
impl Program {
    pub async fn prepare(
        client: &(impl GenericClient + Sync),
        table: &str,
        sink: &Sink,
    ) -> Result<Self> {
        let layout = Layout::new(sink)?;
        let positions = key::positions(sink)?;
        let deletion = if positions.is_empty() {
            batching::statement(table, &layout, false)
        } else {
            keyed::delete_statement(table, sink, &layout)?
        };
        let insert =
            Command::prepare(client, batching::statement(table, &layout, true), sink.output)
                .await?;
        let delete = Command::prepare(client, deletion, sink.output).await?;
        let update = if positions.is_empty() {
            None
        } else {
            Some(
                Command::prepare(client, keyed::update_statement(table, sink)?, sink.output)
                    .await?,
            )
        };
        Ok(Self { sink: sink.clone(), insert, delete, update, positions })
    }
}
impl Command {
    async fn prepare(
        client: &(impl GenericClient + Sync),
        sql: String,
        output: SinkOutput,
    ) -> Result<Self> {
        let prepared = if output == SinkOutput::Postgres {
            Some(client.prepare_typed(&sql, &[Type::JSONB]).await?)
        } else {
            None
        };
        Ok(Self { sql, prepared })
    }
}
