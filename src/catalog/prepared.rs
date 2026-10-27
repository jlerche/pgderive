//! Hot publication statements compiled together with the connection-bound sink.
use super::{Catalog, Sink, sink::program::Program};
use anyhow::Result;
use tokio_postgres::{Client, Statement, types::Type};

pub(super) struct Prepared {
    pub sink: Program,
    pub guard: Statement,
    pub epoch: Statement,
}
impl Prepared {
    pub async fn new(client: &Client, catalog: &Catalog, sink: &Sink) -> Result<Self> {
        let table = format!("{}.{}", catalog.schema, sink.table);
        let sink = Program::prepare(client, &table, sink).await?;
        let guard = client
            .prepare_typed(
                &super::memtable_guard::statement(&catalog.schema),
                &[Type::TEXT, Type::JSONB, Type::JSONB, Type::TEXT],
            )
            .await?;
        let epoch = client
            .prepare_typed(
                &super::epoch::statement(&catalog.schema),
                &[
                    Type::TEXT,
                    Type::INT8,
                    Type::TEXT,
                    Type::TEXT,
                    Type::TEXT,
                    Type::INT8,
                    Type::INT8,
                ],
            )
            .await?;
        Ok(Self { sink, guard, epoch })
    }
}
