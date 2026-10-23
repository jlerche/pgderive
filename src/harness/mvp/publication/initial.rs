//! Synthetic sink arithmetic uses real native descriptors but never opens or ACKs a slot.
use crate::{
    catalog::{Binding, Catalog, Deltas, Sink, Snapshot, Writer},
    engine::{
        Batch,
        plan::{Checkpoint, Plan},
    },
    source::Contract,
};
use anyhow::Result;
use tokio_postgres::Client;

pub(super) struct Initial<'a> {
    pub plan: &'a Plan,
    pub empty: &'a Checkpoint,
    pub source: &'a Contract,
}
impl Initial<'_> {
    pub(super) fn binding(&self, sink: Sink) -> Binding {
        let mut source = self.source.clone();
        source.slot =
            format!("{}_{}", &source.slot[..source.slot.len().min(25)], sink.table.as_str());
        Binding { source, sink }
    }
    pub(super) async fn synthetic(
        &self,
        sql: &mut Client,
        catalog: &Catalog,
        sink: Sink,
    ) -> Result<(Writer, Checkpoint)> {
        let binding = self.binding(sink);
        let mut checkpoint = self.empty.clone();
        super::bags::advance(&mut checkpoint);
        let deltas = Deltas::bag(&Batch::<(), ()>::default())?;
        catalog
            .activate_snapshot(
                sql,
                self.plan,
                Snapshot {
                    checkpoint: &checkpoint,
                    deltas: &deltas,
                    source: &binding.source,
                    boundary: "0/0".parse()?,
                    sink: binding.sink.clone(),
                },
            )
            .await?;
        let writer = catalog.claim(sql, self.plan, binding).await?;
        Ok((writer, checkpoint))
    }
}

pub(super) async fn source(sql: &Client, config: &crate::Config) -> Result<Contract> {
    let identity = crate::source::Identity::inspect(config).await?;
    Contract::inspect(sql, identity, &config.replication.publication, &config.replication.slot)
        .await
}
