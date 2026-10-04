use super::{GroupedJoin, Output, Prepared};
use crate::{
    catalog::{Deltas, Progress, Publication, Writer},
    engine::{dataflow::Stream, reader::BatchData},
};
use anyhow::Result;
use tokio_postgres::Client;

impl<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> GroupedJoin<K, A, B, L, R, G, V>
{
    /// Publish equivalent compaction memberships, then move local visibility.
    /// Logical time, source positions and destination data remain unchanged.
    ///
    /// # Errors
    /// Rejects stale/foreign work or a local base different from authoritative state.
    /// After uncertain COMMIT, resolve maintenance and cold-restore before retrying.
    pub async fn publish_compaction(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        prepared: super::Compaction<K, L, R, G>,
    ) -> Result<()> {
        self.engine.validate_maintenance(&prepared)?;
        let durable = writer.confirmed(sql, self.plan()).await?;
        anyhow::ensure!(
            durable.stored.checkpoint == self.checkpoint()?,
            "compaction base differs from durable membership"
        );
        let checkpoint = self.prepared_compaction_checkpoint(&prepared)?;
        writer.maintain(sql, self.plan(), &checkpoint).await?;
        self.engine.commit_maintenance(prepared)
    }
    /// Durably publish one prepared grouped transaction, then move local visibility.
    ///
    /// Exclusive runtime ownership spans candidate validation and `PostgreSQL` COMMIT.
    /// Immutable objects were uploaded by preparation. Returned output follows a
    /// confirmed atomic membership/sink/progress commit; acknowledgement follows later.
    ///
    /// # Errors
    /// Rejects foreign/stale work and propagates encoding, fencing and database errors.
    /// On an uncertain COMMIT, recover the authoritative boundary before retrying.
    pub async fn publish_prepared(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        prepared: Prepared<K, L, R, G>,
        progress: &Progress,
    ) -> Result<Stream<Output<G>>> {
        let checkpoint = self.prepared_checkpoint(&prepared)?;
        let deltas = Deltas::grouped(&prepared.output().batch)?;
        writer
            .publish(
                sql,
                self.plan(),
                Publication { checkpoint: &checkpoint, progress, deltas: &deltas },
            )
            .await?;
        self.commit(prepared)
    }
}
