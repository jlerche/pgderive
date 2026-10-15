use super::{GroupedJoin, Output, Prepared};
use crate::{
    catalog::{Deltas, Progress, Publication, Writer},
    engine::{dataflow::TimedBatch, reader::BatchData},
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
        self.engine.publish_compaction(sql, writer, prepared).await
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
    ) -> Result<TimedBatch<Output<G>>> {
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
