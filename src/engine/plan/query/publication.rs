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
