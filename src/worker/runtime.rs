use super::program::relational;
use crate::{
    catalog::Deltas,
    compiler::{Compiled, Projected},
    engine::{
        Batch,
        dataflow::TimedBatch,
        plan::{self, Checkpoint, Plan, Settings},
        reader::CacheStats,
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::Result;
pub(super) type Input = TimedBatch<Vec<Batch<relational::Key, Row>>>;
pub(super) type Compaction = plan::relational::Compaction<relational::Key, Row>;
pub(super) struct Prepared {
    pub(super) work: plan::relational::Prepared<relational::Key, Row>,
    output: Projected,
}
impl Prepared {
    pub(super) fn deltas(&self) -> Result<Deltas> {
        relational::deltas(&self.work.output().batch, &self.output)
    }
}
pub(super) struct Runtime {
    query: relational::Query,
    output: Projected,
}
pub(super) fn inputs(batch: &weighted::Batch, compiled: &Compiled, time: u64) -> Result<Input> {
    relational::inputs(batch, compiled.relational(), time)
}
impl Runtime {
    pub(super) fn build(
        contract: &Contract,
        compiled: &Compiled,
        settings: &Settings,
    ) -> Result<Self> {
        Ok(Self {
            query: relational::build(contract, compiled, settings)?,
            output: compiled.output().clone(),
        })
    }
    pub(super) fn plan(&self) -> &Plan {
        self.query.plan()
    }
    pub(super) fn time(&self) -> u64 {
        self.query.time()
    }
    pub(super) fn checkpoint(&self) -> Result<Checkpoint> {
        self.query.checkpoint()
    }
    pub(super) fn cache_stats(&self) -> Result<CacheStats> {
        self.query.cache_stats()
    }
    pub(super) fn run_count(&self) -> usize {
        self.query.run_count()
    }
    pub(super) async fn restore_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        self.query.restore_checkpoint(checkpoint).await
    }
    pub(super) async fn prepare_protected(
        &self,
        input: Input,
        protection: &crate::catalog::Protection,
    ) -> Result<Prepared> {
        Ok(Prepared {
            work: self.query.prepare_protected(input, protection).await?,
            output: self.output.clone(),
        })
    }
    pub(super) fn prepared_checkpoint(&self, work: &Prepared) -> Result<Checkpoint> {
        self.query.prepared_checkpoint(&work.work)
    }
    pub(super) fn commit(&mut self, work: Prepared) -> Result<()> {
        self.query.commit(work.work).map(|_| ())
    }
    pub(super) async fn prepare_compaction_protected(
        &self,
        protection: &crate::catalog::Protection,
    ) -> Result<Compaction> {
        self.query.prepare_compaction_protected(protection).await
    }
    pub(super) fn prepared_compaction_checkpoint(&self, work: &Compaction) -> Result<Checkpoint> {
        self.query.prepared_compaction_checkpoint(work)
    }
    pub(super) async fn publish_compaction(
        &mut self,
        sql: &mut tokio_postgres::Client,
        writer: &mut crate::catalog::Writer,
        work: Compaction,
    ) -> Result<()> {
        self.query.publish_compaction(sql, writer, work).await
    }
}
