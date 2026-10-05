use super::program::{self, projected};
use crate::{
    catalog::{Deltas, Protection, Writer},
    compiler::Compiled,
    engine::{
        Batch,
        dataflow::TimedBatch,
        plan::{Checkpoint, Plan, projection, query},
        reader::CacheStats,
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::{Result, bail};
use tokio_postgres::Client;

type GroupedPrepared = query::Prepared<String, Row, Row, Option<String>>;
type GroupedCompaction = query::Compaction<String, Row, Row, Option<String>>;
pub(super) enum Runtime {
    Grouped(Box<program::Query>),
    Projection(Box<projected::Query>),
}
pub(super) enum Input {
    Grouped(TimedBatch<query::Inputs<String, Row, Row>>),
    Projection(TimedBatch<Batch<(), Row>>),
}
pub(super) enum Prepared {
    Grouped(GroupedPrepared),
    Projection(projection::Prepared<projected::Output>),
}
pub(super) enum Compaction {
    Grouped(GroupedCompaction),
    Projection(projection::Compaction<projected::Output>),
}
pub(super) fn inputs(batch: &weighted::Batch, compiled: &Compiled, time: u64) -> Result<Input> {
    if let Some(projection) = compiled.projection() {
        Ok(Input::Projection(projected::inputs(batch, projection, time)?))
    } else {
        Ok(Input::Grouped(program::inputs(batch, compiled.selectors()?, time)?))
    }
}
impl Prepared {
    pub(super) fn deltas(&self) -> Result<Deltas> {
        match self {
            Self::Grouped(work) => Deltas::grouped(&work.output().batch),
            Self::Projection(work) => Deltas::bag(&work.output().batch),
        }
    }
}
impl Runtime {
    pub(super) fn build(
        contract: &Contract,
        compiled: &Compiled,
        settings: query::Settings,
    ) -> Result<Self> {
        if compiled.projection().is_some() {
            Ok(Self::Projection(Box::new(projected::build(contract, compiled, settings)?)))
        } else {
            Ok(Self::Grouped(Box::new(program::build(contract, compiled, settings)?)))
        }
    }
    pub(super) fn plan(&self) -> &Plan {
        match self {
            Self::Grouped(query) => query.plan(),
            Self::Projection(query) => query.plan(),
        }
    }
    pub(super) fn time(&self) -> u64 {
        match self {
            Self::Grouped(query) => query.time(),
            Self::Projection(query) => query.time(),
        }
    }
    pub(super) fn checkpoint(&self) -> Result<Checkpoint> {
        match self {
            Self::Grouped(query) => query.checkpoint(),
            Self::Projection(query) => query.checkpoint(),
        }
    }
    pub(super) fn cache_stats(&self) -> Result<CacheStats> {
        match self {
            Self::Grouped(query) => query.cache_stats(),
            Self::Projection(query) => query.cache_stats(),
        }
    }
    pub(super) fn run_count(&self) -> usize {
        match self {
            Self::Grouped(query) => {
                let state = query.snapshot();
                [
                    state.left.run_count(),
                    state.right.run_count(),
                    state.sums.run_count(),
                    state.output.run_count(),
                ]
                .into_iter()
                .max()
                .unwrap_or(0)
            }
            Self::Projection(query) => query.snapshot().output.run_count(),
        }
    }
    pub(super) async fn restore_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        match self {
            Self::Grouped(query) => query.restore_checkpoint(checkpoint).await,
            Self::Projection(query) => query.restore_checkpoint(checkpoint).await,
        }
    }
    pub(super) async fn prepare_protected(
        &self,
        input: Input,
        protection: &Protection,
    ) -> Result<Prepared> {
        match (self, input) {
            (Self::Grouped(query), Input::Grouped(input)) => {
                Ok(Prepared::Grouped(query.prepare_protected(input, protection).await?))
            }
            (Self::Projection(query), Input::Projection(input)) => {
                Ok(Prepared::Projection(query.prepare_protected(input, protection).await?))
            }
            _ => bail!("runtime input shape mismatch"),
        }
    }
    pub(super) fn prepared_checkpoint(&self, work: &Prepared) -> Result<Checkpoint> {
        match (self, work) {
            (Self::Grouped(query), Prepared::Grouped(work)) => query.prepared_checkpoint(work),
            (Self::Projection(query), Prepared::Projection(work)) => {
                query.prepared_checkpoint(work)
            }
            _ => bail!("runtime candidate shape mismatch"),
        }
    }
    pub(super) fn commit(&mut self, work: Prepared) -> Result<()> {
        match self {
            Self::Grouped(query) => {
                let Prepared::Grouped(work) = work else {
                    bail!("runtime candidate shape mismatch");
                };
                query.commit(work).map(|_| ())
            }
            Self::Projection(query) => {
                let Prepared::Projection(work) = work else {
                    bail!("runtime candidate shape mismatch");
                };
                query.commit(work).map(|_| ())
            }
        }
    }
    pub(super) async fn prepare_compaction_protected(
        &self,
        protection: &Protection,
    ) -> Result<Compaction> {
        match self {
            Self::Grouped(query) => {
                Ok(Compaction::Grouped(query.prepare_compaction_protected(protection).await?))
            }
            Self::Projection(query) => {
                Ok(Compaction::Projection(query.prepare_compaction_protected(protection).await?))
            }
        }
    }
    pub(super) fn prepared_compaction_checkpoint(&self, work: &Compaction) -> Result<Checkpoint> {
        match (self, work) {
            (Self::Grouped(query), Compaction::Grouped(work)) => {
                query.prepared_compaction_checkpoint(work)
            }
            (Self::Projection(query), Compaction::Projection(work)) => {
                query.prepared_compaction_checkpoint(work)
            }
            _ => bail!("runtime compaction shape mismatch"),
        }
    }
    pub(super) async fn publish_compaction(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        work: Compaction,
    ) -> Result<()> {
        match (self, work) {
            (Self::Grouped(query), Compaction::Grouped(work)) => {
                query.publish_compaction(sql, writer, work).await
            }
            (Self::Projection(query), Compaction::Projection(work)) => {
                query.publish_compaction(sql, writer, work).await
            }
            _ => bail!("runtime compaction shape mismatch"),
        }
    }
}
