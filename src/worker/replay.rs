//! Reconstruct the volatile suffix without republishing its already durable output.
use super::{open::Open, program::relational};
use crate::{
    acknowledged::Stream,
    catalog::{Durable, Lsn, Progress},
};
use anyhow::{Context, Result, ensure};

pub(super) async fn monitored(
    open: &mut Open,
    stream: &mut Stream,
    durable: &Durable,
    stop: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool> {
    let limit = std::time::Duration::from_secs(open.settings.startup_timeout_secs);
    let result = tokio::select! {
        result = tokio::time::timeout(limit, recover(open, stream, durable)) => result.context("worker WAL replay timed out before readiness").and_then(std::convert::identity).map(|()| true),
        signal = stop.changed() => signal.context("worker stop monitor closed").map(|()| false),
    };
    if matches!(result, Ok(true)) {
        return result;
    }
    let shutdown = tokio::time::timeout(std::time::Duration::from_secs(10), stream.shutdown())
        .await
        .context("replay shutdown timed out")
        .and_then(std::convert::identity);
    crate::outcome::combine(result, shutdown, "replay replication shutdown")
}
async fn recover(open: &mut Open, stream: &mut Stream, durable: &Durable) -> Result<()> {
    let mut replay = Replay::new(durable.clone());
    let epochs = open.catalog.replay_epochs(&open.session.client, durable).await?;
    if let Some(epochs) = epochs {
        for epoch in epochs {
            recover_epoch(open, stream, &mut replay, &epoch).await?;
        }
    }
    while !replay.complete() {
        let transaction = stream.recv().await?;
        let progress =
            Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        if !replay.accept(&progress)? {
            continue;
        }
        let time = open
            .query
            .time()
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("replay time overflow"))?;
        let input = relational::inputs(&transaction.batch, open.compiled.relational(), time)?;
        let work = open.query.prepare_memtable(input, open.settings.memtable_bytes).await?;
        open.query.commit(work)?;
    }
    ensure!(
        open.query.time() == durable.published_time,
        "replay logical clock disagrees with published progress"
    );
    open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    super::drive::emit(
        &serde_json::json!({"event":"replayed","time":open.query.time(),"checkpoint_lsn":durable.checkpoint_end.to_string(),"published_lsn":durable.end.to_string()}),
    )
}
async fn recover_epoch(
    open: &mut Open,
    stream: &mut Stream,
    replay: &mut Replay,
    boundary: &crate::catalog::epoch::ReplayEpoch,
) -> Result<()> {
    let mut epoch: Option<super::epoch::Epoch> = None;
    let mut transactions = 0_u64;
    while replay.applied < boundary.progress.end {
        let transaction = stream.recv().await?;
        let progress =
            Progress::new(transaction.xid, &transaction.commit_lsn, &transaction.end_lsn)?;
        ensure!(progress.end <= boundary.progress.end, "WAL replay skipped epoch boundary");
        if !replay.accept(&progress)? {
            continue;
        }
        transactions =
            transactions.checked_add(1).context("epoch replay transaction count overflow")?;
        ensure!(
            transactions <= boundary.transactions,
            "epoch replay has excess source transactions"
        );
        if let Some(epoch) = &mut epoch {
            epoch.push(transaction, stream.decode_us(), open.replication.execution)?;
        } else {
            epoch = Some(super::epoch::Epoch::new(transaction, stream.decode_us())?);
        }
    }
    ensure!(transactions == boundary.transactions, "epoch replay transaction count mismatch");
    ensure!(
        replay.last.as_ref() == Some(&boundary.progress),
        "epoch replay final transaction mismatch"
    );
    let epoch = epoch.context("empty WAL replay epoch")?;
    ensure!(
        open.query.time().checked_add(1) == Some(boundary.time),
        "epoch replay logical time mismatch"
    );
    let input =
        relational::inputs(&epoch.transaction().batch, open.compiled.relational(), boundary.time)?;
    let work = open.query.prepare_memtable(input, open.settings.memtable_bytes).await?;
    open.query.commit(work).map(|_| ())
}
struct Replay {
    durable: Durable,
    applied: Lsn,
    last: Option<Progress>,
}
impl Replay {
    const fn new(durable: Durable) -> Self {
        Self { applied: durable.checkpoint_end, durable, last: None }
    }
    fn complete(&self) -> bool {
        self.applied == self.durable.end
    }
    fn accept(&mut self, progress: &Progress) -> Result<bool> {
        ensure!(progress.end >= progress.commit, "invalid replay positions");
        if progress.end < self.applied {
            return Ok(false);
        }
        if progress.end == self.applied {
            if let Some(last) = &self.last {
                ensure!(
                    last.commit == progress.commit && last.xid == progress.xid,
                    "conflicting replay transaction identity"
                );
            }
            return Ok(false);
        }
        ensure!(progress.commit >= self.applied, "replay overlaps applied transaction");
        ensure!(progress.end <= self.durable.end, "replay skipped published boundary");
        self.durable.covers(progress)?;
        self.applied = progress.end;
        self.last = Some(progress.clone());
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{Binding, Stored},
        engine::plan::Checkpoint,
    };
    fn boundary() -> Result<Durable> {
        Ok(Durable {
            stored: Stored {
                epoch: 1,
                checkpoint: Checkpoint {
                    version: 1,
                    plan_identity: "replay-test".into(),
                    time: 1,
                    arrangements: vec![],
                },
            },
            binding: Binding {
                source: crate::source::tests::contract(),
                sink: crate::harness::tuple_sink::sink("sink"),
            },
            fence: 1,
            commit: "0/50".parse()?,
            end: "0/60".parse()?,
            checkpoint_end: "0/10".parse()?,
            published_time: 3,
            xid: Some(2),
        })
    }
    #[test]
    fn replay_deduplicates_transactions_and_rejects_conflicts_holes_and_overlap() -> Result<()> {
        let first = Progress::new(1, "0/20", "0/30")?;
        let last = Progress::new(2, "0/50", "0/60")?;
        let mut replay = Replay::new(boundary()?);
        assert!(!replay.accept(&Progress::new(99, "0/1", "0/5")?)?);
        assert!(replay.accept(&first)?);
        assert!(!replay.accept(&first)?);
        assert!(replay.accept(&Progress::new(9, "0/20", "0/30")?).is_err());
        assert!(replay.accept(&Progress::new(2, "0/25", "0/40")?).is_err());
        assert!(replay.accept(&Progress::new(2, "0/60", "0/70")?).is_err());
        assert!(replay.accept(&Progress::new(3, "0/50", "0/60")?).is_err());
        assert!(replay.accept(&last)?);
        assert!(replay.complete());
        assert!(!replay.accept(&first)?);
        assert!(!replay.accept(&last)?);
        Ok(())
    }
}
