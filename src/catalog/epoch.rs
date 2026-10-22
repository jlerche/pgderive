//! Coarse WAL boundaries needed to reconstruct adaptive execution ticks exactly.
use super::{Catalog, Durable, Lsn, Progress, Writer};
use crate::engine::plan::Plan;
use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, Transaction};

#[derive(Clone)]
pub struct ReplayEpoch {
    pub time: u64,
    pub previous: Lsn,
    pub progress: Progress,
    pub transactions: u64,
}
impl Writer {
    pub(crate) async fn epochs_enabled(&self, sql: &Client) -> Result<bool> {
        self.catalog.epochs_enabled(sql).await
    }
    /// Upgrade only a fully checkpointed boundary; old readers reject format three.
    pub(crate) async fn activate_epochs(&mut self, sql: &mut Client, plan: &Plan) -> Result<()> {
        ensure!(!self.uncertain, "uncertain epoch activation requires recovery");
        ensure!(
            self.boundary.checkpoint_end == self.boundary.end,
            "epoch activation requires checkpointed progress"
        );
        let tx = sql.transaction().await?;
        super::storage::publication_lock(&tx, &self.catalog).await?;
        self.guard_memtable(&tx, plan).await?;
        tx.execute(
            &format!(
                "UPDATE {}.pgderive_queries SET format_version=3 WHERE query_id=$1",
                self.catalog.schema
            ),
            &[&self.catalog.query],
        )
        .await?;
        self.commit(tx, "epoch activation").await
    }
    pub(super) async fn journal_epoch(
        &self,
        tx: &Transaction<'_>,
        publication: (u64, &Progress, u64),
    ) -> Result<()> {
        let (time, progress, transactions) = publication;
        ensure!(transactions > 0, "empty source epoch");
        let inserted = tx.execute(&format!("INSERT INTO {0}.pgderive_epoch_boundaries(query_id,logical_time,previous_end_lsn,commit_lsn,end_lsn,xid,transactions) SELECT $1,$2,$3::text::pg_lsn,$4::text::pg_lsn,$5::text::pg_lsn,$6,$7 FROM {0}.pgderive_queries WHERE query_id=$1 AND format_version=3",self.catalog.schema), &[&self.catalog.query,&i64::try_from(time)?,&self.boundary.end.to_string(),&progress.commit.to_string(),&progress.end.to_string(),&i64::from(progress.xid),&i64::try_from(transactions)?]).await?;
        ensure!(inserted == 1, "adaptive publication requires epoch journal activation");
        Ok(())
    }
}
impl Catalog {
    async fn epochs_enabled(&self, sql: &Client) -> Result<bool> {
        let version: i32 = sql
            .query_one(
                &format!(
                    "SELECT format_version FROM {}.pgderive_queries WHERE query_id=$1",
                    self.schema
                ),
                &[&self.query],
            )
            .await?
            .try_get(0)?;
        ensure!(matches!(version, 1..=3), "unsupported catalog query format");
        Ok(version == 3)
    }
    pub(crate) async fn replay_epochs(
        &self,
        sql: &Client,
        durable: &Durable,
    ) -> Result<Option<Vec<ReplayEpoch>>> {
        if !self.epochs_enabled(sql).await? {
            return Ok(None);
        }
        let count = durable
            .published_time
            .checked_sub(durable.stored.checkpoint.time)
            .context("epoch journal clock precedes checkpoint")?;
        let limit = i64::try_from(count.checked_add(1).context("epoch journal count overflow")?)?;
        let rows = sql.query(&format!("SELECT logical_time,previous_end_lsn::text,commit_lsn::text,end_lsn::text,xid,transactions FROM {}.pgderive_epoch_boundaries WHERE query_id=$1 ORDER BY logical_time LIMIT $2",self.schema), &[&self.query, &limit]).await?;
        let epochs = rows
            .into_iter()
            .map(|row| {
                Ok(ReplayEpoch {
                    time: u64::try_from(row.try_get::<_, i64>(0)?)?,
                    previous: row.try_get::<_, String>(1)?.parse()?,
                    progress: Progress::new(
                        u32::try_from(row.try_get::<_, i64>(4)?)?,
                        &row.try_get::<_, String>(2)?,
                        &row.try_get::<_, String>(3)?,
                    )?,
                    transactions: u64::try_from(row.try_get::<_, i64>(5)?)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        validate_epochs(&epochs, durable)?;
        Ok(Some(epochs))
    }
}
fn validate_epochs(epochs: &[ReplayEpoch], durable: &Durable) -> Result<()> {
    let mut time = durable.stored.checkpoint.time;
    let mut end = durable.checkpoint_end;
    for epoch in epochs {
        time = time.checked_add(1).context("epoch journal clock overflow")?;
        ensure!(
            epoch.time == time && epoch.previous == end,
            "epoch journal has missing or overlapping boundary"
        );
        ensure!(
            epoch.transactions > 0 && epoch.progress.commit >= end && epoch.progress.end > end,
            "invalid epoch journal source boundary"
        );
        end = epoch.progress.end;
    }
    ensure!(
        time == durable.published_time && end == durable.end,
        "epoch journal disagrees with durable progress"
    );
    if let Some(last) = epochs.last() {
        ensure!(
            last.progress.commit == durable.commit && Some(last.progress.xid) == durable.xid,
            "epoch journal final transaction identity mismatch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{Binding, Stored},
        engine::plan::Checkpoint,
    };
    fn fixture() -> Result<(Durable, Vec<ReplayEpoch>)> {
        let progress = Progress::new(42, "0/50", "0/60")?;
        let durable = Durable {
            stored: Stored {
                epoch: 1,
                checkpoint: Checkpoint {
                    version: 1,
                    plan_identity: "epochs".into(),
                    time: 1,
                    arrangements: vec![],
                },
            },
            binding: Binding {
                source: crate::source::tests::contract(),
                sink: crate::harness::tuple_sink::sink("sink"),
            },
            fence: 1,
            checkpoint_end: "0/10".parse()?,
            published_time: 3,
            commit: progress.commit,
            end: progress.end,
            xid: Some(progress.xid),
        };
        let epochs = vec![
            ReplayEpoch {
                time: 2,
                previous: durable.checkpoint_end,
                progress: Progress::new(12, "0/20", "0/30")?,
                transactions: 5,
            },
            ReplayEpoch { time: 3, previous: "0/30".parse()?, progress, transactions: 2 },
        ];
        Ok((durable, epochs))
    }
    #[test]
    fn journal_requires_complete_contiguous_clock_and_source_suffix() -> Result<()> {
        let (durable, epochs) = fixture()?;
        validate_epochs(&epochs, &durable)?;
        assert!(validate_epochs(&[], &durable).is_err());
        assert!(validate_epochs(&epochs[1..], &durable).is_err());
        assert!(validate_epochs(&epochs[..1], &durable).is_err());
        let mut invalid = epochs.clone();
        invalid[1].previous = "0/20".parse()?;
        assert!(validate_epochs(&invalid, &durable).is_err());
        let mut invalid = epochs.clone();
        invalid[1].time = 4;
        assert!(validate_epochs(&invalid, &durable).is_err());
        let mut invalid = epochs.clone();
        invalid[0].transactions = 0;
        assert!(validate_epochs(&invalid, &durable).is_err());
        let mut invalid = epochs;
        invalid[1].progress.xid = 43;
        assert!(validate_epochs(&invalid, &durable).is_err());
        Ok(())
    }
    #[test]
    fn checkpointed_boundary_requires_no_retained_journal() -> Result<()> {
        let (mut durable, epochs) = fixture()?;
        durable.stored.checkpoint.time = durable.published_time;
        durable.checkpoint_end = durable.end;
        validate_epochs(&[], &durable)?;
        assert!(validate_epochs(&epochs, &durable).is_err());
        Ok(())
    }
}
